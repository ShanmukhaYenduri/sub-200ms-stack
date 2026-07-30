'use strict';

const jwt = require('jsonwebtoken');
const config = require('./config');

// Authentication is the first hop in the chain, and it is deliberately cheap.
//
// Verifying an HS256 signature is a hash over the token, not a network call:
// no session table, no Redis lookup, nothing that would spend the 2ms this hop
// is allowed in config.budgetMs.auth. That is the entire reason the token is
// self-contained.
//
// The tradeoff I am accepting: a stateless token cannot be revoked before it
// expires. I take that deal because these tokens are short lived by policy
// (scripts/token.js mints them for one hour). If revocation ever has to be
// immediate, the honest fix is a deny list of jti values in Redis, which puts
// the network hop back on the hot path. That should be a decision someone
// makes on purpose, not a default nobody chose.

const VERIFY_OPTIONS = {
  // Pinned algorithm. Left unpinned, the library honours whatever the token
  // header nominates, which lets the token choose how it gets checked.
  algorithms: ['HS256'],
  audience: 'metrics-api',
  issuer: 'sub-200ms-stack',
  // Enough tolerance for ordinary clock drift between services, not enough to
  // meaningfully extend the life of an expired token.
  clockTolerance: 5,
};

function bearerFrom(req) {
  const header = req.headers.authorization;
  if (!header) return null;

  const parts = header.split(' ');
  if (parts.length !== 2) return null;
  if (parts[0].toLowerCase() !== 'bearer') return null;
  if (!parts[1]) return null;

  return parts[1];
}

function verify(token) {
  return jwt.verify(token, config.jwtSecret, VERIFY_OPTIONS);
}

// Expired is worth distinguishing, because a client can act on it by
// refreshing. Every other failure collapses to one message: telling a caller
// whether the signature, the issuer or the audience was the problem is free
// reconnaissance.
function describe(err) {
  if (err.name === 'TokenExpiredError') return { error: 'token expired' };
  return { error: 'invalid token' };
}

// Two named middlewares rather than one with an options flag. A route that is
// public should say so at the route, because optional auth hidden behind a
// boolean is how endpoints quietly stop being protected.
function requireAuth(req, res, next) {
  const token = bearerFrom(req);
  if (!token) return res.status(401).json({ error: 'missing bearer token' });

  try {
    req.auth = verify(token);
    return next();
  } catch (err) {
    return res.status(401).json(describe(err));
  }
}

// The read paths use this one. The load test runs unauthenticated by default,
// and unauthenticated traffic is still rate limited, just keyed by IP instead
// of by subject (see subjectOf in rateLimit.js). A bad token is still rejected:
// the choice here is between no credential and a valid one, never between no
// credential and any credential.
function optionalAuth(req, res, next) {
  const token = bearerFrom(req);
  if (!token) return next();

  try {
    req.auth = verify(token);
    return next();
  } catch (err) {
    return res.status(401).json(describe(err));
  }
}

module.exports = { requireAuth, optionalAuth };

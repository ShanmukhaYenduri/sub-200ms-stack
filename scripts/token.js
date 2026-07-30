'use strict';

const crypto = require('node:crypto');
const jwt = require('jsonwebtoken');

const config = require('../src/config');

// Mints a token that src/auth.js will accept.
//
// This exists so that testing the authenticated paths does not require standing
// up an identity provider, and so that the claims the API verifies are written
// down somewhere executable. A README that describes the shape of a token drifts
// from the code; a script that produces one cannot.
//
//   npm run token
//   npm run token -- dashboard-service 900

// These three have to match VERIFY_OPTIONS in src/auth.js exactly. A mismatch
// here is a 401 that looks precisely like a wrong secret, and that is an hour of
// somebody's afternoon.
const AUDIENCE = 'metrics-api';
const ISSUER = 'sub-200ms-stack';
const ALGORITHM = 'HS256';

// One hour, and the reason matters: src/auth.js cannot revoke a token before it
// expires, and it takes that deal explicitly on the grounds that these are short
// lived. A script that minted year-long tokens would quietly invalidate the
// argument the auth middleware is built on.
const TTL_SECONDS = 3600;

function mint({ subject = 'local-operator', ttlSeconds = TTL_SECONDS, secret = config.jwtSecret } = {}) {
  if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
    throw new Error('ttlSeconds must be a positive integer');
  }

  // Empty payload. Every claim the API reads is a registered one, set below, so
  // there is nothing to put here. Padding a token with profile data is how a
  // credential turns into a cache with no invalidation rule.
  return jwt.sign({}, secret, {
    algorithm: ALGORITHM,
    subject: String(subject),
    audience: AUDIENCE,
    issuer: ISSUER,
    expiresIn: ttlSeconds,

    // A jti, even though nothing checks it yet.
    //
    // It makes two tokens minted in the same second distinguishable, which is
    // what lets a log line identify a credential without printing it, and it is
    // the value a deny list would key on if revocation ever has to be immediate.
    jwtid: crypto.randomUUID(),
  });
}

if (require.main === module) {
  const [subjectArg, ttlArg] = process.argv.slice(2);
  const ttlSeconds = ttlArg === undefined ? TTL_SECONDS : Number.parseInt(ttlArg, 10);

  if (Number.isNaN(ttlSeconds)) {
    process.stderr.write('usage: npm run token -- [subject] [ttlSeconds]\n');
    process.exit(1);
  }

  const token = mint({ subject: subjectArg || 'local-operator', ttlSeconds });

  // The token alone on stdout, everything else on stderr, so the useful case
  // works without any parsing:
  //
  //   k6 run -e TOKEN=$(npm run --silent token) loadtest/metrics.js
  process.stderr.write('subject=' + (subjectArg || 'local-operator') + ' ttl=' + ttlSeconds + 's\n');
  process.stdout.write(token + '\n');
}

module.exports = { mint, AUDIENCE, ISSUER, ALGORITHM, TTL_SECONDS };

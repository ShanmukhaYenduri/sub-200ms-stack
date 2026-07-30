'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const jwt = require('jsonwebtoken');

// Set before src/config.js is required, because config throws on a missing
// variable by design. These are strings, never connections: nothing in this file
// opens a socket, which is why it can run in CI with no services.
process.env.DATABASE_URL = process.env.DATABASE_URL || 'postgres://unused/unused';
process.env.REDIS_URL = process.env.REDIS_URL || 'redis://unused:6379';
process.env.JWT_SECRET = 'test-only-secret';

const { requireAuth, optionalAuth } = require('../src/auth');
const { mint, AUDIENCE, ISSUER } = require('../scripts/token');

// A response object that records what was done to it instead of writing to a
// socket. Both middlewares are synchronous -- verifying an HS256 signature is a
// hash, not a network call -- so the whole harness can be synchronous too.
function run(middleware, headers) {
  const req = { headers };
  const res = {
    statusCode: null,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };

  let nexted = false;
  middleware(req, res, () => {
    nexted = true;
  });

  return { req, res, nexted };
}

function bearer(token) {
  return { authorization: 'Bearer ' + token };
}

test('a token from scripts/token.js is accepted and its subject reaches the request', () => {
  // The reason the minting script is a module and not a shell one-liner: the
  // token the load test uses and the token this asserts on come from the same
  // code, so a claim that drifts fails here rather than at 3am.
  const { req, res, nexted } = run(requireAuth, bearer(mint({ subject: 'dashboard-service' })));

  assert.equal(nexted, true);
  assert.equal(res.statusCode, null);
  assert.equal(req.auth.sub, 'dashboard-service');
  assert.equal(req.auth.aud, AUDIENCE);
  assert.equal(req.auth.iss, ISSUER);
});

test('requireAuth rejects a request with no credential', () => {
  const { res, nexted } = run(requireAuth, {});

  assert.equal(nexted, false);
  assert.equal(res.statusCode, 401);
});

test('requireAuth rejects a header that is not a bearer token', () => {
  for (const header of ['Basic abc', 'Bearer', 'Bearer  ', 'abc', 'Bearer a b']) {
    const { res, nexted } = run(requireAuth, { authorization: header });
    assert.equal(nexted, false, header + ' should not have been accepted');
    assert.equal(res.statusCode, 401);
  }
});

test('the scheme is matched case insensitively', () => {
  // Clients send 'bearer' as often as 'Bearer'. Rejecting one of them is an
  // interoperability bug that reads like an auth bug.
  const { nexted } = run(requireAuth, { authorization: 'bearer ' + mint({}) });

  assert.equal(nexted, true);
});

test('a token that nominates its own algorithm is rejected', () => {
  // The reason algorithms is pinned in src/auth.js. Unpinned, jsonwebtoken
  // honours the alg in the token header, which lets the credential choose how it
  // gets checked. This is the assertion that would catch that pin being removed.
  const hs512 = jwt.sign({}, process.env.JWT_SECRET, {
    algorithm: 'HS512',
    subject: 'attacker',
    audience: AUDIENCE,
    issuer: ISSUER,
    expiresIn: 3600,
  });

  const { res, nexted } = run(requireAuth, bearer(hs512));

  assert.equal(nexted, false);
  assert.equal(res.statusCode, 401);
  assert.equal(res.body.error, 'invalid token');
});

test('a token minted for a different audience or issuer is rejected', () => {
  for (const claims of [{ audience: 'someone-elses-api', issuer: ISSUER }, { audience: AUDIENCE, issuer: 'someone-else' }]) {
    const token = jwt.sign({}, process.env.JWT_SECRET, {
      algorithm: 'HS256',
      subject: 'x',
      expiresIn: 3600,
      ...claims,
    });

    const { res, nexted } = run(requireAuth, bearer(token));
    assert.equal(nexted, false);
    assert.equal(res.statusCode, 401);
  }
});

test('a token signed with the wrong secret is rejected', () => {
  const forged = mint({ subject: 'x', secret: 'not-the-secret' });
  const { res, nexted } = run(requireAuth, bearer(forged));

  assert.equal(nexted, false);
  assert.equal(res.statusCode, 401);
});

test('an expired token is distinguished from an invalid one', () => {
  // Worth telling apart: a client can act on 'expired' by refreshing. Every
  // other failure collapses to one message, because naming the specific reason
  // is free reconnaissance.
  const stale = jwt.sign({}, process.env.JWT_SECRET, {
    algorithm: 'HS256',
    subject: 'x',
    audience: AUDIENCE,
    issuer: ISSUER,
    expiresIn: -3600,
  });

  const { res } = run(requireAuth, bearer(stale));

  assert.equal(res.statusCode, 401);
  assert.equal(res.body.error, 'token expired');
});

test('optionalAuth allows no credential but never a bad one', () => {
  // The distinction the read paths depend on. Anonymous traffic is served and
  // rate limited by IP; a broken credential is still an error, because the choice
  // is between no credential and a valid one, never between no credential and
  // any credential.
  const anonymous = run(optionalAuth, {});
  assert.equal(anonymous.nexted, true);
  assert.equal(anonymous.req.auth, undefined);

  const bad = run(optionalAuth, bearer('not-even-a-jwt'));
  assert.equal(bad.nexted, false);
  assert.equal(bad.res.statusCode, 401);
});

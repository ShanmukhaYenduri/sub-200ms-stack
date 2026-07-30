'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

process.env.DATABASE_URL = process.env.DATABASE_URL || 'postgres://unused/unused';
process.env.REDIS_URL = process.env.REDIS_URL || 'redis://unused:6379';
process.env.JWT_SECRET = process.env.JWT_SECRET || 'test-only-secret';

const config = require('../src/config');

// The budget in src/config.js says it is enforced rather than aspirational. This
// file is part of what makes that true: it checks the numbers add up, and that
// the threshold the load test fails on is still the same number the budget
// claims. Two files drifting apart quietly is how a project ends up with a
// README that is measurably wrong.

const budget = config.budgetMs;

test('every hop has a positive integer budget', () => {
  for (const [hop, ms] of Object.entries(budget)) {
    assert.ok(Number.isInteger(ms), hop + ' must be an integer, got ' + ms);
    assert.ok(ms > 0, hop + ' must be positive');
  }
});

test('a cache hit fits inside the total with room to spare', () => {
  // auth, then the limiter, then Redis. If this sum ever reaches the total then
  // the fast path has no headroom at all and the p95 is one GC pause from
  // breaching, with nothing left to attribute the breach to.
  const fastPath = budget.auth + budget.rateLimit + budget.cacheHit;

  assert.ok(fastPath < budget.total, 'cache-hit path ' + fastPath + 'ms does not fit in ' + budget.total + 'ms');
  assert.ok(fastPath <= budget.total / 4, 'the fast path should be a small fraction of the budget, not most of it');
});

test('a cold read fits inside the total', () => {
  // The expensive case: auth, limiter, a cache miss, then Postgres. This is the
  // sum the 200ms claim actually rests on.
  const coldPath = budget.auth + budget.rateLimit + budget.cacheHit + budget.dbQuery;

  assert.ok(coldPath <= budget.total, 'cold read path ' + coldPath + 'ms exceeds ' + budget.total + 'ms');
});

test('the cache is allowed to be cheaper than the query it replaces', () => {
  // Not a tautology. A cache budgeted at or above the query it avoids is a
  // network hop that buys nothing, and the honest response to that measurement
  // is to delete the cache rather than keep it for the architecture diagram.
  assert.ok(budget.cacheHit < budget.dbQuery);
});

test('the load test fails on the same number the budget claims', () => {
  // The link between the claim and the evidence. If someone relaxes the k6
  // threshold to make a run go green, or tightens the budget without touching
  // the load test, this fails instead of the numbers silently disagreeing.
  const loadTest = fs.readFileSync(path.join(__dirname, '..', 'loadtest', 'metrics.js'), 'utf8');
  const expected = 'p(95)<' + budget.total;

  assert.ok(
    loadTest.includes(expected),
    'loadtest/metrics.js does not assert ' + expected + ', so the budget is not enforced anywhere'
  );
});

test('the staleness window and the limiter are configured, not defaulted to nonsense', () => {
  assert.ok(config.cacheTtlSeconds > 0, 'a cache with no TTL has no backstop for an invalidation bug');
  assert.ok(config.rateLimit.max > 0);
  assert.ok(config.rateLimit.windowSeconds > 0);

  // The pool ceiling exists so that N instances cannot collectively exhaust
  // Postgres. A pool larger than the database allows is a queue in the wrong
  // place, and it fails at the worst possible moment.
  assert.ok(config.pgPoolMax > 0 && config.pgPoolMax <= 100);
});

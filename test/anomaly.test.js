'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const anomaly = require('../src/anomaly');

// These tests need nothing running.
//
// That is the point of src/anomaly.js being a separate file: the one piece of
// this service with logic worth getting wrong is also the one piece that can be
// checked without Postgres, Redis or a broker. A test suite that requires the
// whole stack is a test suite that stops being run.

// A baseline of 20 nines-and-ones either side of 100: mean exactly 100, and a
// standard deviation of about 1. Written out rather than randomised so a failure
// is reproducible instead of a story about a seed.
function tightBaseline(count) {
  const values = [];
  for (let i = 0; i < count; i += 1) values.push(i % 2 === 0 ? 99 : 101);
  return values;
}

test('mean of an empty baseline is 0 rather than NaN', () => {
  assert.equal(anomaly.mean([]), 0);
});

test('stddev divides by n - 1, not n', () => {
  // Population sigma of [1, 2, 3, 4] is 1.1180; sample sigma is 1.2910. Getting
  // this wrong understates the spread and manufactures anomalies, so it is
  // asserted on a case where the two answers visibly differ.
  assert.ok(Math.abs(anomaly.stddev([1, 2, 3, 4]) - 1.2909944487) < 1e-9);
});

test('a flat baseline scores 0 instead of Infinity', () => {
  // sigma is 0 here. Left unguarded this is Infinity, which marks the first
  // movement after any quiet period as an anomaly, or NaN, which fails the
  // NUMERIC insert and takes the consumer down.
  const flat = new Array(50).fill(7);
  const verdict = anomaly.score(9000, flat);

  assert.equal(verdict.score, 0);
  assert.equal(verdict.anomaly, false);
  assert.ok(Number.isFinite(verdict.score));
});

test('an empty baseline is not evidence of an anomaly', () => {
  const verdict = anomaly.score(500, []);

  assert.equal(verdict.samples, 0);
  assert.equal(verdict.anomaly, false);
});

test('an obvious outlier with enough history is an anomaly', () => {
  const verdict = anomaly.score(110, tightBaseline(40));

  assert.equal(verdict.samples, 40);
  assert.ok(verdict.score > anomaly.THRESHOLD);
  assert.equal(verdict.anomaly, true);
});

test('the same outlier without enough history is not', () => {
  // The guard that stops a quiet account from having everything flagged. The
  // score is still recorded, so the threshold can be re-tested against it later;
  // only the verdict is withheld.
  const verdict = anomaly.score(110, tightBaseline(10));

  assert.equal(verdict.samples, 10);
  assert.ok(verdict.score > anomaly.THRESHOLD);
  assert.equal(verdict.anomaly, false);
});

test('a value inside its baseline is not an anomaly', () => {
  const verdict = anomaly.score(100, tightBaseline(40));

  assert.ok(Math.abs(verdict.score) < anomaly.THRESHOLD);
  assert.equal(verdict.anomaly, false);
});

test('a value below the baseline scores negative and still flags', () => {
  // Signed, not absolute. A collapse in request volume is as interesting as a
  // spike, and storing the sign is what lets the anomaly view tell them apart.
  const verdict = anomaly.score(80, tightBaseline(40));

  assert.ok(verdict.score < 0);
  assert.equal(verdict.anomaly, true);
});

test('score is rounded to the four decimal places the column stores', () => {
  const verdict = anomaly.score(103, tightBaseline(40));
  const decimals = String(verdict.score).split('.')[1] || '';

  assert.ok(decimals.length <= 4, 'score ' + verdict.score + ' would be rounded by Postgres');
});

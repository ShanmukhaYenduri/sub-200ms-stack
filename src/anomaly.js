'use strict';

// Anomaly scoring, kept in its own file with no I/O.
//
// Everything here is a pure function of the numbers handed to it: no pool, no
// Redis client, no clock, no require that opens a socket. That is what makes it
// the one part of the write path that node --test can exercise with nothing
// running, which is also why the arithmetic lives here instead of inline in
// src/consumer.js.

// A z-score needs a population before it means anything. Below this many
// samples the standard deviation is mostly noise, and scoring against noise
// produces a service that cries wolf on its quietest accounts.
const MIN_SAMPLES = 30;

// Three sigma. Chosen, not tuned: on roughly normal data it fires on about 0.3%
// of points, which is an alert volume a human can actually triage. If that turns
// out to be wrong this is the single number to change, and it should be changed
// against a measurement rather than a feeling.
const THRESHOLD = 3;

function mean(values) {
  if (values.length === 0) return 0;
  let sum = 0;
  for (const value of values) sum += value;
  return sum / values.length;
}

// Sample standard deviation, dividing by n - 1 rather than n.
//
// The baseline is a bounded sample of an account's history, never the whole of
// it. Dividing by n treats that sample as the entire population, understates the
// spread, and manufactures anomalies out of ordinary variance.
function stddev(values, avg = mean(values)) {
  if (values.length < 2) return 0;
  let acc = 0;
  for (const value of values) acc += (value - avg) ** 2;
  return Math.sqrt(acc / (values.length - 1));
}

// How many standard deviations value sits from its baseline.
//
// A flat baseline has sigma 0, which would make this Infinity, or NaN when the
// value is flat too. Both are worse than useless: NaN fails the NUMERIC insert
// and takes the consumer down, and Infinity flags the first movement after any
// quiet period. A constant series is not evidence of anything, so it scores 0.
function zScore(value, baseline) {
  const avg = mean(baseline);
  const sigma = stddev(baseline, avg);
  if (!Number.isFinite(sigma) || sigma === 0) return 0;
  return (value - avg) / sigma;
}

// The decision src/consumer.js writes to the insights table.
//
// The score is returned even when anomaly is false, deliberately. That makes
// insights a continuous record rather than a list of alerts, so THRESHOLD can be
// re-evaluated against history that already exists instead of only against
// traffic that has not arrived yet. Rounded to four places to match the column.
function score(value, baseline) {
  const samples = baseline.length;
  const z = zScore(value, baseline);
  return {
    samples,
    score: Number(z.toFixed(4)),
    anomaly: samples >= MIN_SAMPLES && Math.abs(z) >= THRESHOLD,
  };
}

module.exports = { MIN_SAMPLES, THRESHOLD, mean, stddev, zScore, score };

'use strict';

const { query } = require('./db');

// Every statement the service runs lives in this file.
//
// Two reasons. The SQL can be reviewed on its own, without reading the HTTP
// layer around it; and no route handler can quietly grow a query of its own,
// which is how a service ends up with a slow path nobody knew existed.
//
// Placeholders everywhere, without exception. Not one value is concatenated
// into a statement: parameters travel separately from the SQL text, which is
// what makes injection structurally impossible here rather than merely
// unlikely.

// The four metrics db/seed.js writes. Used as an allow list at the edge so an
// unknown metric is a 400 instead of a query that scans and returns nothing.
const METRICS = ['latency_ms', 'requests', 'errors', 'saturation'];

// Cheapest possible liveness probe for Postgres. SELECT 1 touches no table and
// no index, so a failure means the connection is genuinely gone rather than
// that one relation is locked.
async function ping() {
  await query('SELECT 1');
}

// The hot path.
//
// Written to match metric_events_hot_path_idx exactly:
// (account_id, metric, recorded_at DESC). Two equality predicates then one
// range, in that order, so Postgres seeks straight to the matching slice and
// reads it already sorted. That removes the Sort node from the plan, which is
// most of the difference between this fitting the budget and not.
//
// LIMIT is not defensive decoration. An endpoint whose response size is chosen
// by the caller has no latency budget at all, it only has one that has not been
// exceeded yet.
async function hourlyMetric({ accountId, metric, since }) {
  const { rows } = await query(
    `SELECT date_trunc('hour', recorded_at) AS bucket,
            count(*)::int                   AS samples,
            avg(value)::float8              AS avg_value,
            max(value)::float8              AS max_value
       FROM metric_events
      WHERE account_id = $1
        AND metric = $2
        AND recorded_at >= $3
      GROUP BY 1
      ORDER BY 1 DESC
      LIMIT 168`,
    [accountId, metric, since]
  );
  return rows;
}

// The heavier read: every metric for an account, bucketed by day, with a
// percentile.
//
// This one is honestly slower and I am not pretending otherwise.
// percentile_cont has to materialise and sort each group, so the covering index
// metric_events_rollup_idx can feed the scan index-only but cannot remove the
// sort. That is exactly why loadtest/metrics.js samples this route on roughly
// one iteration in ten instead of hammering it: a traffic mix that ignores your
// expensive endpoint produces a p95 that describes traffic you do not serve.
async function dailyRollup({ accountId, since }) {
  const { rows } = await query(
    `SELECT date_trunc('day', recorded_at) AS bucket,
            metric,
            count(*)::int      AS samples,
            avg(value)::float8 AS avg_value,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY value)::float8 AS p95_value
       FROM metric_events
      WHERE account_id = $1
        AND recorded_at >= $2
      GROUP BY 1, 2
      ORDER BY 1 DESC, 2 ASC
      LIMIT 120`,
    [accountId, since]
  );
  return rows;
}

// Serves the anomaly view. The WHERE clause mirrors the predicate on
// insights_anomaly_idx, which matters: a partial index is only usable when the
// query restates the condition the index was built with.
async function anomalies({ accountId, since }) {
  const { rows } = await query(
    `SELECT metric, window_start, window_end, score::float8 AS score
       FROM insights
      WHERE account_id = $1
        AND anomaly
        AND window_start >= $2
      ORDER BY window_start DESC
      LIMIT 50`,
    [accountId, since]
  );
  return rows;
}

// The baseline the consumer scores a new event against.
//
// Bounded to the most recent N samples rather than to a time window on purpose:
// a quiet account would otherwise produce a baseline of two data points and
// declare everything anomalous.
async function recentValues({ accountId, metric, limit = 200 }) {
  const { rows } = await query(
    `SELECT value::float8 AS value
       FROM metric_events
      WHERE account_id = $1
        AND metric = $2
      ORDER BY recorded_at DESC
      LIMIT $3`,
    [accountId, metric, limit]
  );
  return rows.map((r) => r.value);
}

// Idempotency, enforced by the database rather than by the consumer.
//
// Replay is a feature of a log, not an accident: after a bad deploy I will
// deliberately rewind the offset and see the same messages again. Checking with
// a SELECT and then inserting is a race between two consumer instances. One
// INSERT with ON CONFLICT DO NOTHING is atomic, and rowCount tells me which
// instance won. Zero means somebody already handled this event.
async function claimEvent(eventId) {
  const res = await query(
    `INSERT INTO processed_events (event_id)
          VALUES ($1)
     ON CONFLICT (event_id) DO NOTHING`,
    [eventId]
  );
  return res.rowCount === 1;
}

async function insertEvent({ accountId, metric, value, recordedAt }) {
  const { rows } = await query(
    `INSERT INTO metric_events (account_id, metric, value, recorded_at)
          VALUES ($1, $2, $3, $4)
       RETURNING id`,
    [accountId, metric, value, recordedAt]
  );
  return rows[0].id;
}

async function insertInsight({ accountId, metric, windowStart, windowEnd, score, anomaly }) {
  await query(
    `INSERT INTO insights (account_id, metric, window_start, window_end, score, anomaly)
          VALUES ($1, $2, $3, $4, $5, $6)`,
    [accountId, metric, windowStart, windowEnd, score, anomaly]
  );
}

// Guard for the read routes. An unknown account should be a 404, not an empty
// 200: a dashboard cannot tell the difference between no data and a typo in the
// account id, and it will render a confident empty chart for both.
async function accountExists(accountId) {
  const { rows } = await query('SELECT 1 FROM accounts WHERE id = $1', [accountId]);
  return rows.length === 1;
}

module.exports = {
  METRICS,
  ping,
  hourlyMetric,
  dailyRollup,
  anomalies,
  recentValues,
  claimEvent,
  insertEvent,
  insertInsight,
  accountExists,
};

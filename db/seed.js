'use strict';

// Seeds enough data that the index choices in db/schema.sql actually matter.
//
// Two million events. At that size a sequential scan is unmistakable in EXPLAIN.
// At two thousand rows everything looks fast and the benchmark proves nothing,
// which is how most "we added Redis and it got faster" writeups end up
// meaningless.

const { Client } = require('pg');
const config = require('../src/config');

const ACCOUNTS = 500;
const EVENTS_PER_ACCOUNT = 4000;
const METRICS = ['latency_ms', 'requests', 'errors', 'saturation'];
const BATCH = 5000;

async function main() {
  const client = new Client({ connectionString: config.databaseUrl });
  await client.connect();

  const existing = await client.query('SELECT count(*)::int AS n FROM metric_events');
  if (existing.rows[0].n > 0) {
    console.log(`metric_events already holds ${existing.rows[0].n} rows, skipping`);
    await client.end();
    return;
  }

  console.log(`seeding ${ACCOUNTS} accounts`);
  const accountIds = [];
  for (let i = 0; i < ACCOUNTS; i += 1) {
    const res = await client.query(
      'INSERT INTO accounts (external_ref, name) VALUES ($1, $2) RETURNING id',
      [`acct-${i}`, `Account ${i}`]
    );
    accountIds.push(res.rows[0].id);
  }

  const total = ACCOUNTS * EVENTS_PER_ACCOUNT;
  console.log(`seeding ${total} metric events in batches of ${BATCH}`);

  const now = Date.now();
  let rows = [];
  let written = 0;

  for (const accountId of accountIds) {
    for (let j = 0; j < EVENTS_PER_ACCOUNT; j += 1) {
      const metric = METRICS[j % METRICS.length];

      // Spread history across 90 days so a range predicate has something to cut.
      const recordedAt = new Date(now - Math.floor(Math.random() * 90 * 86400000));
      rows.push([accountId, metric, (Math.random() * 500).toFixed(4), recordedAt]);

      if (rows.length >= BATCH) {
        written += await flush(client, rows);
        rows = [];
        if (written % 100000 === 0) console.log(`  ${written}/${total}`);
      }
    }
  }
  if (rows.length) written += await flush(client, rows);
  console.log(`  ${written}/${total}`);

  // ANALYZE is not optional here. Without fresh statistics the planner guesses,
  // and the first EXPLAIN you run describes a database that does not exist.
  console.log('running ANALYZE so the planner has real statistics');
  await client.query('ANALYZE accounts');
  await client.query('ANALYZE metric_events');

  await client.end();
  console.log('done');
}

// Multi-row INSERT instead of one round trip per row. Two million sequential
// inserts spend their entire life waiting on the network, not on Postgres.
async function flush(client, rows) {
  const placeholders = [];
  const params = [];

  rows.forEach((row, i) => {
    const b = i * 4;
    placeholders.push(`($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4})`);
    params.push(row[0], row[1], row[2], row[3]);
  });

  await client.query(
    `INSERT INTO metric_events (account_id, metric, value, recorded_at)
     VALUES ${placeholders.join(',')}`,
    params
  );

  return rows.length;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

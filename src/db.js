'use strict';

const { Pool } = require('pg');
const config = require('./config');

// A pool, not a client per request. Establishing a Postgres connection costs
// more than most of the queries on this hot path, so paying it per request
// would spend the whole budget on setup.
const pool = new Pool({
  connectionString: config.databaseUrl,
  max: config.pgPoolMax,
  idleTimeoutMillis: 30000,

  // Fail fast rather than queue forever. A request that cannot get a connection
  // inside its budget should be shed while the caller still cares about the
  // answer.
  connectionTimeoutMillis: 2000,

  // Hard ceiling on any single statement. Without it, one pathological query
  // holds a connection, the pool drains, and a slow query becomes an outage.
  statement_timeout: 1500,
});

// Idle clients can fail independently of any request. Unhandled, this takes the
// process down.
pool.on('error', (err) => {
  console.error({ msg: 'idle postgres client error', err: err.message });
});

async function query(text, params) {
  const startedAt = process.hrtime.bigint();
  try {
    return await pool.query(text, params);
  } finally {
    const ms = Number(process.hrtime.bigint() - startedAt) / 1e6;
    if (ms > config.budgetMs.dbQuery) {
      // Log the breach, not every query. Logging every query turns a latency
      // problem into a disk problem and buries the signal you needed.
      console.warn({
        msg: 'db query over budget',
        budgetMs: config.budgetMs.dbQuery,
        ms: Math.round(ms),
        text: text.replace(/\s+/g, ' ').slice(0, 120),
      });
    }
  }
}

async function close() {
  await pool.end();
}

module.exports = { pool, query, close };

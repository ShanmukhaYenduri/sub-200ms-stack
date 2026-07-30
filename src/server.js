'use strict';

const crypto = require('node:crypto');
const express = require('express');
const { Kafka } = require('kafkajs');

const config = require('./config');
const cache = require('./cache');
const db = require('./db');
const queries = require('./queries');
const { optionalAuth, requireAuth } = require('./auth');
const { rateLimit } = require('./rateLimit');

// The HTTP layer, and nothing else.
//
// No SQL (src/queries.js), no key construction (src/cache.js), no arithmetic
// (src/anomaly.js). What is left here is the order the hops run in, the
// validation that happens before any of them, and the one header that makes the
// cache measurable from outside the process.

const DEFAULT_METRIC = 'latency_ms';

// Ingest is the only route that needs Kafka, so a broker that is down must not
// be allowed to take the read path with it. The producer is connected on start,
// and if that fails the API still serves reads and POST /events answers 503.
const kafka = new Kafka({
  clientId: 'metrics-api',
  brokers: config.kafkaBrokers,
  retry: { retries: 3 },
});
const producer = kafka.producer({ allowAutoTopicCreation: true });
let producerReady = false;

const app = express();
app.disable('x-powered-by');

// ETags are computed by hashing the body on every response. On a route that
// already answers from Redis that is pure cost: work done after the answer was
// found, inside the budget, for a validator no client here sends.
app.set('etag', false);

// A metrics event is a handful of numbers. Anything larger is a mistake or an
// attack, and either way it should be rejected before it reaches a JSON parser.
app.use(express.json({ limit: '16kb' }));

// The budget from src/config.js, asserted on every request rather than
// described in a design doc. Logged only on breach: logging every request turns
// a latency problem into a disk problem and buries the signal.
app.use((req, res, next) => {
  const startedAt = process.hrtime.bigint();
  res.on('finish', () => {
    const ms = Number(process.hrtime.bigint() - startedAt) / 1e6;
    if (ms > config.budgetMs.total) {
      console.warn({
        msg: 'request over budget',
        budgetMs: config.budgetMs.total,
        ms: Math.round(ms),
        method: req.method,
        path: req.path,
        status: res.statusCode,
      });
    }
  });
  next();
});

// ---------------------------------------------------------------------------
// Input parsing
//
// Every one of these returns null for bad input rather than throwing or
// coercing. A route that silently turns 'abc' into NaN and then into a query is
// how an endpoint ends up scanning a table to return nothing.
// ---------------------------------------------------------------------------

function positiveInt(raw) {
  if (raw === undefined || raw === null || raw === '') return null;
  if (!/^[0-9]+$/.test(String(raw))) return null;
  const parsed = Number.parseInt(String(raw), 10);
  return parsed > 0 ? parsed : null;
}

function boundedInt(raw, fallback, min, max) {
  if (raw === undefined || raw === '') return fallback;
  const parsed = positiveInt(raw);
  if (parsed === null || parsed < min || parsed > max) return null;
  return parsed;
}

// Truncate the window start to the top of the hour.
//
// This is what makes the cache a cache. A since of 'now minus 168 hours' is a
// different value on every request, so every key would be unique, every read
// would miss, and Redis would be decoration that costs a network hop. Truncated,
// the key is stable for the whole hour and the TTL is what bounds staleness --
// which is the knob that is supposed to bound it.
//
// It also matches the query: hourlyMetric groups by date_trunc('hour', ...), so
// a sub-hour window start could not change the answer anyway.
function windowStart(hours) {
  const topOfHour = new Date();
  topOfHour.setUTCMinutes(0, 0, 0);
  return new Date(topOfHour.getTime() - hours * 3600000);
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

// Liveness only: is this process able to answer at all.
//
// Deliberately checks nothing downstream. A liveness probe that touches
// Postgres restarts every API instance the moment the database blips, which
// converts one outage into two and throws away the warm pool on the way.
app.get('/healthz', (req, res) => {
  res.json({ status: 'ok' });
});

// Readiness: should this instance be sent traffic.
//
// Postgres and Redis gate it because the read path cannot answer without them.
// Kafka is reported but does not gate it: refusing all traffic because ingest is
// down would take the dashboards offline for a problem they do not have.
app.get('/readyz', async (req, res) => {
  const checks = { postgres: false, redis: false, kafka: producerReady };

  try {
    await queries.ping();
    checks.postgres = true;
  } catch (err) {
    console.error({ msg: 'readiness: postgres unreachable', err: err.message });
  }

  try {
    checks.redis = (await cache.redis.ping()) === 'PONG';
  } catch (err) {
    console.error({ msg: 'readiness: redis unreachable', err: err.message });
  }

  const ready = checks.postgres && checks.redis;
  res.status(ready ? 200 : 503).json({ ready, checks });
});

// ---------------------------------------------------------------------------
// Read paths
//
// Middleware order is the budget order from src/config.js: auth (2ms, a hash),
// then the limiter (3ms, one atomic Redis call), then the cache (10ms), then
// Postgres (120ms) only if the cache had nothing. Auth runs before the limiter
// on purpose, because the limiter keys on the authenticated subject when there
// is one and only falls back to IP when there is not.
// ---------------------------------------------------------------------------

app.get('/metrics', optionalAuth, rateLimit, async (req, res, next) => {
  try {
    const accountId = positiveInt(req.query.accountId);
    if (accountId === null) {
      return res.status(400).json({ error: 'accountId must be a positive integer' });
    }

    // Allow list, not a pass-through. An unknown metric is a typo, and answering
    // it with an indexed scan that returns nothing spends the budget to say so.
    const metric = req.query.metric === undefined ? DEFAULT_METRIC : String(req.query.metric);
    if (!queries.METRICS.includes(metric)) {
      return res.status(400).json({ error: 'unknown metric, expected one of ' + queries.METRICS.join(', ') });
    }

    // 168 hours, because hourlyMetric is LIMIT 168. Asking for more would
    // silently return a truncated week and look like data loss.
    const hours = boundedInt(req.query.hours, 168, 1, 168);
    if (hours === null) {
      return res.status(400).json({ error: 'hours must be an integer between 1 and 168' });
    }

    const since = windowStart(hours);
    const key = cache.metricsKey({ accountId, metric, since: since.toISOString() });

    // Cache first, before the existence check. Confirming the account exists on a
    // hit would put a Postgres round trip back on the path the cache exists to
    // remove, to answer a question the cached body already answered once.
    const cached = await cache.get(key);
    if (cached) {
      // Set on hits and misses so the load test can measure the hit rate from
      // outside the process instead of trusting a log line.
      res.setHeader('X-Cache', 'HIT');
      return res.json(cached);
    }
    res.setHeader('X-Cache', 'MISS');

    // 404, not an empty 200. A dashboard cannot tell the difference between an
    // account with no data and a typo in the account id, and it will render a
    // confident empty chart for both.
    if (!(await queries.accountExists(accountId))) {
      return res.status(404).json({ error: 'unknown account' });
    }

    const series = await queries.hourlyMetric({ accountId, metric, since });
    const body = {
      accountId,
      metric,
      since: since.toISOString(),
      buckets: series.length,
      series,
    };

    // Cached only because there is a named event that clears it: any write for
    // this account, in src/consumer.js. The TTL is the backstop for the
    // invalidation bugs I have not thought of yet.
    await cache.set(key, body);
    return res.json(body);
  } catch (err) {
    return next(err);
  }
});

// The heavier read, and deliberately not cached.
//
// Not an oversight: a cached key is a promise to invalidate it, and this one has
// no event that clears it cheaply. The dashboard also asks for it roughly once
// per page load rather than in a loop, so a cache here would mostly store
// answers nobody asks for twice while adding a second thing to get wrong.
// It stays inside the budget on the index alone -- see bench/explain.md.
app.get('/reports/daily', optionalAuth, rateLimit, async (req, res, next) => {
  try {
    const accountId = positiveInt(req.query.accountId);
    if (accountId === null) {
      return res.status(400).json({ error: 'accountId must be a positive integer' });
    }

    // 30 days across 4 metrics is 120 rows, which is exactly the LIMIT on
    // dailyRollup. The cap is 90 because that is the history db/seed.js writes;
    // beyond it the response is truncated rather than empty, which is worse.
    const days = boundedInt(req.query.days, 30, 1, 90);
    if (days === null) {
      return res.status(400).json({ error: 'days must be an integer between 1 and 90' });
    }

    if (!(await queries.accountExists(accountId))) {
      return res.status(404).json({ error: 'unknown account' });
    }

    const since = windowStart(days * 24);
    const rows = await queries.dailyRollup({ accountId, since });
    return res.json({ accountId, since: since.toISOString(), days, rows });
  } catch (err) {
    return next(err);
  }
});

// The anomaly view. Reads what src/consumer.js wrote.
app.get('/insights', optionalAuth, rateLimit, async (req, res, next) => {
  try {
    const accountId = positiveInt(req.query.accountId);
    if (accountId === null) {
      return res.status(400).json({ error: 'accountId must be a positive integer' });
    }

    const days = boundedInt(req.query.days, 7, 1, 90);
    if (days === null) {
      return res.status(400).json({ error: 'days must be an integer between 1 and 90' });
    }

    if (!(await queries.accountExists(accountId))) {
      return res.status(404).json({ error: 'unknown account' });
    }

    const since = windowStart(days * 24);
    const rows = await queries.anomalies({ accountId, since });
    return res.json({ accountId, since: since.toISOString(), days, anomalies: rows });
  } catch (err) {
    return next(err);
  }
});

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

// Ingest returns 202, not 201.
//
// The event has been accepted for processing, not processed: the row and the
// insight are written by src/consumer.js, after this response is already sent.
// Claiming 201 Created would be a lie about durability that a caller could
// reasonably build a retry policy on.
//
// requireAuth rather than optionalAuth. Writes are the one place where the
// difference between 'no credential' and 'a valid credential' has to be an
// error and not a fallback.
app.post('/events', requireAuth, rateLimit, async (req, res, next) => {
  try {
    if (!producerReady) {
      // Honest 503 rather than a queue in process memory. Buffering here would
      // acknowledge events this process is about to lose on the next deploy.
      res.setHeader('Retry-After', '5');
      return res.status(503).json({ error: 'ingest unavailable' });
    }

    const body = req.body || {};

    const accountId = positiveInt(body.accountId);
    if (accountId === null) {
      return res.status(400).json({ error: 'accountId must be a positive integer' });
    }

    const metric = String(body.metric === undefined ? '' : body.metric);
    if (!queries.METRICS.includes(metric)) {
      return res.status(400).json({ error: 'unknown metric, expected one of ' + queries.METRICS.join(', ') });
    }

    // Number.isFinite, not a truthiness check: 0 is a legitimate measurement and
    // NaN and Infinity both survive Number() and then fail on the NUMERIC insert
    // inside the consumer, where the error is much further from its cause.
    const value = Number(body.value);
    if (!Number.isFinite(value)) {
      return res.status(400).json({ error: 'value must be a finite number' });
    }

    const recordedAt = body.recordedAt === undefined ? new Date() : new Date(body.recordedAt);
    if (Number.isNaN(recordedAt.getTime())) {
      return res.status(400).json({ error: 'recordedAt must be an ISO 8601 timestamp' });
    }

    // The caller may supply the id, and should when it retries.
    //
    // This is the whole idempotency story: the consumer deduplicates on this
    // value through processed_events, so a client that retries a timed-out POST
    // with the same eventId cannot double count. Minted here only when the
    // caller has no opinion, in which case a retry is a genuinely new event.
    const eventId = typeof body.eventId === 'string' && body.eventId.length > 0
      ? body.eventId
      : crypto.randomUUID();

    await producer.send({
      topic: config.kafkaTopic,
      messages: [
        {
          // Partition key is the account, so every event for one account lands
          // on one partition and is therefore ordered. Without this, two events
          // for the same account can be scored out of order by two consumers and
          // the later one wins.
          key: String(accountId),
          value: JSON.stringify({ eventId, accountId, metric, value, recordedAt: recordedAt.toISOString() }),
        },
      ],
    });

    return res.status(202).json({ accepted: true, eventId });
  } catch (err) {
    return next(err);
  }
});

// ---------------------------------------------------------------------------
// Fallbacks
// ---------------------------------------------------------------------------

app.use((req, res) => {
  res.status(404).json({ error: 'not found' });
});

// One error handler, and it does not echo err.message to the caller.
//
// A stack trace or a Postgres error string in a 500 body is free reconnaissance:
// it names tables, columns and versions. The detail goes to the log, where the
// person who needs it can read it and the person probing the endpoint cannot.
app.use((err, req, res, next) => {
  console.error({
    msg: 'unhandled request error',
    method: req.method,
    path: req.path,
    err: err.message,
  });
  if (res.headersSent) return next(err);
  return res.status(500).json({ error: 'internal error' });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

async function start() {
  try {
    await producer.connect();
    producerReady = true;
  } catch (err) {
    // Not fatal. Reads do not need Kafka, and killing the API because ingest is
    // unavailable is a self-inflicted read outage.
    console.error({ msg: 'kafka producer unavailable at startup', err: err.message });
  }

  const server = app.listen(config.port, () => {
    console.log({ msg: 'listening', port: config.port, budgetMs: config.budgetMs });
  });

  // Node closes idle keep-alive sockets after 5 seconds by default. A load
  // generator holding connections open across a 130 second run would eat that
  // as reconnects, and a TCP handshake charged to the p95 is latency the query
  // never spent. headersTimeout must stay above keepAliveTimeout or the race
  // between them shows up as sporadic 502s behind a proxy.
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;

  installShutdown(server);
  return server;
}

// SIGTERM is how this process finds out about every deploy, so the shutdown path
// runs far more often than any of the error paths above.
//
// Order matters: stop accepting new connections, let in-flight requests finish,
// then close the clients they were using. Closing the pool first would fail the
// requests that were already accepted -- a 500 caused by the deploy, not by a
// bug, and one that looks exactly like a bug in the logs.
function installShutdown(server) {
  let shuttingDown = false;

  const close = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log({ msg: 'shutting down', signal });

    // A drain that never finishes is a pod that never restarts. One request
    // stuck on a slow query should not hold the whole deploy, so the wait is
    // bounded and then the process leaves anyway, loudly.
    const giveUp = setTimeout(() => {
      console.error({ msg: 'shutdown timed out, exiting with work in flight' });
      process.exit(1);
    }, 10000);
    giveUp.unref();

    try {
      await new Promise((resolve) => server.close(resolve));
      // Settled individually: a Redis client that refuses to quit must not
      // prevent the Postgres pool from closing.
      await Promise.allSettled([
        producerReady ? producer.disconnect() : Promise.resolve(),
        cache.close(),
        db.close(),
      ]);
      clearTimeout(giveUp);
      console.log({ msg: 'shutdown complete' });
      process.exit(0);
    } catch (err) {
      console.error({ msg: 'shutdown failed', err: err.message });
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => close('SIGTERM'));
  process.on('SIGINT', () => close('SIGINT'));

  // An unhandled rejection leaves the process in a state nobody reasoned about.
  // Crashing on purpose is more honest than serving from it, and the
  // orchestrator already knows how to replace a dead container.
  process.on('unhandledRejection', (reason) => {
    console.error({ msg: 'unhandled rejection, exiting', err: String(reason) });
    process.exit(1);
  });
}

// Exported so the app can be mounted by a test without binding a port. Only the
// direct invocation listens, which is what makes require() of this file safe.
if (require.main === module) {
  start().catch((err) => {
    console.error({ msg: 'failed to start', err: err.message });
    process.exit(1);
  });
}

module.exports = { app, start };

'use strict';

const { Kafka } = require('kafkajs');

const config = require('./config');
const cache = require('./cache');
const db = require('./db');
const queries = require('./queries');
const anomaly = require('./anomaly');

// The write path, which is separate from the API on purpose.
//
// Scoring an event means reading a baseline and writing two rows. Doing that
// inside POST /events would put it inside a caller's latency budget, and the
// caller gains nothing by waiting for it. Here it runs at its own pace, and a
// slow scoring pass shows up as consumer lag rather than as a slow API.
//
// It is also the only component that invalidates the cache, which is what makes
// the invalidation rule in src/cache.js a rule and not a convention: one writer,
// one place that clears the keys the writer just made stale.

const GROUP_ID = 'insights-worker';

const kafka = new Kafka({
  clientId: GROUP_ID,
  brokers: config.kafkaBrokers,
  retry: { retries: 5 },
});

const consumer = kafka.consumer({
  groupId: GROUP_ID,
  // Long enough that an ordinary GC pause or a slow query is not mistaken for a
  // dead consumer. A rebalance triggered by a 200ms hiccup costs far more than
  // the hiccup did.
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

// Parsing is separated from handling because the two failures need opposite
// treatment. A malformed message will never become well formed, so retrying it
// blocks its partition forever; a database that is down will come back, so not
// retrying loses data. This function decides which kind of failure it is.
function parse(raw) {
  let payload;
  try {
    payload = JSON.parse(raw.toString('utf8'));
  } catch (err) {
    return { error: 'not valid JSON' };
  }

  if (typeof payload.eventId !== 'string' || payload.eventId.length === 0) {
    return { error: 'missing eventId' };
  }

  const accountId = Number(payload.accountId);
  if (!Number.isInteger(accountId) || accountId <= 0) {
    return { error: 'accountId is not a positive integer' };
  }

  if (!queries.METRICS.includes(payload.metric)) {
    return { error: 'unknown metric' };
  }

  const value = Number(payload.value);
  if (!Number.isFinite(value)) {
    return { error: 'value is not a finite number' };
  }

  const recordedAt = new Date(payload.recordedAt);
  if (Number.isNaN(recordedAt.getTime())) {
    return { error: 'recordedAt is not a timestamp' };
  }

  return { event: { eventId: payload.eventId, accountId, metric: payload.metric, value, recordedAt } };
}

// The window an insight describes: the hour the event was recorded in.
//
// Aligned to the hour rather than to the arrival time so that the same event,
// replayed a week later, produces the same window. An insight whose window
// depends on when the consumer happened to run is not reproducible.
function hourWindow(recordedAt) {
  const start = new Date(recordedAt);
  start.setUTCMinutes(0, 0, 0);
  return { windowStart: start, windowEnd: new Date(start.getTime() + 3600000) };
}

async function handle(event) {
  // Idempotency first, and enforced by the database rather than by this process.
  //
  // Replay is deliberate: after a bad deploy I rewind the offset and see these
  // messages again. A SELECT-then-INSERT check would be a race between two
  // consumer instances; INSERT ... ON CONFLICT DO NOTHING is atomic, and losing
  // the race is a zero rowCount, which is the signal to stop here.
  const claimed = await queries.claimEvent(event.eventId);
  if (!claimed) {
    console.log({ msg: 'duplicate event ignored', eventId: event.eventId });
    return { outcome: 'duplicate' };
  }

  // The baseline is read before the insert, not after.
  //
  // Otherwise the new value is part of the distribution it is being compared
  // against, which drags the mean toward it and shrinks its own z-score. The
  // effect is largest for exactly the outliers this is meant to catch.
  const baseline = await queries.recentValues({ accountId: event.accountId, metric: event.metric });

  await queries.insertEvent({
    accountId: event.accountId,
    metric: event.metric,
    value: event.value,
    recordedAt: event.recordedAt,
  });

  const verdict = anomaly.score(event.value, baseline);
  const { windowStart, windowEnd } = hourWindow(event.recordedAt);

  await queries.insertInsight({
    accountId: event.accountId,
    metric: event.metric,
    windowStart,
    windowEnd,
    score: verdict.score,
    anomaly: verdict.anomaly,
  });

  // Invalidate last, after the row it makes stale is actually visible.
  //
  // Clearing the cache before the write would open a window where a read misses,
  // goes to Postgres, gets the old answer, and caches it again for a full TTL --
  // an invalidation that makes the staleness last longer than doing nothing.
  const removed = await cache.invalidateAccount(event.accountId);

  if (verdict.anomaly) {
    console.log({
      msg: 'anomaly recorded',
      accountId: event.accountId,
      metric: event.metric,
      value: event.value,
      score: verdict.score,
      samples: verdict.samples,
    });
  }

  return { outcome: 'processed', keysRemoved: removed };
}

async function start() {
  await consumer.connect();

  // fromBeginning is false so a new group does not replay the entire topic on
  // first start. Replay is something I do on purpose by resetting offsets, not
  // something that should happen because a container was recreated.
  await consumer.subscribe({ topic: config.kafkaTopic, fromBeginning: false });

  console.log({ msg: 'consuming', topic: config.kafkaTopic, groupId: GROUP_ID });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const startedAt = process.hrtime.bigint();
      const parsed = parse(message.value);

      if (parsed.error) {
        // Logged and skipped, never retried.
        //
        // Retrying a message that can never parse stalls the partition and every
        // valid message queued behind it: one bad payload becomes a total outage
        // of the write path. The honest next step is a dead letter topic so these
        // are inspectable rather than only countable; until then this log line is
        // the record, and it is an error because someone should look at it.
        console.error({
          msg: 'unprocessable message skipped',
          err: parsed.error,
          topic,
          partition,
          offset: message.offset,
        });
        return;
      }

      // Anything thrown from here is a transient failure -- Postgres or Redis
      // unreachable -- so it is allowed to propagate. kafkajs will not commit the
      // offset, and the message is redelivered once the dependency is back.
      // processed_events makes that redelivery safe.
      const result = await handle(parsed.event);

      const ms = Number(process.hrtime.bigint() - startedAt) / 1e6;
      console.log({
        msg: 'event handled',
        result,
        eventId: parsed.event.eventId,
        accountId: parsed.event.accountId,
        metric: parsed.event.metric,
        ms: Math.round(ms),
      });
    },
  });

  installShutdown();
}

// SIGTERM during a rebalance is the interesting case: leaving without telling
// the group means the broker waits out sessionTimeout before reassigning the
// partitions, so every message on them is stalled for 30 seconds by a clean
// deploy. disconnect() leaves the group deliberately instead.
function installShutdown() {
  let shuttingDown = false;

  const close = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log({ msg: 'shutting down', signal });

    const giveUp = setTimeout(() => {
      console.error({ msg: 'shutdown timed out, exiting mid-message' });
      process.exit(1);
    }, 10000);
    giveUp.unref();

    // Kafka first: stop new messages arriving, then close what handling them
    // needed. The other order hands a live message a dead connection pool.
    await Promise.allSettled([consumer.disconnect()]);
    await Promise.allSettled([cache.close(), db.close()]);

    clearTimeout(giveUp);
    console.log({ msg: 'shutdown complete' });
    process.exit(0);
  };

  process.on('SIGTERM', () => close('SIGTERM'));
  process.on('SIGINT', () => close('SIGINT'));
}

// No require.main guard on the shutdown handlers, but one here: this file is a
// process, not a library. It is exported only so a test can reach parse() and
// hourWindow() without connecting to a broker.
if (require.main === module) {
  start().catch((err) => {
    console.error({ msg: 'consumer failed to start', err: err.message });
    process.exit(1);
  });
}

module.exports = { parse, hourWindow, handle, start };

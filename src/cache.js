'use strict';

const Redis = require('ioredis');
const config = require('./config');

const redis = new Redis(config.redisUrl, {
  // Two retries, then give up and let the caller fall through to Postgres.
  // Retrying a cache forever is slower than not having one.
  maxRetriesPerRequest: 2,
  enableReadyCheck: true,
});

redis.on('error', (err) => {
  console.error({ msg: 'redis error', err: err.message });
});

// Key shape: metrics:v1:<accountId>:<metric>:<sinceIso>
//
// The v1 segment is a manual epoch. When the response shape changes, bump it:
// every old key becomes unreachable at once and expires on its own schedule.
// That is cheaper and safer than writing a migration for a cache.
function metricsKey({ accountId, metric, since }) {
  return `metrics:v1:${accountId}:${metric}:${since}`;
}

// The other half of the contract. Every cached key must have a named event that
// clears it -- here, any write for that account. If I cannot name the event, the
// read does not get cached at all.
function accountPattern(accountId) {
  return `metrics:v1:${accountId}:*`;
}

async function get(key) {
  const raw = await redis.get(key);
  return raw ? JSON.parse(raw) : null;
}

async function set(key, value, ttlSeconds = config.cacheTtlSeconds) {
  // TTL is mandatory, never optional. It is the backstop for every invalidation
  // bug I have not thought of yet, which is the set of bugs that matters.
  await redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
}

// SCAN, never KEYS.
//
// KEYS walks the entire keyspace while holding the single Redis thread, so on a
// large instance it is a self-inflicted outage. SCAN is cursored and yields
// between batches. UNLINK rather than DEL so the memory is reclaimed on a
// background thread instead of stalling the foreground one.
async function invalidateAccount(accountId) {
  const pattern = accountPattern(accountId);
  let cursor = '0';
  let removed = 0;

  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
    cursor = next;
    if (keys.length) {
      await redis.unlink(...keys);
      removed += keys.length;
    }
  } while (cursor !== '0');

  return removed;
}

async function close() {
  await redis.quit();
}

module.exports = { redis, metricsKey, accountPattern, get, set, invalidateAccount, close };

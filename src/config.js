'use strict';

// The only file that reads process.env.
//
// When an environment misbehaves there is exactly one place to look, and the
// process refuses to start rather than discovering a missing variable halfway
// through the first request.

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`missing required env var: ${name}`);
  return value;
}

function int(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed)) throw new Error(`env var ${name} must be an integer`);
  return parsed;
}

module.exports = {
  port: int('PORT', 3000),

  databaseUrl: required('DATABASE_URL'),
  redisUrl: required('REDIS_URL'),
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),

  // The topic, read here rather than declared as a constant in two files. The
  // producer in src/server.js and the consumer in src/consumer.js have to agree
  // on this string, and disagreeing is silent: messages go somewhere real and
  // nobody ever reads them. One definition removes the failure mode.
  kafkaTopic: process.env.KAFKA_TOPIC || 'metric-events',

  jwtSecret: required('JWT_SECRET'),

  // The staleness window we are explicitly choosing to accept on reads.
  cacheTtlSeconds: int('CACHE_TTL_SECONDS', 30),

  rateLimit: {
    max: int('RATE_LIMIT_MAX', 300),
    windowSeconds: int('RATE_LIMIT_WINDOW_SECONDS', 60),
  },

  pgPoolMax: int('PG_POOL_MAX', 20),

  // The latency budget, per hop, in milliseconds.
  //
  // Written down in code rather than in a design doc for one reason: code can be
  // asserted against. src/db.js warns when a query exceeds dbQuery, the server
  // warns when a request exceeds total, and loadtest/metrics.js fails the run if
  // p95 crosses it. A budget nobody enforces is a wish.
  budgetMs: {
    auth: 2,
    rateLimit: 3,
    cacheHit: 10,
    dbQuery: 120,
    total: 200,
  },
};

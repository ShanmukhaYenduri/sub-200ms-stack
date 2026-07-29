'use strict';

const config = require('./config');
const { redis } = require('./cache');

// Token bucket, evaluated inside Redis as a Lua script.
//
// Why not GET, compare, SET from Node? Because that is a race. Two concurrent
// requests both read the same count, both conclude they are under the limit,
// and the limit quietly becomes a suggestion. Redis executes Lua atomically on
// its single thread, so read, refill and decrement happen as one indivisible
// step. That is the entire reason this is a script and not three commands.
//
// Why a bucket rather than a fixed window? A fixed window lets a client spend
// its full allowance at 11:59:59 and again at 12:00:00, so the real ceiling is
// double the configured one. Tokens here refill continuously, so the limit
// holds across every boundary.
const SCRIPT = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local window   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

local refillPerSecond = capacity / window

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + (elapsed * refillPerSecond))

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)

-- Expire idle buckets so the keyspace does not grow with every client that
-- ever showed up once. Two windows is enough to refill from empty.
redis.call('EXPIRE', key, math.ceil(window * 2))

return { allowed, math.floor(tokens) }
`;

redis.defineCommand('takeToken', { numberOfKeys: 1, lua: SCRIPT });

// Prefer the authenticated subject over the IP.
//
// Limiting by IP alone punishes every user behind one corporate NAT and does
// nothing about a single abusive client rotating addresses. IP is the fallback
// for unauthenticated traffic only.
function subjectOf(req) {
  return req.auth && req.auth.sub ? `sub:${req.auth.sub}` : `ip:${req.ip}`;
}

async function rateLimit(req, res, next) {
  const key = `rl:v1:${subjectOf(req)}`;

  try {
    const [allowed, remaining] = await redis.takeToken(
      key,
      config.rateLimit.max,
      config.rateLimit.windowSeconds,
      Date.now() / 1000
    );

    res.setHeader('X-RateLimit-Limit', config.rateLimit.max);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, remaining));

    if (allowed !== 1) {
      res.setHeader('Retry-After', '1');
      return res.status(429).json({ error: 'rate limit exceeded' });
    }

    return next();
  } catch (err) {
    // Fail open, loudly.
    //
    // If Redis is unreachable the choice is between rejecting all traffic and
    // serving it unmetered. An outage of the limiter should not become an outage
    // of the product -- but it must page someone, so this is an error log and
    // not a debug line.
    console.error({ msg: 'rate limiter unavailable, failing open', err: err.message });
    return next();
  }
}

module.exports = { rateLimit, SCRIPT };

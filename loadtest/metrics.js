// k6 load test for the metrics read path.
//
// The point of this file is that the claim on my profile is falsifiable. The
// budget is expressed as a threshold, so k6 exits non-zero when it is missed.
// A performance claim that cannot fail a build is a slogan.
//
//   k6 run loadtest/metrics.js
//   k6 run -e BASE_URL=http://localhost:3000 -e VUS=100 loadtest/metrics.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const TOKEN = __ENV.TOKEN || '';
const VUS = Number(__ENV.VUS || 50);

// Separate trends per path, because a single aggregate p95 lets a fast endpoint
// pay for a slow one. The budget was allocated per hop, so it is measured per hop.
const cachedRead = new Trend('latency_cached_read', true);
const coldRead = new Trend('latency_cold_read', true);
const reportRead = new Trend('latency_report', true);
const cacheHit = new Rate('cache_hit');

export const options = {
  scenarios: {
    // Ramping arrival rate, not a fixed VU count: it holds throughput constant
    // and lets latency be the variable. With fixed VUs a slow system quietly
    // reduces its own load and the graph looks better than the system is.
    steady: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      preAllocatedVUs: VUS,
      maxVUs: VUS * 4,
      stages: [
        { target: 200, duration: '30s' }, // ramp
        { target: 200, duration: '60s' }, // hold: this is the window that counts
        { target: 400, duration: '30s' }, // push past the design point on purpose
        { target: 0, duration: '10s' },
      ],
    },
  },

  thresholds: {
    // The claim, as a build failure.
    'http_req_duration{expected_response:true}': ['p(95)<200'],
    'latency_cached_read': ['p(95)<50'],
    'latency_cold_read': ['p(95)<200'],
    'latency_report': ['p(95)<200'],
    'http_req_failed': ['rate<0.01'],
    // If the cache is not actually being hit, the p95 above is a measurement of
    // an empty claim -- so the hit rate is a threshold too, not a nice-to-have.
    'cache_hit': ['rate>0.80'],
  },

  // Report percentiles, not just the average. Stated explicitly so nobody has to
  // wonder which number the README is quoting.
  summaryTrendStats: ['avg', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const headers = TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {};

// A small hot set plus a long tail. Hammering one key measures Redis and nothing
// else; hammering random keys measures Postgres and nothing else. Real traffic is
// neither, so the mix is roughly 80/20 and the cache hit rate is asserted above.
const HOT_ACCOUNTS = [101, 102, 103, 104, 105];
// Matches ACCOUNTS in db/seed.js. It has to: pick an id the seed never created
// and the API correctly answers 404, which is a check failure and a
// http_req_failed breach, so the run fails for a reason that has nothing to do
// with latency. Overridable for a larger seed.
const TOTAL_ACCOUNTS = Number(__ENV.TOTAL_ACCOUNTS || 500);

function pickAccount() {
  if (Math.random() < 0.8) {
    return HOT_ACCOUNTS[Math.floor(Math.random() * HOT_ACCOUNTS.length)];
  }
  return 1 + Math.floor(Math.random() * TOTAL_ACCOUNTS);
}

export default function () {
  const account = pickAccount();
  const isHot = HOT_ACCOUNTS.includes(account);

  const res = http.get(`${BASE_URL}/metrics?accountId=${account}`, {
    headers,
    tags: { name: 'GET /metrics' },
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
    // Correctness under load, not just latency. A cache that returns the wrong
    // account's data quickly is the worst possible outcome and it will not show
    // up in a latency graph.
    'account matches': (r) => {
      try {
        return r.json('accountId') === account;
      } catch (e) {
        return false;
      }
    },
  });

  // The API sets this header on a cache hit, which is what makes the hit rate
  // measurable from outside the process instead of trusted from a log line.
  const hit = res.headers['X-Cache'] === 'HIT';
  cacheHit.add(hit);
  (hit ? cachedRead : coldRead).add(res.timings.duration);

  // Reports are the heavier read path: no cache, indexed query, wider scan.
  // Sampled rather than hit on every iteration, to keep the traffic mix honest.
  if (Math.random() < 0.1) {
    const rep = http.get(`${BASE_URL}/reports/daily?accountId=${account}`, {
      headers,
      tags: { name: 'GET /reports/daily' },
    });
    check(rep, { 'report status is 200': (r) => r.status === 200 });
    reportRead.add(rep.timings.duration);
  }

  sleep(0.1);
}

export function handleSummary(data) {
  // Written to disk so a run can be attached to a PR. A number in a terminal
  // that nobody kept is not evidence of anything.
  return {
    'bench/last-run.json': JSON.stringify(data, null, 2),
    stdout: '\n' + textSummary(data),
  };
}

// k6 ships this helper but does not expose it by default.
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';

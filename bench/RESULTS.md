# Results

The numbers in this file are the evidence for the claim in the README. They are
deliberately not filled in from a run I am describing rather than one I did: a
p95 copied from memory, or worse from an expectation, is exactly the kind of
number this repository exists to argue against.

Run the load test, paste the output, and commit it. Until then the tables below
are blank, which is an honest state for them to be in.

## Reproducing

```sh
docker compose up -d --build
docker compose exec api npm run seed      # 500 accounts, 2,000,000 events
docker compose exec api npm test          # unit tests, no services needed
k6 run loadtest/metrics.js
```

`k6` exits non-zero if any threshold in `loadtest/metrics.js` is missed, so a
failed run is a failed command and not a judgement call. The full summary is
written to `bench/last-run.json` by `handleSummary`.

## The machine

Latency is a property of the whole system, not of the code, so a result without
the machine attached is not reproducible.

| | |
| --- | --- |
| CPU | |
| Cores allocated to Docker | |
| Memory allocated to Docker | |
| Storage | |
| Host OS / Docker version | |
| k6 version | |
| Run date | |

## Load profile

From `loadtest/metrics.js`, restated here so this file stands on its own:

- Ramping arrival rate, not fixed VUs. Throughput is held constant so latency is
  the variable; with fixed VUs a slow system reduces its own load and the graph
  flatters it.
- 30s ramp to 200 rps, 60s hold at 200 rps, 30s push to 400 rps, 10s down.
- The hold is the window that counts. The push past 400 is there to find the
  knee, not to be quoted.
- Traffic mix: 80% of reads hit five hot accounts, 20% spread across the seeded
  range, and roughly one iteration in ten also calls the uncached daily report.

## Thresholds

These are assertions, not targets. Each one fails the run.

| Metric | Threshold | Result |
| --- | --- | --- |
| `http_req_duration` p95 | < 200ms | |
| `latency_cached_read` p95 | < 50ms | |
| `latency_cold_read` p95 | < 200ms | |
| `latency_report` p95 | < 200ms | |
| `http_req_failed` | < 1% | |
| `cache_hit` rate | > 80% | |

The cache hit rate is a threshold on purpose. Without it a p95 measured against
a cache that was never hit describes a system nobody is running.

## Percentiles

| Path | avg | p50 | p90 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| Cached read | | | | | | |
| Cold read | | | | | | |
| Daily report | | | | | | |

The p99 and the max are here because quoting only the p95 hides the tail, and
the tail is what a user actually complains about.

## What invalidates a run

Worth writing down before there is a number to defend:

- Seeding fewer than 2,000,000 events. At small volumes every plan looks fast
  and the index choices in `db/schema.sql` stop mattering.
- Skipping `ANALYZE`. `db/seed.js` runs it; without fresh statistics the planner
  guesses and the first `EXPLAIN` describes a database that does not exist.
- Running k6 on a saturated host. If the load generator is competing with
  Postgres for cores, the measurement is of the contention.
- A cache hit rate near 100%. That means the hot set swallowed the whole run and
  Postgres was never asked anything; the cold read number is then meaningless.
- Reporting the ramp or the 400 rps push as the headline. Only the hold is the
  design point.

## Plans

The load test says the system is fast enough. It does not say why. `explain.md`
in this directory has the query plans, which is where the latency is actually
won or lost.

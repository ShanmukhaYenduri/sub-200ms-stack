# Plans

`RESULTS.md` shows that the read path is fast enough. This file is the argument
for why, which is the part that survives being ported to another schema.

Each section states the query, the index it is supposed to use, and the specific
line in the plan that proves it did. The plan output is left empty until it is
pasted from a real run against a seeded database -- a plan invented from what the
planner ought to do is worth less than no plan at all.

## Producing them

```sh
docker compose exec api npm run seed
docker compose exec postgres psql -U metrics -d metrics
```

`EXPLAIN (ANALYZE, BUFFERS)`, never bare `EXPLAIN`. Without `ANALYZE` the output
is the planner's opinion; with it you get what actually happened, and `BUFFERS`
is what distinguishes a fast query from a query whose data was already in cache.

Statistics have to be current. `db/seed.js` runs `ANALYZE` at the end for exactly
this reason: on stale statistics the planner guesses, and the plan you are reading
describes a database that does not exist.

---

## 1. The hot query

`hourlyMetric` in `src/queries.js`, behind `GET /metrics`. This is the one that
has to be quick, because it is the one that runs on every dashboard load.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT date_trunc('hour', recorded_at) AS bucket,
       count(*)::int AS samples,
       avg(value)::float8 AS avg_value,
       max(value)::float8 AS max_value
  FROM metric_events
 WHERE account_id = 101
   AND metric = 'latency_ms'
   AND recorded_at >= now() - interval '7 days'
 GROUP BY 1
 ORDER BY 1 DESC
 LIMIT 168;
```

Expected index: `metric_events_hot_path_idx (account_id, metric, recorded_at DESC)`.

What to look for:

- `Index Scan using metric_events_hot_path_idx`. A `Seq Scan` or a `Bitmap Heap
  Scan` here means the column order is being ignored, usually because one of the
  equality predicates was dropped from the query.
- **No `Sort` node.** This is the whole point of `recorded_at DESC` being last in
  the index: the rows arrive already ordered, so the sort disappears from the plan
  rather than being made faster. Reorder the index with `recorded_at` first and a
  `Sort` reappears along with a much wider scan.
- `rows removed by filter` near zero. A large number means the scan is reading a
  range it then throws away, which is a predicate the index is not serving.

```text

```

---

## 2. The rollup

`dailyRollup` in `src/queries.js`, behind `GET /reports/daily`. Honestly the
slower of the two, and not pretending otherwise.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT date_trunc('day', recorded_at) AS bucket,
       metric,
       count(*)::int AS samples,
       avg(value)::float8 AS avg_value,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY value)::float8 AS p95_value
  FROM metric_events
 WHERE account_id = 101
   AND recorded_at >= now() - interval '30 days'
 GROUP BY 1, 2
 ORDER BY 1 DESC, 2 ASC
 LIMIT 120;
```

Expected index: `metric_events_rollup_idx (account_id, recorded_at DESC) INCLUDE (value)`.

What to look for:

- `Index Only Scan using metric_events_rollup_idx`, with `Heap Fetches: 0`. That
  zero is what `INCLUDE (value)` bought: the aggregate is answered from the index
  leaf pages and the table is never touched. A non-zero count usually means the
  visibility map is stale, so run it again after a `VACUUM`.
- A `Sort` node that does **not** disappear, and should not be expected to.
  `percentile_cont` has to materialise and order each group; the index can feed
  the scan cheaply but cannot remove that work. This is the reason
  `loadtest/metrics.js` samples this route on roughly one iteration in ten rather
  than hammering it.

```text

```

---

## 3. The anomaly view

`anomalies` in `src/queries.js`, behind `GET /insights`.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT metric, window_start, window_end, score::float8 AS score
  FROM insights
 WHERE account_id = 101
   AND anomaly
   AND window_start >= now() - interval '7 days'
 ORDER BY window_start DESC
 LIMIT 50;
```

Expected index: `insights_anomaly_idx (account_id, window_start DESC) WHERE anomaly`.

What to look for:

- `Index Scan using insights_anomaly_idx`. A partial index is only usable when
  the query restates the condition it was built with, so dropping `AND anomaly`
  from the query silently loses the index. Worth confirming by deleting that line
  and watching the plan change.
- This table starts empty. Post some events through `POST /events` and let
  `src/consumer.js` score them before reading anything into this plan.

```text

```

---

## The index that is deliberately absent

`db/schema.sql` notes that `CREATE INDEX ON metric_events (metric)` is not
created. Four distinct values across two million rows means the planner ignores
it, while every insert still pays to maintain it. If you want to see that rather
than take it on faith:

```sql
CREATE INDEX tmp_metric_idx ON metric_events (metric);
ANALYZE metric_events;
-- re-run the query in section 1; the plan should be unchanged
DROP INDEX tmp_metric_idx;
```

An unused index is a write-path tax disguised as a read-path optimisation, and
the plan is how you tell the difference.

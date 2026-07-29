-- Schema for the metrics read path.
--
-- The tables here are deliberately boring. The interesting part of this file is
-- the index section at the bottom: that is where the latency budget is won or
-- lost. See bench/explain.md for the plans these produce and the numbers they
-- move.

CREATE TABLE IF NOT EXISTS accounts (
  id           BIGSERIAL   PRIMARY KEY,
  external_ref TEXT        NOT NULL UNIQUE,
  name         TEXT        NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS metric_events (
  id          BIGSERIAL     PRIMARY KEY,
  account_id  BIGINT        NOT NULL REFERENCES accounts(id),
  metric      TEXT          NOT NULL,
  value       NUMERIC(14,4) NOT NULL,
  recorded_at TIMESTAMPTZ   NOT NULL
);

CREATE TABLE IF NOT EXISTS insights (
  id           BIGSERIAL   PRIMARY KEY,
  account_id   BIGINT      NOT NULL REFERENCES accounts(id),
  metric       TEXT        NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  summary      TEXT        NOT NULL,
  anomaly      BOOLEAN     NOT NULL DEFAULT false,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency ledger for the consumer.
--
-- Replay is a feature of a log, not an accident, so the consumer will see the
-- same message twice on purpose after a bad deploy. Without this table, replay
-- means duplicate insights. With it, the second delivery is a no-op.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id     TEXT        PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- The hot query is:
--
--   WHERE account_id = $1 AND metric = $2 AND recorded_at >= $3
--   GROUP BY date_trunc('hour', recorded_at)
--   ORDER BY 1 DESC
--
-- Column order is not arbitrary:
--   * account_id and metric are equality predicates, so they come first;
--   * recorded_at is a range scan and the sort key, so it comes last and
--     carries DESC.
--
-- Ordered that way, Postgres seeks straight to the matching range and reads it
-- already sorted, which removes the sort node from the plan entirely. Put
-- recorded_at first instead and you get a much wider scan plus a sort.
CREATE INDEX IF NOT EXISTS metric_events_hot_path_idx
  ON metric_events (account_id, metric, recorded_at DESC);

-- Covering index for the cross-metric rollup. INCLUDE keeps value in the leaf
-- pages, so the aggregate is answered index-only and never touches the heap.
-- Heap fetches are the difference between tens of milliseconds and hundreds.
CREATE INDEX IF NOT EXISTS metric_events_rollup_idx
  ON metric_events (account_id, recorded_at DESC)
  INCLUDE (value);

-- Anomalies are a small fraction of rows and the dashboard only ever asks for
-- those. A partial index stays small enough to live in cache permanently.
CREATE INDEX IF NOT EXISTS insights_anomaly_idx
  ON insights (account_id, window_start DESC)
  WHERE anomaly;

-- Deliberately NOT created:
--
--   CREATE INDEX ON metric_events (metric);
--
-- Four distinct values across two million rows. The planner would ignore it,
-- and an index nobody reads still has to be maintained on every single write.
-- Unused indexes are a write-path tax disguised as a read-path optimisation.

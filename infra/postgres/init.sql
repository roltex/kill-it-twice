-- Kill It Twice schema
CREATE TABLE IF NOT EXISTS records (
  id            BIGSERIAL PRIMARY KEY,
  email         TEXT NOT NULL,
  name          TEXT NOT NULL,
  payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
  version       BIGINT NOT NULL DEFAULT 1,
  poison        BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_records_updated_at_id ON records (updated_at, id);

CREATE TABLE IF NOT EXISTS checkpoints (
  mode          TEXT PRIMARY KEY,          -- 'backfill' | 'incremental'
  cursor_id     BIGINT NOT NULL DEFAULT 0,
  cursor_ts     TIMESTAMPTZ,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO checkpoints (mode, cursor_id, cursor_ts)
VALUES
  ('backfill', 0, NULL),
  ('incremental', 0, TIMESTAMPTZ '1970-01-01')
ON CONFLICT (mode) DO NOTHING;

CREATE TABLE IF NOT EXISTS dlq (
  id            BIGSERIAL PRIMARY KEY,
  record_id     BIGINT NOT NULL,
  version       BIGINT NOT NULL,
  batch_id      TEXT NOT NULL,
  sink          TEXT NOT NULL DEFAULT 'elasticsearch',
  payload       JSONB NOT NULL,
  error         TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'open',  -- open | replayed | discarded
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  replayed_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq (status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_open_unique
  ON dlq (record_id, version, sink)
  WHERE status = 'open';

CREATE TABLE IF NOT EXISTS consumed_events (
  event_id        TEXT PRIMARY KEY,           -- recordId:version
  record_id       BIGINT NOT NULL,
  version         BIGINT NOT NULL,
  duplicate_hits  INT NOT NULL DEFAULT 0,
  consumed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumed_events_record ON consumed_events (record_id);

-- app/storage/schema.sql

CREATE TABLE IF NOT EXISTS bars (
  symbol       TEXT        NOT NULL,
  timeframe    TEXT        NOT NULL,
  ts           BIGINT      NOT NULL,   -- 统一毫秒
  open         DOUBLE PRECISION NOT NULL,
  high         DOUBLE PRECISION NOT NULL,
  low          DOUBLE PRECISION NOT NULL,
  close        DOUBLE PRECISION NOT NULL,
  volume       DOUBLE PRECISION NOT NULL,
  source       TEXT        NOT NULL DEFAULT 'okx',
  inserted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, timeframe, ts)
);

CREATE INDEX IF NOT EXISTS idx_bars_symbol_tf_ts
ON bars(symbol, timeframe, ts);
-- heartbeat: service liveness indicator
CREATE TABLE IF NOT EXISTS heartbeat (
  service_name TEXT PRIMARY KEY,
  last_seen_ts BIGINT NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

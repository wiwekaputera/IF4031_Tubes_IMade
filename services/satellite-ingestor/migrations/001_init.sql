-- Satellite ingestor schema. Owned by satellite-ingestor only.
-- Applied by the store test and by the service at startup.

-- One row: the durable processing state. next_offset is the resume point
-- for GET /v1/stream?from=, and it always commits together with the fire
-- changes in the same transaction.
CREATE TABLE IF NOT EXISTS satellite_state (
  id          INT PRIMARY KEY CHECK (id = 1),
  next_offset BIGINT NOT NULL DEFAULT 0,
  watermark   TIMESTAMPTZ,              -- newest detection time seen; NULL until the first one
  invalid     BIGINT NOT NULL DEFAULT 0,
  low_conf    BIGINT NOT NULL DEFAULT 0,
  late        BIGINT NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Added with update and retract handling. The ALTER keeps existing databases working.
ALTER TABLE satellite_state ADD COLUMN IF NOT EXISTS unknown BIGINT NOT NULL DEFAULT 0;

INSERT INTO satellite_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Accepted detections. det_key is the dedup key (satellite|date|time|lat|lon),
-- and the raw coordinate and time text is kept so the key rebuilds exactly.
CREATE TABLE IF NOT EXISTS satellite_detections (
  det_key        TEXT PRIMARY KEY,
  satellite      TEXT NOT NULL,
  acq_date       DATE NOT NULL,
  acq_time       TEXT NOT NULL,
  latitude_text  TEXT NOT NULL,
  longitude_text TEXT NOT NULL,
  confidence     TEXT NOT NULL,
  frp            DOUBLE PRECISION NOT NULL,
  acquired_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_satellite_detections_acquired
  ON satellite_detections (acquired_at);

-- Fires, keyed by the stable source_ref_id. Upserted on every change.
CREATE TABLE IF NOT EXISTS satellite_fires (
  source_ref_id   TEXT PRIMARY KEY,
  center_lat      DOUBLE PRECISION NOT NULL,
  center_lon      DOUBLE PRECISION NOT NULL,
  occurred_at     TIMESTAMPTZ NOT NULL,
  detection_count INT NOT NULL,
  satellites      TEXT[] NOT NULL,
  max_confidence  TEXT NOT NULL,
  total_frp_mw    DOUBLE PRECISION NOT NULL,
  detection_keys  TEXT[] NOT NULL,
  severity        TEXT NOT NULL CHECK (severity IN ('WASPADA', 'SIAGA', 'AWAS')),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest detection time of each fire, for queries and dashboards.
ALTER TABLE satellite_fires ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
UPDATE satellite_fires SET last_seen_at = occurred_at WHERE last_seen_at IS NULL;
ALTER TABLE satellite_fires ALTER COLUMN last_seen_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_satellite_fires_last_seen
  ON satellite_fires (last_seen_at);

-- Hazard messages for the aggregator, written in the same transaction as the
-- fire change and published to JetStream by the relay. message_id is also the
-- JetStream Nats-Msg-Id, so a publish repeated after a crash is dropped.
CREATE TABLE IF NOT EXISTS satellite_outbox (
  id           BIGSERIAL PRIMARY KEY,
  message_id   TEXT UNIQUE NOT NULL,
  subject      TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_satellite_outbox_unpublished
  ON satellite_outbox (id) WHERE published_at IS NULL;

-- Feed events that could not be processed: malformed or oversized payloads,
-- detections that fail validation, and messages that crashed the processor.
-- The raw bytes are kept exactly as received. The feed moves past them, so
-- one poison event cannot stall ingestion.
CREATE TABLE IF NOT EXISTS satellite_dead_letters (
  id          BIGSERIAL PRIMARY KEY,
  feed_offset BIGINT UNIQUE NOT NULL,
  event_type  TEXT NOT NULL,
  raw_payload BYTEA NOT NULL,
  reason      TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'parked' CHECK (status IN ('parked', 'resolved')),
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Numeric coordinates for the neighbour search. The text columns stay the
-- source of the dedup key; these are only for the box query.
ALTER TABLE satellite_detections ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
ALTER TABLE satellite_detections ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
UPDATE satellite_detections SET lat = latitude_text::double precision, lon = longitude_text::double precision
  WHERE lat IS NULL OR lon IS NULL;
ALTER TABLE satellite_detections ALTER COLUMN lat SET NOT NULL;
ALTER TABLE satellite_detections ALTER COLUMN lon SET NOT NULL;

-- Which fire holds each detection, so the processor can find the fires a
-- change touches by primary key instead of searching the key arrays.
CREATE TABLE IF NOT EXISTS satellite_fire_members (
  det_key       TEXT PRIMARY KEY,
  source_ref_id TEXT NOT NULL REFERENCES satellite_fires (source_ref_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_satellite_fire_members_fire
  ON satellite_fire_members (source_ref_id);

INSERT INTO satellite_fire_members (det_key, source_ref_id)
SELECT unnest(detection_keys), source_ref_id FROM satellite_fires
ON CONFLICT (det_key) DO NOTHING;

-- Retracted detections stay as marked rows, so a copy the feed re-sends later
-- is recognised as a duplicate instead of bringing a false positive back.
ALTER TABLE satellite_detections ADD COLUMN IF NOT EXISTS retracted BOOLEAN NOT NULL DEFAULT false;

-- Detections re-sent by the feed with a key that was already stored.
ALTER TABLE satellite_state ADD COLUMN IF NOT EXISTS duplicate BIGINT NOT NULL DEFAULT 0;

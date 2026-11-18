-- Canonical Store Initialization SQL (CockroachDB dialect)
-- Idempotent: safe to re-run on an already-initialized cluster.

CREATE DATABASE IF NOT EXISTS canonical;
CREATE USER IF NOT EXISTS aggregator;
GRANT ALL ON DATABASE canonical TO aggregator;

USE canonical;

-- ============================================================
-- Volcano reference table (static seed)
-- ============================================================
CREATE TABLE IF NOT EXISTS volcano_reference (
  volcano_id   STRING PRIMARY KEY,
  name         STRING NOT NULL,
  latitude     DOUBLE PRECISION NOT NULL,
  longitude    DOUBLE PRECISION NOT NULL
);

INSERT INTO volcano_reference (volcano_id, name, latitude, longitude) VALUES
  ('GNG-001', 'Gunung Merapi',   -7.5407,  110.4457),
  ('GNG-002', 'Gunung Semeru',   -8.1080,  112.9220),
  ('GNG-003', 'Gunung Krakatau', -6.1020,  105.4230),
  ('GNG-004', 'Gunung Rinjani',  -8.4167,  116.4667),
  ('GNG-005', 'Gunung Sinabung',  3.1700,   98.3920)
ON CONFLICT (volcano_id) DO NOTHING;

-- ============================================================
-- Canonical events table (8-shard logical partitioning)
-- ============================================================
CREATE TABLE IF NOT EXISTS canonical_events (
  hazard_id      STRING NOT NULL,
  shard          INT2 NOT NULL AS (mod(crc32ieee(hazard_id), 8)::INT2) STORED,
  source         STRING NOT NULL,
  source_ref_id  STRING NOT NULL,
  hazard_type    STRING NOT NULL,
  severity       STRING NOT NULL,
  area_name      STRING NOT NULL,
  latitude       DOUBLE PRECISION NOT NULL,
  longitude      DOUBLE PRECISION NOT NULL,
  occurred_at    TIMESTAMPTZ NOT NULL,
  ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  version        INT8 NOT NULL DEFAULT 1,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  sources        JSONB NOT NULL DEFAULT '{}',
  attributes     JSONB NOT NULL DEFAULT '{}',
  source_version INT8 NOT NULL DEFAULT 0,
  deleted_at     TIMESTAMPTZ NULL,
  written_by     STRING NULL,
  fencing_token  INT8 NULL,
  PRIMARY KEY (shard, hazard_id)
);

CREATE INDEX IF NOT EXISTS idx_canonical_events_shard_occurred
  ON canonical_events (shard, occurred_at DESC, hazard_id);

CREATE INDEX IF NOT EXISTS idx_canonical_events_shard_ingested
  ON canonical_events (shard, ingested_at, hazard_id);

CREATE INDEX IF NOT EXISTS idx_canonical_events_shard_source_ref
  ON canonical_events (shard, source, source_ref_id);

CREATE INDEX IF NOT EXISTS idx_canonical_events_shard_type_time
  ON canonical_events (shard, hazard_type, occurred_at DESC);

ALTER TABLE canonical_events SPLIT AT VALUES (1), (2), (3), (4), (5), (6), (7);
ALTER INDEX canonical_events@idx_canonical_events_shard_occurred SPLIT AT VALUES (1), (2), (3), (4), (5), (6), (7);
ALTER INDEX canonical_events@idx_canonical_events_shard_ingested SPLIT AT VALUES (1), (2), (3), (4), (5), (6), (7);
ALTER INDEX canonical_events@idx_canonical_events_shard_source_ref SPLIT AT VALUES (1), (2), (3), (4), (5), (6), (7);
ALTER INDEX canonical_events@idx_canonical_events_shard_type_time SPLIT AT VALUES (1), (2), (3), (4), (5), (6), (7);

-- ============================================================
-- Transactional outbox table
-- ============================================================
CREATE TABLE IF NOT EXISTS outbox (
  id           INT8 DEFAULT unique_rowid() PRIMARY KEY,
  event_key    STRING UNIQUE NOT NULL,
  payload      JSONB NOT NULL,
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
  ON outbox (id) WHERE published_at IS NULL;

-- ============================================================
-- Leader election & fencing tables (M2 P2)
-- ============================================================
CREATE TABLE IF NOT EXISTS leader_leases (
  name          STRING PRIMARY KEY,
  holder        STRING NOT NULL DEFAULT '',
  fencing_token INT8 NOT NULL DEFAULT 0,
  expires_at    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'
);

INSERT INTO leader_leases (name) VALUES ('ingest-poller')
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS poller_watermarks (
  source        STRING PRIMARY KEY,
  watermark     TIMESTAMPTZ NOT NULL,
  fencing_token INT8 NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- Cluster settings & replication
-- ============================================================
GRANT ALL ON ALL TABLES IN SCHEMA canonical.public TO aggregator;

SET CLUSTER SETTING server.time_until_store_dead = '1m15s';
ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;

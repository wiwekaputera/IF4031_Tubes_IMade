-- IF4031 Milestone 1 — Token DB init SQL
-- Owned by: token-service
-- DO NOT run manually; mounted as /docker-entrypoint-initdb.d/init.sql

CREATE TABLE IF NOT EXISTS refresh_tokens (
  id            BIGSERIAL PRIMARY KEY,
  token_hash    TEXT UNIQUE NOT NULL,      -- SHA-256 of the opaque token
  client_id     TEXT NOT NULL,
  scope         TEXT NOT NULL,             -- 'media' | 'field_team' | 'internal_ops'
  issued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at    TIMESTAMPTZ NOT NULL,
  revoked_at    TIMESTAMPTZ,               -- NULL = still valid
  replaced_by   TEXT                       -- token_hash of the new token on rotation
);

CREATE INDEX IF NOT EXISTS idx_refresh_tokens_hash
  ON refresh_tokens (token_hash) WHERE revoked_at IS NULL;

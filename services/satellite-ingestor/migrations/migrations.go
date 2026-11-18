// Package migrations holds the satellite-ingestor schema. The SQL is embedded
// so the binary carries its own schema and needs no files at runtime.
package migrations

import _ "embed"

// Init is the initial schema, applied at startup. It is idempotent.
//
//go:embed 001_init.sql
var Init string

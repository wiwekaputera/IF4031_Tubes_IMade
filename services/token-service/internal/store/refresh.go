package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrTokenNotFound = errors.New("refresh token not found")

	ErrTokenExpired = errors.New("refresh token expired")

	ErrTokenReused = errors.New("refresh token reused (possible theft)")
)

// Store defines operations on the refresh token database.
type Store interface {
	Insert(ctx context.Context, tokenHash, clientID, scope string, expiresAt time.Time) error
	Rotate(ctx context.Context, oldHash, newHash, clientID, scope string, newExpiresAt time.Time) error
	RevokeAllForClient(ctx context.Context, clientID string) error
}

// PGStore implements Store backed by PostgreSQL (token-db).
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a new PostgreSQL-backed refresh token store.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// Insert inserts a newly issued refresh token record.
func (s *PGStore) Insert(ctx context.Context, tokenHash, clientID, scope string, expiresAt time.Time) error {
	query := `
		INSERT INTO refresh_tokens (token_hash, client_id, scope, expires_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := s.pool.Exec(ctx, query, tokenHash, clientID, scope, expiresAt)
	if err != nil {
		return fmt.Errorf("failed to insert refresh token: %w", err)
	}
	return nil
}

// Rotate performs single-transaction rotation of an existing refresh token with reuse detection.
func (s *PGStore) Rotate(ctx context.Context, oldHash, newHash, clientID, scope string, newExpiresAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id int64
	var recClientID string
	var recScope string
	var expiresAt time.Time
	var revokedAt *time.Time
	var replacedBy *string

	selectQuery := `
		SELECT id, client_id, scope, expires_at, revoked_at, replaced_by
		FROM refresh_tokens
		WHERE token_hash = $1
		FOR UPDATE
	`

	err = tx.QueryRow(ctx, selectQuery, oldHash).Scan(&id, &recClientID, &recScope, &expiresAt, &revokedAt, &replacedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTokenNotFound
	}
	if err != nil {
		return fmt.Errorf("failed to select refresh token: %w", err)
	}

	if recClientID != clientID {
		return ErrTokenNotFound
	}

	if revokedAt != nil {
		revokeAllQuery := `
			UPDATE refresh_tokens
			SET revoked_at = now()
			WHERE client_id = $1 AND revoked_at IS NULL
		`
		_, _ = tx.Exec(ctx, revokeAllQuery, clientID)
		_ = tx.Commit(ctx)
		return ErrTokenReused
	}

	if time.Now().After(expiresAt) {
		_, _ = tx.Exec(ctx, "UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1", id)
		_ = tx.Commit(ctx)
		return ErrTokenExpired
	}

	updateQuery := `
		UPDATE refresh_tokens
		SET revoked_at = now(), replaced_by = $2
		WHERE id = $1
	`
	if _, err := tx.Exec(ctx, updateQuery, id, newHash); err != nil {
		return fmt.Errorf("failed to revoke old token: %w", err)
	}

	insertQuery := `
		INSERT INTO refresh_tokens (token_hash, client_id, scope, expires_at)
		VALUES ($1, $2, $3, $4)
	`
	if _, err := tx.Exec(ctx, insertQuery, newHash, clientID, scope, newExpiresAt); err != nil {
		return fmt.Errorf("failed to insert new refresh token: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit token rotation: %w", err)
	}

	return nil
}

// RevokeAllForClient revokes all active refresh tokens for the given client.
func (s *PGStore) RevokeAllForClient(ctx context.Context, clientID string) error {
	query := `
		UPDATE refresh_tokens
		SET revoked_at = now()
		WHERE client_id = $1 AND revoked_at IS NULL
	`
	_, err := s.pool.Exec(ctx, query, clientID)
	if err != nil {
		return fmt.Errorf("failed to revoke tokens for client %s: %w", clientID, err)
	}
	return nil
}

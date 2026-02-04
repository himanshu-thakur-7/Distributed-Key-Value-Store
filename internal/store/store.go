package store

import (
	"database/sql"
	"time"
)

// TombstoneTime is the sentinel value used for soft deletes
// Any key with expires_at = TombstoneTime is considered deleted
var TombstoneTime = time.Unix(0, 0).UTC()

// Store handles all PostgreSQL operations for the KV store
type Store struct {
	db *sql.DB
}

// New creates a new Store instance
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Get retrieves a key from the database
// Only returns keys that are alive (not tombstoned, not expired)
// Returns (value, expiresAt, nil) if found
// Returns (nil, nil, nil) if not found or expired/deleted
func (s *Store) Get(key string) ([]byte, *time.Time, error) {
	var value []byte
	var expiresAt *time.Time

	// Only select keys that are alive:
	// - expires_at IS NULL (no TTL, not deleted)
	// - expires_at > NOW() (TTL in future)
	err := s.db.QueryRow(`
		SELECT value, expires_at 
		FROM kv 
		WHERE key = $1 
		  AND (expires_at IS NULL OR expires_at > NOW())
	`, key).Scan(&value, &expiresAt)

	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	return value, expiresAt, nil
}

// Set stores a key-value pair without TTL
func (s *Store) Set(key string, value []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO kv (key, value, expires_at) 
		VALUES ($1, $2, NULL)
		ON CONFLICT (key) 
		DO UPDATE SET value = $2, expires_at = NULL
	`, key, value)
	return err
}

// SetWithTTL stores a key-value pair with a TTL
func (s *Store) SetWithTTL(key string, value []byte, ttlSeconds int64) (time.Time, error) {
	expiresAt := time.Now().Add(time.Duration(ttlSeconds) * time.Second)

	_, err := s.db.Exec(`
		INSERT INTO kv (key, value, expires_at) 
		VALUES ($1, $2, $3)
		ON CONFLICT (key) 
		DO UPDATE SET value = $2, expires_at = $3
	`, key, value, expiresAt)

	return expiresAt, err
}

// SoftDelete marks a key as deleted by setting expires_at to TombstoneTime
// This is a cheap UPDATE operation - no B+tree rebalancing
func (s *Store) SoftDelete(key string) error {
	_, err := s.db.Exec(`
		UPDATE kv 
		SET expires_at = $1 
		WHERE key = $2
	`, TombstoneTime, key)
	return err
}

// HardDelete physically removes a key from the database
// Use sparingly - causes B+tree rebalancing
func (s *Store) HardDelete(key string) error {
	_, err := s.db.Exec(`DELETE FROM kv WHERE key = $1`, key)
	return err
}

// HardDeleteBatch removes multiple expired/tombstoned keys in a single operation
// This is the only place where physical deletes should happen
// Returns the number of rows deleted
func (s *Store) HardDeleteBatch(limit int) (int64, error) {
	result, err := s.db.Exec(`
		DELETE FROM kv 
		WHERE key IN (
			SELECT key FROM kv 
			WHERE expires_at IS NOT NULL 
			  AND expires_at <= NOW() 
			LIMIT $1
		)
	`, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// SampleExpiredKeys returns a random sample of keys with TTL
// Used for Redis-style probabilistic expiration
func (s *Store) SampleExpiredKeys(sampleSize int) (total int, expired int, err error) {
	rows, err := s.db.Query(`
		SELECT expires_at 
		FROM kv 
		WHERE expires_at IS NOT NULL 
		ORDER BY random() 
		LIMIT $1
	`, sampleSize)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	now := time.Now()
	for rows.Next() {
		var expiresAt time.Time
		if err := rows.Scan(&expiresAt); err != nil {
			continue
		}
		total++
		if now.After(expiresAt) {
			expired++
		}
	}

	return total, expired, rows.Err()
}

// GetDB returns the underlying database connection
// Useful for running custom queries or migrations
func (s *Store) GetDB() *sql.DB {
	return s.db
}

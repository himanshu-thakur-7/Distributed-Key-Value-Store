package engine

import (
	"fmt"
	"time"

	"kvstore/internal/cache"
	"kvstore/internal/store"
)

// Engine is the main KV engine that coordinates cache and store
type Engine struct {
	cache *cache.Cache
	store *store.Store
}

// New creates a new Engine instance
func New(c *cache.Cache, s *store.Store) *Engine {
	return &Engine{
		cache: c,
		store: s,
	}
}

// Get retrieves a value by key
// Read path:
//  1. Check cache → if hit and not expired, return
//  2. If cache hit but expired → soft delete, return nil
//  3. If cache miss → check DB
//  4. If DB hit and not expired → populate cache, return
//  5. If DB hit but expired → soft delete, return nil (shouldn't happen with proper query)
func (e *Engine) Get(key string) ([]byte, error) {
	start := time.Now()

	// 1. Cache lookup
	if entry, ok := e.cache.Get(key); ok {
		// Check if expired
		if e.cache.IsExpired(entry) {
			// Soft delete: mark as tombstone in DB, remove from cache
			if err := e.store.SoftDelete(key); err != nil {
				fmt.Printf("[GET] key=%s soft delete error: %v\n", key, err)
			}
			e.cache.Delete(key)
			fmt.Printf("[GET] key=%s expired (cache) time=%s\n", key, time.Since(start))
			return nil, nil
		}

		fmt.Printf("[GET] key=%s source=cache time=%s\n", key, time.Since(start))
		return entry.Value, nil
	}

	// 2. DB lookup (query already filters expired/tombstoned keys)
	value, expiresAt, err := e.store.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		fmt.Printf("[GET] key=%s source=db (not found) time=%s\n", key, time.Since(start))
		return nil, nil
	}

	// 3. Populate cache
	e.cache.Set(key, value, expiresAt)

	fmt.Printf("[GET] key=%s source=db time=%s\n", key, time.Since(start))
	return value, nil
}

// Set stores a key-value pair without TTL
func (e *Engine) Set(key string, value []byte) error {
	start := time.Now()

	// Write to DB first (source of truth)
	if err := e.store.Set(key, value); err != nil {
		return err
	}

	// Update cache
	e.cache.Set(key, value, nil)

	fmt.Printf("[SET] key=%s time=%s\n", key, time.Since(start))
	return nil
}

// SetWithTTL stores a key-value pair with a TTL in seconds
func (e *Engine) SetWithTTL(key string, value []byte, ttlSeconds int64) error {
	start := time.Now()

	// Write to DB first
	expiresAt, err := e.store.SetWithTTL(key, value, ttlSeconds)
	if err != nil {
		return err
	}

	// Update cache with expiry
	e.cache.Set(key, value, &expiresAt)

	fmt.Printf("[SET] key=%s ttl=%ds time=%s\n", key, ttlSeconds, time.Since(start))
	return nil
}

// Delete removes a key (soft delete)
// This is cheap - just marks the key with tombstone timestamp
func (e *Engine) Delete(key string) error {
	start := time.Now()

	// Soft delete in DB (cheap UPDATE)
	if err := e.store.SoftDelete(key); err != nil {
		return err
	}

	// Remove from cache
	e.cache.Delete(key)

	fmt.Printf("[DEL] key=%s (soft) time=%s\n", key, time.Since(start))
	return nil
}

// GetCache returns the underlying cache (for testing/debugging)
func (e *Engine) GetCache() *cache.Cache {
	return e.cache
}

// GetStore returns the underlying store (for testing/debugging)
func (e *Engine) GetStore() *store.Store {
	return e.store
}

package cache

import (
	"sync"
	"time"
)

// Entry represents a cached key-value pair with optional expiration
type Entry struct {
	Value     []byte
	ExpiresAt *time.Time
}

// Cache is a thread-safe in-memory cache using sync.Map
type Cache struct {
	data sync.Map
}

// New creates a new Cache instance
func New() *Cache {
	return &Cache{}
}

// Get retrieves a value from cache
// Returns (entry, true) if found, (nil, false) if not found
func (c *Cache) Get(key string) (*Entry, bool) {
	val, ok := c.data.Load(key)
	if !ok {
		return nil, false
	}
	entry := val.(*Entry)
	return entry, true
}

// Set stores a value in cache
func (c *Cache) Set(key string, value []byte, expiresAt *time.Time) {
	c.data.Store(key, &Entry{
		Value:     value,
		ExpiresAt: expiresAt,
	})
}

// Delete removes a key from cache
func (c *Cache) Delete(key string) {
	c.data.Delete(key)
}

// IsExpired checks if an entry has expired
func (c *Cache) IsExpired(entry *Entry) bool {
	if entry == nil || entry.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*entry.ExpiresAt)
}

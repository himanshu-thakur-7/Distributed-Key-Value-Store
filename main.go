package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// cache entry with expiration
type CacheEntry struct {
	value     []byte
	expiresAt *time.Time
}

type Store struct {
	cache sync.Map
	db    *sql.DB
}

// constructor method
func NewStore(db *sql.DB) *Store {
	return &Store{
		db: db,
	}
}

// StartExpiryWorker starts a background routine to clean up expired keys
func (s *Store) StartExpiryWorker(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		fmt.Printf("Expiry worker started (interval: %v)\n", interval)

		for range ticker.C {
			s.cleanupCycle()
		}
	}()
}

func (s *Store) cleanupCycle() {
	// STEP 1: Sample 20 random keys with TTL (include expired ones for counting)
	rows, err := s.db.Query(`
		SELECT key, expires_at
		FROM kv
		WHERE expires_at IS NOT NULL
		ORDER BY random()
		LIMIT 20
	`)
	if err != nil {
		fmt.Println("[EXPIRY] Sample Error:", err)
		return
	}

	defer rows.Close()

	// STEP 2: count expired keys
	now := time.Now()
	totalSampled := 0
	expiredCount := 0

	for rows.Next() {
		var key string
		var expiresAt time.Time

		if err := rows.Scan(&key, &expiresAt); err != nil {
			continue
		}

		totalSampled++
		if now.After(expiresAt) {
			expiredCount++
		}
	}

	if totalSampled == 0 {
		return
	}

	// Step 3: Calculate ratio
	ratio := float64(expiredCount) / float64(totalSampled)

	// Step 4: Decide whether to cleanup
	if ratio < 0.25 {
		fmt.Printf("[EXPIRY] sampled %d keys, %d expired (%.0f%%) -> Skipping\n", totalSampled, expiredCount, ratio*100)
		return
	}
	fmt.Printf("[EXPIRY] sampled %d keys, %d expired (%.0f%%) -> Cleaning\n", totalSampled, expiredCount, ratio*100)

	// Step 5: Hard Delete expired keys
	s.hardDeleteBatch(500)

}

func (s *Store) hardDeleteBatch(limit int) {
	result, err := s.db.Exec(`
		DELETE from kv
		WHERE expires_at IS NOT NULL
		AND expires_at <= NOW()
		LIMIT $1
	`, limit)

	if err != nil {
		fmt.Println("[EXPIRY] Delete error:", err)
		return
	}
	rowsDeleted, _ := result.RowsAffected()
	if rowsDeleted > 0 {
		fmt.Printf("[EXPIRY] Hard deleted %d keys\n", rowsDeleted)
	}

}

func connectDB() *sql.DB {
	connStr := "postgres://kvuser:kvpass@localhost:5432/kvdb?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		panic(fmt.Sprintf("Cannot connect to db: %v", err))
	}
	fmt.Println("connected to postgres")
	return db
}

// Tombstone time for soft delete
var TombstoneTime = time.Unix(0, 0).UTC()

// soft delete
func (s *Store) softDelete(key string) {
	s.db.Exec("UPDATE kv SET expires_at = $1 WHERE key=$2", TombstoneTime, key)
	s.cache.Delete(key)
}

// SET method write to DB without TTL
func (s *Store) Set(key string, value []byte) error {
	// write to db
	_, err := s.db.Exec(`
		INSERT INTO kv (key, value, expires_at) 
		VALUES ($1, $2, NULL) 
		ON CONFLICT (key) 
		DO UPDATE SET value = $2, expires_at = NULL
	`, key, value)
	if err != nil {
		return err
	}
	//  update cache
	s.cache.Store(key, CacheEntry{
		value:     value,
		expiresAt: nil,
	})
	return nil
}

// SET with TTL
func (s *Store) SetWithTTL(key string, value []byte, ttlSeconds int64) error {
	expires_at := time.Now().Add(time.Duration(ttlSeconds) * time.Second)

	_, err := s.db.Exec(`
	INSERT INTO kv (key,value,expires_at)
	VALUES ($1,$2,$3)
	ON CONFLICT (key)
	DO UPDATE SET value = $2, expires_at = $3
	`, key, value, expires_at)

	if err != nil {
		return err
	}
	s.cache.Store(key, CacheEntry{
		value:     value,
		expiresAt: &expires_at,
	})

	return nil
}

// GET method read from cache , fall back from db, with lazy expiration
func (s *Store) Get(key string) ([]byte, error) {
	// 1 try cache
	if val, ok := s.cache.Load(key); ok {
		entry := val.(CacheEntry)

		// check if expired
		if entry.expiresAt != nil && time.Now().After(*entry.expiresAt) {
			s.softDelete(key)
			return nil, fmt.Errorf("key not found")
		}
		return entry.value, nil
	}

	// 2 cache miss - read from DB
	var val []byte
	var expiresAt *time.Time

	err := s.db.QueryRow(`
		SELECT value, expires_at 
		FROM kv 
		WHERE key = $1 
		  AND (expires_at IS NULL OR expires_at > NOW())
	`, key).Scan(&val, &expiresAt)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("key not found")
	}
	if err != nil {
		return nil, err
	}

	// 3 populate cache
	s.cache.Store(key, CacheEntry{
		value:     val,
		expiresAt: expiresAt,
	})
	return val, nil

}

// DELETE method
func (s *Store) Delete(key string) error {
	// soft delete
	s.softDelete(key)
	return nil
}

// // GET method - without db
// func (s *Store) Get(key string) ([]byte, error) {
// 	value, ok := s.cache.Load(key)
// 	if !ok {
// 		return nil, fmt.Errorf("Key not found")
// 	}
// 	return value.([]byte), nil
// }

// // Set nethod. - without db

// func (s *Store) Set(key string, value []byte) {
// 	s.cache.Store(key, value)
// }

// // delete method. - without db
// func (s *Store) Delete(key string) {
// 	s.cache.Delete(key)
// }

func main() {
	// connect to db
	db := connectDB()
	defer db.Close()
	store := NewStore(db)

	// // Simulate concurrent access
	// go store.Set("user1", []byte("alice"))
	// go store.Set("user1", []byte("bob"))
	// go store.Set("user1", []byte("charlie"))

	// // Wait a bit for goroutines to finish
	// time.Sleep(100 * time.Millisecond)

	// Read values
	// user1, _ := store.Get("user1")
	// user2, _ := store.Get("user2")
	// user3, _ := store.Get("user3")

	// fmt.Println("User1:", string(user1))
	// fmt.Println("User2:", string(user2))
	// fmt.Println("User3:", string(user3))

	// fmt.Println("\n--- Testing TTL ---")

	// store.SetWithTTL("temp", []byte("expires-soon"), 3)
	// fmt.Println(" Set temp with 3 second TTL")

	// val, _ := store.Get("temp")
	// fmt.Println(" Immediate GET:", string(val))

	// time.Sleep(2 * time.Second)
	// val, _ = store.Get("temp")
	// fmt.Println(" After 2 seconds:", string(val))

	// time.Sleep(2 * time.Second)
	// val, err := store.Get("temp")
	// if err != nil {
	// 	fmt.Println(" After 4 seconds:", err)
	// }

	// // Test permanent key
	// fmt.Println("\n--- Testing Permanent Key ---")
	// store.Set("permanent", []byte("no-expiry"))
	// val, _ = store.Get("permanent")
	// fmt.Println(" Permanent key:", string(val))

	store.StartExpiryWorker(3 * time.Second)

	// Test: Create 200 keys with SHORT 1-second TTL
	fmt.Println("\n--- Creating 200 keys with 1s TTL ---")
	for i := 0; i < 200; i++ {
		store.SetWithTTL(fmt.Sprintf("temp-%d", i), []byte("expires-fast"), 1)
	}
	fmt.Println("✓ Created 200 keys")

	// Wait 2 seconds for all to expire
	fmt.Println("\n--- Waiting 2 seconds for expiration... ---")
	time.Sleep(2 * time.Second)

	// Now worker should detect HIGH expiry ratio
	fmt.Println("\n--- Worker should trigger cleanup now... ---")
	time.Sleep(10 * time.Second)

	fmt.Println("\n✓ Test complete")

}

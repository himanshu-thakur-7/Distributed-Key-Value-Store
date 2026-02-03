package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type Cache struct {
	data sync.Map
}

type CacheEntry struct {
	value     []byte
	expiresAt *time.Time
}

type StoreTTL struct {
	db    *sql.DB
	cache *Cache
}

type Store struct {
	db    *sql.DB
	cache *Cache
}

func (s *Store) Set(key string, value []byte) error {
	_, err := s.db.Exec(
		"INSERT INTO kv (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value =$2",
		key, value,
	)
	if err != nil {
		return err
	}

	// update cache with CacheEntry (no expiry)
	s.cache.data.Store(key, CacheEntry{value: value, expiresAt: nil})
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	// 1. Cache lookup
	if val, ok := s.cache.data.Load(key); ok {
		ce := val.(CacheEntry)
		// If expiresAt is set and expired, delete and return nil
		if ce.expiresAt != nil && time.Now().After(*ce.expiresAt) {
			s.cache.data.Delete(key)
			s.db.Exec("DELETE FROM kv WHERE key = $1", key)
			return nil, nil
		}
		return ce.value, nil
	}

	// 2. DB lookup
	var val []byte
	err := s.db.QueryRow("SELECT value FROM kv WHERE key = $1", key).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// 3. Populate cache with CacheEntry (no expiry)
	s.cache.data.Store(key, CacheEntry{value: val, expiresAt: nil})

	return val, err
}

func (s *Store) Delete(key string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE key = $1", key)
	if err != nil {
		return err
	}

	s.cache.data.Delete(key)
	return err
}

func concurrentExecution(store Store) {
	var wg sync.WaitGroup

	workers := 10
	iterations := 10

	// writers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("key-%d", j%5)
				value := []byte(fmt.Sprintf("val-%d-%d", id, j))
				fmt.Printf("[WRITER] setting %s to %s \n", key, value)
				store.Set(key, value)
			}
		}(i)
	}

	// readers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("key-%d", j%5)
				val, err := store.Get(key)
				if err == nil {
					fmt.Printf("[READER] reading value of %s is %s \n", key, val)
				}
			}
		}()
	}

	wg.Wait()
	fmt.Println("Concurrrent test completed")
}

func (s *StoreTTL) SetWithTTL(key string, value []byte, ttlSeconds int64) error {
	expiresAt := time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	_, err := s.db.Exec(
		`INSERT INTO kv (key,value,expires_at)
	       VALUES ($1,$2,$3)
	       ON CONFLICT (key)
	       DO UPDATE SET value = $2, expires_at = $3
	       `, key, value, expiresAt,
	)

	if err != nil {
		return err
	}

	// update cache with expiry info
	s.cache.data.Store(key, CacheEntry{
		value:     value,
		expiresAt: &expiresAt,
	})

	return nil
}

func (s *StoreTTL) GetTTL(key string) ([]byte, error) {
	start := time.Now()

	// 1. Cache check
	if entry, ok := s.cache.data.Load(key); ok {
		ce := entry.(CacheEntry)

		if ce.expiresAt != nil && time.Now().After(*ce.expiresAt) {
			// expired
			s.cache.data.Delete(key)
			s.db.Exec("DELETE FROM kv WHERE key = $1", key)

			fmt.Printf("[GET] key=%s expired (cache)\n", key)

			return nil, nil
		}
		fmt.Printf("[GET] key=%s source=cache time=%s\n", key, time.Since(start))
		return ce.value, nil
	}

	// 2. DB lookup
	var val []byte
	var expiresAt *time.Time

	err := s.db.QueryRow(
		"SELECT value, expires_at FROM kv WHERE key = $1", key,
	).Scan(&val, &expiresAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	// 3. Expiry check
	if expiresAt != nil && time.Now().After(*expiresAt) {
		s.db.Exec("DELETE FROM kv WHERE key = $1", key)
		fmt.Printf("[GET] key=%s expired {db}\n", key)
	}

	// 4. Populate cache
	s.cache.data.Store(key, CacheEntry{
		value:     val,
		expiresAt: expiresAt,
	})

	fmt.Printf("[GET] key=%s source=db time=%s\n", key, time.Since(start))

	return val, nil
}

func TTLCodeExecution(storeTTL *StoreTTL) {
	storeTTL.SetWithTTL("temp", []byte("hello"), 2)

	storeTTL.GetTTL("temp")
	time.Sleep(3 * time.Second)
	storeTTL.GetTTL("temp")
}

func main() {
	db, err := sql.Open("postgres", "postgres://kvuser:kvpass@localhost:5432/kvdb?sslmode=disable")
	if err != nil {
		panic(err)
	}
	// store := &Store{db: db, cache: &Cache{}}

	storeTTL := &StoreTTL{db: db, cache: &Cache{}}

	// concurrentExecution(*store)
	TTLCodeExecution(storeTTL)
}

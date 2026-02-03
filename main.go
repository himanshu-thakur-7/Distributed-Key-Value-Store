package main

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
)

type Cache struct {
	data sync.Map
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

	// update cache
	s.cache.data.Store(key, value)
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	// start := time.Now()
	// 1. Cache lookup
	if val, ok := s.cache.data.Load(key); ok {
		// elapsed := time.Since(start)
		// fmt.Printf("{GET} key=%s source=blah blah time=%s\n", key, elapsed)
		return val.([]byte), nil
	}

	// 2. DB lookup
	var val []byte
	err := s.db.QueryRow("SELECT value FROM kv WHERE key = $1", key).Scan(&val)
	if err == sql.ErrNoRows {
		// elapsed := time.Since(start)
		// fmt.Printf("{GET} key=%s source=db time=%s\n", key, elapsed)
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	// 3. Populate cache
	s.cache.data.Store(key, val)

	// elapsed := time.Since(start)
	// fmt.Printf("{GET} key=%s source=db time=%s\n", key, elapsed)

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

func main() {
	db, err := sql.Open("postgres", "postgres://kvuser:kvpass@localhost:5432/kvdb?sslmode=disable")
	if err != nil {
		panic(err)
	}
	store := &Store{db: db, cache: &Cache{}}

	// store.Set("hello", []byte("world"))
	// val, _ := store.Get("hello")
	// log.Print(string(val))

	// store.Set("Louis", []byte("bar"))

	// v1, _ := store.Get("Louis") // first GET → DB
	// log.Print(string(v1))
	// v2, _ := store.Get("Louis") // first GET → DB
	// log.Print(string(v2))
	// v3, _ := store.Get("Louis") // first GET → DB
	// log.Print(string(v3))

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

package main

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
)

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

// SET method write to DB
func (s *Store) Set(key string, value []byte) error {
	// write to db
	_, err := s.db.Exec(`INSERT INTO kv (key, value) values ($1,$2) ON CONFLICT (key) DO UPDATE SET value = $2`,
		key, value)
	if err != nil {
		return err
	}
	//  update cache
	s.cache.Store(key, value)
	return nil
}

// GET method read from cache , fall back from db
func (s *Store) Get(key string) ([]byte, error) {
	// 1 try cache
	if val, ok := s.cache.Load(key); ok {
		return val.([]byte), nil
	}

	// 2 cachce miss
	var val []byte
	err := s.db.QueryRow("SELECT value FROM kv WHERE key = $1", key).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("key not found")
	}
	if err != nil {
		return nil, err
	}

	// 3 populate cache
	s.cache.Store(key, val)
	return val, nil

}

// DELETE method
func (s *Store) Delete(key string) error {
	// Delete from DB
	_, err := s.db.Exec("DELETE from kv WHERE key = $1", key)
	if err != nil {
		return err
	}

	// Delete from cache
	s.cache.Delete(key)
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
	user1, _ := store.Get("user1")
	user2, _ := store.Get("user2")
	user3, _ := store.Get("user3")

	fmt.Println("User1:", string(user1))
	fmt.Println("User2:", string(user2))
	fmt.Println("User3:", string(user3))
}

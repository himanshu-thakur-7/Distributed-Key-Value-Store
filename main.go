package main

import (
	"fmt"
	"sync"
	"time"
)

type Store struct {
	data sync.Map
}

// constructor method
func NewStore() *Store {
	return &Store{}
}

// GET method
func (s *Store) Get(key string) ([]byte, error) {
	value, ok := s.data.Load(key)
	if !ok {
		return nil, fmt.Errorf("Key not found")
	}
	return value.([]byte), nil
}

// Set nethod

func (s *Store) Set(key string, value []byte) {
	s.data.Store(key, value)
}

// delete method
func (s *Store) Delete(key string) {
	s.data.Delete(key)
}

func main() {
	store := NewStore()

	// Simulate concurrent access
	go store.Set("user1", []byte("alice"))
	go store.Set("user2", []byte("bob"))
	go store.Set("user3", []byte("charlie"))

	// Wait a bit for goroutines to finish
	time.Sleep(100 * time.Millisecond)

	// Read values
	user1, _ := store.Get("user1")
	user2, _ := store.Get("user2")
	user3, _ := store.Get("user3")

	fmt.Println("User1:", string(user1))
	fmt.Println("User2:", string(user2))
	fmt.Println("User3:", string(user3))
}

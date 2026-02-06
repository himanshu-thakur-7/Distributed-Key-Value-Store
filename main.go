package main

import "fmt"

type Store struct {
	data map[string][]byte
}

// constructor method
func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

// GET method
func (s *Store) Get(key string) ([]byte, error) {
	value := s.data[key]
	if value == nil {
		return nil, fmt.Errorf("Key not found")
	}
	return value, nil
}

// Set nethod

func (s *Store) Set(key string, value []byte) {
	s.data[key] = value
}

// delete method
func (s *Store) Delete(key string) {
	delete(s.data, key)
}

func main() {
	store := NewStore()

	// SET operation
	// store["username"] =
	store.Set("username", []byte("alice"))
	store.Set("email", []byte("alice@gmail.com"))

	// GET
	username, err := store.Get("username")
	if err == nil {
		fmt.Println("Username:", string(username))
	} else {
		fmt.Println("Error :", err)
	}

	// DEL
	store.Delete("email")

	// Try to get deleted key
	email, err := store.Get("email")
	if email == nil {
		fmt.Println("Email : (not found)")
	} else {
		fmt.Println("email: ", string(email))
	}
}

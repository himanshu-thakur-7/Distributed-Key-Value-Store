package main

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type Store struct {
	db *sql.DB
}

func (s *Store) Set(key string, value []byte) error {
	_, err := s.db.Exec(
		"INSERT INTO kv (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value =$2",
		key, value,
	)
	return err
}

func (s *Store) Get(key string) ([]byte, error) {
	var val []byte
	err := s.db.QueryRow("SELECT value FROM kv WHERE key = $1", key).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return val, err

}

func (s *Store) Delete(key string) error {
	_, err := s.db.Exec("DELETE FROM kv WHERE key = $1", key)
	return err
}

func main() {
	db, err := sql.Open("postgres", "postgres://kvuser:kvpass@localhost:5432/kvdb?sslmode=disable")
	if err != nil {
		panic(err)
	}
	store := &Store{db}

	store.Set("hello", []byte("world"))
	val, _ := store.Get("hello")
	log.Print(string(val))

}

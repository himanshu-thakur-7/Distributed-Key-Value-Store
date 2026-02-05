package main

import "fmt"

func Get(key string, store map[string][]byte) ([]byte, error) {
	value := store[key]
	if value == nil {
		return nil, fmt.Errorf("key not found")
	}
	return value, nil
}

func Set(key string, value []byte, store map[string][]byte) {
	store[key] = value
}

func Delete(key string, store map[string][]byte) {
	delete(store, key)
}

func main() {
	store := make(map[string][]byte)

	// SET operation
	// store["username"] =
	Set("username", []byte("alice"), store)
	Set("email", []byte("alice@gmail.com"), store)

	// GET
	username, err := Get("username", store)
	if err == nil {
		fmt.Println("Username:", string(username))
	} else {
		fmt.Println("Error :", err)
	}

	// DEL
	Delete("email", store)

	// Try to get deleted key
	email, err := Get("email", store)
	if email == nil {
		fmt.Println("Email : (not found)")
	} else {
		fmt.Println("email: ", string(email))
	}
}

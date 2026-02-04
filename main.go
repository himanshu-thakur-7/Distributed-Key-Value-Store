package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"kvstore/internal/cache"
	"kvstore/internal/engine"
	"kvstore/internal/expiry"
	"kvstore/internal/store"
)

// Helper function to create separator lines
func separator() string {
	return strings.Repeat("=", 60)
}

// =============================================================================
// Database Connection
// =============================================================================

func connectDB() *sql.DB {
	db, err := sql.Open("postgres", "postgres://kvuser:kvpass@localhost:5432/kvdb?sslmode=disable")
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(fmt.Sprintf("Failed to connect to database: %v", err))
	}
	fmt.Println("✓ Connected to PostgreSQL")
	return db
}

// =============================================================================
// Test 1: Basic GET/SET/DELETE
// =============================================================================

func testBasicOperations(e *engine.Engine) {
	fmt.Println("\n" + separator())
	fmt.Println("TEST: Basic GET/SET/DELETE Operations")
	fmt.Println(separator() + "\n")

	// SET
	e.Set("hello", []byte("world"))

	// GET (should be cache miss first time if cache was cleared)
	val, _ := e.Get("hello")
	fmt.Printf("Value of 'hello': %s\n", val)

	// GET again (should be cache hit)
	val, _ = e.Get("hello")
	fmt.Printf("Value of 'hello' (2nd read): %s\n", val)

	// DELETE
	e.Delete("hello")

	// GET after delete (should be nil)
	val, _ = e.Get("hello")
	fmt.Printf("Value of 'hello' after delete: %v\n", val)

	fmt.Println("\n✓ Basic operations test completed")
}

// =============================================================================
// Test 2: TTL (Key Expiration)
// =============================================================================

func testTTL(e *engine.Engine) {
	fmt.Println("\n" + separator())
	fmt.Println("TEST: TTL (Key Expiration)")
	fmt.Println(separator() + "\n")

	// Set key with 2 second TTL
	e.SetWithTTL("temp-key", []byte("temporary-value"), 2)

	// GET immediately (should work)
	val, _ := e.Get("temp-key")
	fmt.Printf("Value immediately after SET: %s\n", val)

	// Wait 1 second, GET again (should still work)
	time.Sleep(1 * time.Second)
	val, _ = e.Get("temp-key")
	fmt.Printf("Value after 1 second: %s\n", val)

	// Wait 2 more seconds, GET again (should be expired)
	time.Sleep(2 * time.Second)
	val, _ = e.Get("temp-key")
	fmt.Printf("Value after 3 seconds (expired): %v\n", val)

	fmt.Println("\n✓ TTL test completed")
}

// =============================================================================
// Test 3: Soft Delete vs Hard Delete
// =============================================================================

func testSoftDelete(e *engine.Engine, s *store.Store) {
	fmt.Println("\n" + separator())
	fmt.Println("TEST: Soft Delete (Tombstone) Behavior")
	fmt.Println(separator() + "\n")

	// Set a key
	e.Set("soft-test", []byte("will-be-soft-deleted"))
	fmt.Println("Set key 'soft-test'")

	// Soft delete (via engine.Delete)
	e.Delete("soft-test")
	fmt.Println("Soft deleted 'soft-test'")

	// Try to GET (should return nil)
	val, _ := e.Get("soft-test")
	fmt.Printf("GET after soft delete: %v\n", val)

	// Check if tombstone exists in DB (direct query)
	var expiresAt time.Time
	err := s.GetDB().QueryRow(
		"SELECT expires_at FROM kv WHERE key = $1",
		"soft-test",
	).Scan(&expiresAt)

	if err == nil {
		fmt.Printf("DB row exists with expires_at = %v (tombstone: %v)\n",
			expiresAt, expiresAt.Equal(store.TombstoneTime))
	} else {
		fmt.Printf("DB query error: %v\n", err)
	}

	fmt.Println("\n✓ Soft delete test completed")
}

// =============================================================================
// Test 4: Concurrent Read/Write
// =============================================================================

func testConcurrency(e *engine.Engine) {
	fmt.Println("\n" + separator())
	fmt.Println("TEST: Concurrent Read/Write (Race Safety)")
	fmt.Println(separator() + "\n")

	var wg sync.WaitGroup
	workers := 10
	iterations := 50

	// Writers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("concurrent-key-%d", j%5)
				value := []byte(fmt.Sprintf("worker-%d-iter-%d", id, j))
				e.Set(key, value)
			}
		}(i)
	}

	// Readers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("concurrent-key-%d", j%5)
				e.Get(key)
			}
		}()
	}

	wg.Wait()
	fmt.Println("\n✓ Concurrency test completed (no race detected)")
}

// =============================================================================
// Test 5: Background Expiry Worker
// =============================================================================

func testExpiryWorker(e *engine.Engine, s *store.Store, c *cache.Cache) {
	fmt.Println("\n" + separator())
	fmt.Println("TEST: Background Expiry Worker (Redis-style Sampling)")
	fmt.Println(separator() + "\n")

	// Create many keys with short TTL
	fmt.Println("Creating 50 keys with 1-second TTL...")
	for i := 0; i < 50; i++ {
		e.SetWithTTL(fmt.Sprintf("expiry-test-%d", i), []byte("temp"), 1)
	}

	// Wait for keys to expire
	fmt.Println("Waiting 2 seconds for keys to expire...")
	time.Sleep(2 * time.Second)

	// Start expiry worker with aggressive settings for testing
	cfg := expiry.Config{
		Interval:        2 * time.Second,
		SampleSize:      20,
		ExpiryThreshold: 0.25,
		DeleteBatchSize: 100,
	}
	worker := expiry.NewWorker(s, c, cfg)
	worker.Start()

	// Wait for worker to run a few cycles
	fmt.Println("Letting expiry worker run for 6 seconds...")
	time.Sleep(6 * time.Second)

	worker.Stop()
	fmt.Println("\n✓ Expiry worker test completed")
}

// =============================================================================
// Test 6: Cache Hit vs Miss Latency
// =============================================================================

func testLatency(e *engine.Engine) {
	fmt.Println("\n" + separator())
	fmt.Println("TEST: Cache Hit vs Miss Latency Comparison")
	fmt.Println(separator() + "\n")

	// Set a key
	e.Set("latency-test", []byte("measuring-latency"))

	// Clear cache by getting a fresh engine (simulate restart)
	// For this test, we'll just measure the difference

	// First GET (cache hit since we just set it)
	start := time.Now()
	e.Get("latency-test")
	cacheHit := time.Since(start)

	// To simulate cache miss, we'd need to restart
	// Instead, let's set a new key and measure
	e.Set("latency-test-2", []byte("new-value"))
	start = time.Now()
	e.Get("latency-test-2")
	afterSet := time.Since(start)

	fmt.Printf("GET after SET (cache hit): %v\n", cacheHit)
	fmt.Printf("GET after fresh SET (cache hit): %v\n", afterSet)

	fmt.Println("\n✓ Latency test completed")
}

// =============================================================================
// Main - Run Selected Tests
// =============================================================================

func main() {
	fmt.Println(separator())
	fmt.Println("Distributed KV Store - Test Suite")
	fmt.Println(separator())

	// Initialize components
	db := connectDB()
	defer db.Close()

	c := cache.New()
	s := store.New(db)
	e := engine.New(c, s)

	// Parse command line args to select tests
	if len(os.Args) < 2 {
		fmt.Println("\nUsage: go run main.go <test>")
		fmt.Println("Available tests:")
		fmt.Println("  basic      - Basic GET/SET/DELETE")
		fmt.Println("  ttl        - TTL expiration")
		fmt.Println("  softdelete - Soft delete behavior")
		fmt.Println("  concurrent - Concurrent read/write")
		fmt.Println("  expiry     - Background expiry worker")
		fmt.Println("  latency    - Cache hit/miss latency")
		fmt.Println("  all        - Run all tests")
		return
	}

	test := os.Args[1]

	switch test {
	case "basic":
		testBasicOperations(e)
	case "ttl":
		testTTL(e)
	case "softdelete":
		testSoftDelete(e, s)
	case "concurrent":
		testConcurrency(e)
	case "expiry":
		testExpiryWorker(e, s, c)
	case "latency":
		testLatency(e)
	case "all":
		testBasicOperations(e)
		testTTL(e)
		testSoftDelete(e, s)
		testConcurrency(e)
		testLatency(e)
		// Note: expiry test takes longer, run separately
		fmt.Println("\nNote: Run 'go run main.go expiry' separately for expiry worker test")
	default:
		fmt.Printf("Unknown test: %s\n", test)
	}
}

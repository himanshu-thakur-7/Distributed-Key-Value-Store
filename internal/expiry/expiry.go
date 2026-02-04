package expiry

import (
	"fmt"
	"time"

	"kvstore/internal/cache"
	"kvstore/internal/store"
)

// Config holds the configuration for the expiry worker
type Config struct {
	// Interval between expiry checks
	Interval time.Duration

	// SampleSize is the number of keys to sample each cycle
	SampleSize int

	// ExpiryThreshold is the ratio of expired keys that triggers aggressive cleanup
	// e.g., 0.25 means if >25% of sampled keys are expired, run hard delete
	ExpiryThreshold float64

	// DeleteBatchSize is the max number of keys to hard delete per cycle
	DeleteBatchSize int
}

// DefaultConfig returns sensible defaults (Redis-like)
func DefaultConfig() Config {
	return Config{
		Interval:        5 * time.Second,
		SampleSize:      20,
		ExpiryThreshold: 0.25, // 25%
		DeleteBatchSize: 500,
	}
}

// Worker handles background expiration of keys
type Worker struct {
	store  *store.Store
	cache  *cache.Cache
	config Config
	stopCh chan struct{}
}

// NewWorker creates a new expiry worker
func NewWorker(s *store.Store, c *cache.Cache, cfg Config) *Worker {
	return &Worker{
		store:  s,
		cache:  c,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins the background expiry process
func (w *Worker) Start() {
	go func() {
		ticker := time.NewTicker(w.config.Interval)
		defer ticker.Stop()

		fmt.Printf("[EXPIRY] Worker started (interval=%s, sample=%d, threshold=%.0f%%)\n",
			w.config.Interval, w.config.SampleSize, w.config.ExpiryThreshold*100)

		for {
			select {
			case <-ticker.C:
				w.runCycle()
			case <-w.stopCh:
				fmt.Println("[EXPIRY] Worker stopped")
				return
			}
		}
	}()
}

// Stop gracefully stops the expiry worker
func (w *Worker) Stop() {
	close(w.stopCh)
}

// runCycle performs one expiry check cycle (Redis-style sampling)
func (w *Worker) runCycle() {
	// Step 1: Sample keys with TTL
	total, expired, err := w.store.SampleExpiredKeys(w.config.SampleSize)
	if err != nil {
		fmt.Printf("[EXPIRY] sample error: %v\n", err)
		return
	}

	if total == 0 {
		return // No keys with TTL
	}

	ratio := float64(expired) / float64(total)

	// Step 2: Check if we should run aggressive cleanup
	if ratio < w.config.ExpiryThreshold {
		fmt.Printf("[EXPIRY] sampled=%d expired=%d (%.1f%%) → skipping cleanup\n",
			total, expired, ratio*100)
		return
	}

	fmt.Printf("[EXPIRY] sampled=%d expired=%d (%.1f%%) → running hard delete\n",
		total, expired, ratio*100)

	// Step 3: Run batched hard delete
	deleted, err := w.store.HardDeleteBatch(w.config.DeleteBatchSize)
	if err != nil {
		fmt.Printf("[EXPIRY] hard delete error: %v\n", err)
		return
	}

	fmt.Printf("[EXPIRY] hard deleted %d keys\n", deleted)
}

// ForceCleanup manually triggers a hard delete cycle (for testing)
func (w *Worker) ForceCleanup() (int64, error) {
	return w.store.HardDeleteBatch(w.config.DeleteBatchSize)
}

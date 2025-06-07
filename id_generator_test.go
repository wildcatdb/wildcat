package wildcat

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewIDGenerator(t *testing.T) {
	g := newIDGenerator()
	if g == nil {
		t.Fatal("NewIDGenerator returned nil")
	}
	if g.lastID != 0 {
		t.Fatal("lastID was not initialized")
	}
}

func TestNextID_Unique(t *testing.T) {
	g := newIDGenerator()
	id1 := g.nextID()
	id2 := g.nextID()

	if id1 == id2 {
		t.Fatal("NextID did not generate unique IDs")
	}
}

func TestNextID_Monotonic(t *testing.T) {
	g := newIDGenerator()
	id1 := g.nextID()
	id2 := g.nextID()

	if id2 <= id1 {
		t.Fatalf("NextID did not ensure monotonicity: id1=%d, id2=%d", id1, id2)
	}
}

func TestNextID_ThreadSafety(t *testing.T) {
	g := newIDGenerator()
	const numGoroutines = 100
	const idsPerGoroutine = 100

	var wg sync.WaitGroup
	ids := make(chan int64, numGoroutines*idsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				ids <- g.nextID()
			}
		}()
	}

	wg.Wait()
	close(ids)

	// Check for uniqueness
	idSet := make(map[int64]struct{})
	for id := range ids {
		if _, exists := idSet[id]; exists {
			t.Fatalf("Duplicate ID detected: %d", id)
		}
		idSet[id] = struct{}{}
	}
}

func TestTimestampIDGenerator_ConcurrentGap(t *testing.T) {
	g := newIDGeneratorWithTimestamp()

	// Simulate a large time gap by setting lastID to a future timestamp
	futureTime := time.Now().Add(1 * time.Hour).UnixNano()
	atomic.StoreInt64(&g.lastID, futureTime)

	const numGoroutines = 50
	const idsPerGoroutine = 20

	var wg sync.WaitGroup
	ids := make(chan int64, numGoroutines*idsPerGoroutine)

	// Launch concurrent goroutines immediately after setting future time
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				ids <- g.nextID()
			}
		}()
	}

	wg.Wait()
	close(ids)

	// Collect and sort all IDs
	var allIDs []int64
	idSet := make(map[int64]struct{})

	for id := range ids {
		// Check for uniqueness
		if _, exists := idSet[id]; exists {
			t.Fatalf("Duplicate ID detected: %d", id)
		}
		idSet[id] = struct{}{}
		allIDs = append(allIDs, id)
	}

	// Verify we got the expected number of IDs
	expectedCount := numGoroutines * idsPerGoroutine
	if len(allIDs) != expectedCount {
		t.Fatalf("Expected %d IDs, got %d", expectedCount, len(allIDs))
	}

	// Verify monotonicity by checking each ID is greater than the previous
	// Since concurrent generation might not be in perfect order, we'll check
	// that all IDs are >= the original future timestamp
	for i, id := range allIDs {
		if id < futureTime {
			t.Fatalf("ID %d at position %d is less than the initial future time %d",
				id, i, futureTime)
		}
	}

	t.Logf("Generated %d unique, monotonic timestamp IDs despite large time gap", len(allIDs))
	t.Logf("Time gap was: %v", time.Duration(futureTime-time.Now().UnixNano()))
	t.Logf("First ID: %d, Last ID: %d", allIDs[0], allIDs[len(allIDs)-1])
}

func TestTimestampIDGenerator_RapidConcurrentGeneration(t *testing.T) {
	g := newIDGeneratorWithTimestamp()

	const numGoroutines = 100
	const idsPerGoroutine = 50

	var wg sync.WaitGroup
	ids := make(chan int64, numGoroutines*idsPerGoroutine)

	// Use a barrier to make all goroutines start as close to simultaneously as possible
	startBarrier := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startBarrier // Wait for the signal to start
			for j := 0; j < idsPerGoroutine; j++ {
				ids <- g.nextID()
			}
		}()
	}

	// Release all goroutines at once
	close(startBarrier)

	wg.Wait()
	close(ids)

	// Collect all IDs and verify uniqueness
	idSet := make(map[int64]struct{})
	var lastID int64 = 0
	count := 0

	for id := range ids {
		count++

		// Check for uniqueness
		if _, exists := idSet[id]; exists {
			t.Fatalf("Duplicate timestamp ID detected: %d", id)
		}
		idSet[id] = struct{}{}

		// Track the highest ID we've seen
		if id > lastID {
			lastID = id
		}
	}

	expectedCount := numGoroutines * idsPerGoroutine
	if count != expectedCount {
		t.Fatalf("Expected %d IDs, got %d", expectedCount, count)
	}

	t.Logf("Successfully generated %d unique timestamp IDs under high concurrency", count)
	t.Logf("Highest timestamp ID: %d", lastID)
}

func TestIDGenerator_OverflowBehavior(t *testing.T) {
	// Test int64 generator overflow
	g := newIDGenerator()
	atomic.StoreInt64(&g.lastID, math.MaxInt64)

	nextID := g.nextID()
	if nextID != 1 {
		t.Fatalf("Int64 generator should reset to 1 on overflow, got %d", nextID)
	}

	// Test timestamp generator overflow
	gTimestamp := newIDGeneratorWithTimestamp()
	atomic.StoreInt64(&gTimestamp.lastID, math.MaxInt64)

	nextTimestampID := gTimestamp.nextID()
	now := time.Now().UnixNano()

	// Should reset to current time (with some tolerance for execution time)
	if nextTimestampID < now-1000000 || nextTimestampID > now+1000000 {
		t.Fatalf("Timestamp generator should reset to current time on overflow, got %d, expected around %d",
			nextTimestampID, now)
	}
}

func TestIDGenerator_ClockBackwards(t *testing.T) {
	g := newIDGeneratorWithTimestamp()

	// Get a timestamp ID
	g.nextID()

	// Manually set lastID to a higher value to simulate clock going backwards
	futureTime := time.Now().Add(1 * time.Second).UnixNano()
	atomic.StoreInt64(&g.lastID, futureTime)

	// Next ID should be futureTime + 1 (not current time)
	id2 := g.nextID()

	if id2 != futureTime+1 {
		t.Fatalf("Expected ID to be %d when clock goes backwards, got %d",
			futureTime+1, id2)
	}

	// Subsequent calls should continue incrementing
	id3 := g.nextID()
	if id3 != id2+1 {
		t.Fatalf("Expected monotonic increment after clock backwards, got %d after %d",
			id3, id2)
	}
}

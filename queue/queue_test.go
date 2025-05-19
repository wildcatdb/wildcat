// Package queue
//
// (C) Copyright Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queue

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestQueueBasicOperations tests basic enqueue and dequeue operations
func TestQueueBasicOperations(t *testing.T) {
	q := New()

	// Test empty queue
	if !q.IsEmpty() {
		t.Error("Queue should be empty")
	}

	if val := q.Dequeue(); val != nil {
		t.Errorf("Dequeue on empty queue should return nil, got %v", val)
	}

	// Test single element
	q.Enqueue(42)
	if q.IsEmpty() {
		t.Error("Queue shouldn't be empty after enqueue")
	}

	val := q.Dequeue()
	if val != 42 {
		t.Errorf("Expected 42, got %v", val)
	}

	if !q.IsEmpty() {
		t.Error("Queue should be empty after dequeue")
	}

	// Test multiple elements
	values := []interface{}{1, "string", 3.14, struct{}{}, nil}
	for _, v := range values {
		q.Enqueue(v)
	}

	for i, expected := range values {
		val := q.Dequeue()
		if val != expected {
			t.Errorf("Element %d: expected %v, got %v", i, expected, val)
		}
	}

	if !q.IsEmpty() {
		t.Error("Queue should be empty after dequeueing all elements")
	}
}

// TestQueueEdgeCases tests edge cases like nil values and empty-full-empty cycles
func TestQueueEdgeCases(t *testing.T) {
	q := New()

	// Test handling nil values
	q.Enqueue(nil)
	if q.IsEmpty() {
		t.Error("Queue shouldn't be empty after enqueueing nil")
	}

	val := q.Dequeue()
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}

	// Test many empty-full-empty cycles
	for i := 0; i < 100; i++ {
		if !q.IsEmpty() {
			t.Errorf("Cycle %d: queue should be empty at start", i)
		}

		q.Enqueue(i)

		if q.IsEmpty() {
			t.Errorf("Cycle %d: queue shouldn't be empty after enqueue", i)
		}

		val := q.Dequeue()
		if val != i {
			t.Errorf("Cycle %d: expected %d, got %v", i, i, val)
		}

		if !q.IsEmpty() {
			t.Errorf("Cycle %d: queue should be empty after dequeue", i)
		}
	}
}

// TestQueueOrder ensures FIFO behavior with multiple elements
func TestQueueOrder(t *testing.T) {
	q := New()
	count := 1000

	// Enqueue many elements
	for i := 0; i < count; i++ {
		q.Enqueue(i)
	}

	// Verify they come out in the same order
	for i := 0; i < count; i++ {
		val := q.Dequeue()
		if val != i {
			t.Errorf("Expected %d, got %v", i, val)
		}
	}
}

// TestQueueConcurrentEnqueue tests concurrent enqueue operations
func TestQueueConcurrentEnqueue(t *testing.T) {
	q := New()
	count := 10000
	var wg sync.WaitGroup

	// Concurrently enqueue items
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			q.Enqueue(val)
		}(i)
	}

	wg.Wait()

	// Verify we have exactly count items
	seen := make(map[interface{}]bool)
	duplicates := 0
	missing := 0

	for i := 0; i < count; i++ {
		val := q.Dequeue()
		if val == nil {
			missing++
			continue
		}

		if seen[val] {
			duplicates++
		}
		seen[val] = true
	}

	if val := q.Dequeue(); val != nil {
		t.Errorf("Queue should be empty, but got %v", val)
	}

	if duplicates > 0 {
		t.Errorf("Found %d duplicate items", duplicates)
	}

	if missing > 0 {
		t.Errorf("Missing %d items", missing)
	}

	// Check that all values 0 to count-1 are present
	for i := 0; i < count; i++ {
		if !seen[i] {
			t.Errorf("Value %d missing from queue", i)
		}
	}
}

// TestQueueConcurrentDequeue tests concurrent dequeue operations
func TestQueueConcurrentDequeue(t *testing.T) {
	q := New()
	count := 10000

	// Enqueue items
	for i := 0; i < count; i++ {
		q.Enqueue(i)
	}

	var wg sync.WaitGroup
	results := make(chan interface{}, count)

	// Concurrently dequeue items
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- q.Dequeue()
		}()
	}

	wg.Wait()
	close(results)

	// Verify we got exactly count unique items
	seen := make(map[interface{}]bool)
	total := 0

	for val := range results {
		if val == nil {
			t.Error("Got unexpected nil value")
			continue
		}

		if seen[val] {
			t.Errorf("Got duplicate value: %v", val)
		}
		seen[val] = true
		total++
	}

	if total != count {
		t.Errorf("Expected %d values, got %d", count, total)
	}

	if !q.IsEmpty() {
		t.Error("Queue should be empty after test")
	}
}

// TestQueueConcurrentMixed tests concurrent enqueue and dequeue operations
func TestQueueConcurrentMixed(t *testing.T) {
	q := New()
	count := 10000
	var wg sync.WaitGroup

	// Start enqueuers
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			q.Enqueue(val)
		}(i)
	}

	// Start dequeuers
	results := make(chan interface{}, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Try to dequeue until successful, with short backoff
			for {
				val := q.Dequeue()
				if val != nil {
					results <- val
					return
				}
				runtime.Gosched() // Yield to other goroutines
			}
		}()
	}

	wg.Wait()
	close(results)

	// Verify results
	seen := make(map[interface{}]bool)
	total := 0

	for val := range results {
		if seen[val] {
			t.Errorf("Got duplicate value: %v", val)
		}
		seen[val] = true
		total++
	}

	if total != count {
		t.Errorf("Expected %d values, got %d", count, total)
	}

	if !q.IsEmpty() {
		t.Error("Queue should be empty after test")
	}
}

// TestQueueStress performs a stress test with many concurrent operations
func TestQueueStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	q := New()
	count := 100000
	procs := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup

	// Start mixed operations across multiple goroutines
	for p := 0; p < procs*2; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			localCount := count / (procs * 2)

			// Each goroutine does a mix of enqueues and dequeues
			for i := 0; i < localCount; i++ {
				if r.Intn(2) == 0 {
					q.Enqueue(r.Intn(1000000))
				} else {
					q.Dequeue()
				}
			}
		}(p)
	}

	wg.Wait()

	// Final queue state is unpredictable, but operations should complete without errors
	t.Logf("Final queue state: empty=%v", q.IsEmpty())
}

// TestQueueDequeueEmptyStress stress tests dequeuing from an empty queue
func TestQueueDequeueEmptyStress(t *testing.T) {
	q := New()
	var wg sync.WaitGroup

	// Multiple goroutines try to dequeue from an empty queue
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				val := q.Dequeue()
				if val != nil {
					t.Errorf("Expected nil from empty queue, got %v", val)
				}
			}
		}()
	}

	wg.Wait()

	// Queue should still be empty
	if !q.IsEmpty() {
		t.Error("Queue should be empty")
	}
}

// BenchmarkEnqueueDequeue measures the performance of queue operations
func BenchmarkEnqueueDequeue(b *testing.B) {
	q := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
		q.Dequeue()
	}
}

// BenchmarkEnqueueDequeueParallel measures parallel performance of queue operations
func BenchmarkEnqueueDequeueParallel(b *testing.B) {
	q := New()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Enqueue(i)
			q.Dequeue()
			i++
		}
	})
}

// BenchmarkEnqueueOnly measures enqueue performance
func BenchmarkEnqueueOnly(b *testing.B) {
	q := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
	}
}

// BenchmarkDequeueOnly measures dequeue performance with prefilled queue
func BenchmarkDequeueOnly(b *testing.B) {
	q := New()

	// Pre-fill the queue
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Dequeue()
	}
}

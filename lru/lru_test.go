// Package lru
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
package lru

import (
	"sync"
	"testing"
	"time"
)

func TestLRUBasicOperations(t *testing.T) {
	lru := New(10, 0.25, 0.7)

	if lru.Length() != 0 {
		t.Errorf("Expected initial length 0, got %d", lru.Length())
	}

	// Test Put and Get operations
	lru.Put("key1", "value1")
	if lru.Length() != 1 {
		t.Errorf("Expected length 1 after Put, got %d", lru.Length())
	}

	val, found := lru.Get("key1")
	if !found {
		t.Error("Expected to find key1, but not found")
	}

	if val != "value1" {
		t.Errorf("Expected value 'value1', got %v", val)
	}

	// Test updating existing key
	lru.Put("key1", "value1-updated")
	val, found = lru.Get("key1")
	if !found || val != "value1-updated" {
		t.Errorf("Expected updated value 'value1-updated', got %v", val)
	}

	// Test non-existent key
	_, found = lru.Get("nonexistent")
	if found {
		t.Error("Expected not to find nonexistent key, but found")
	}

	// Test Delete operation
	result := lru.Delete("key1")
	if !result {
		t.Error("Delete operation failed")
	}

	if lru.Length() != 0 {
		t.Errorf("Expected length 0 after Delete, got %d", lru.Length())
	}

	_, found = lru.Get("key1")
	if found {
		t.Error("Expected not to find deleted key, but found")
	}
}

func TestLRUCapacityAndEviction(t *testing.T) {
	// Create a new LRU with small capacity to test eviction
	capacity := int64(5)
	lru := New(capacity, 0.4, 0.7) // Evict 40% (2 items) when full

	// Fill the LRU
	for i := 0; i < int(capacity); i++ {
		lru.Put(i, i*10)
	}

	// Verify all items are present
	for i := 0; i < int(capacity); i++ {
		val, found := lru.Get(i)
		if !found {
			t.Errorf("Expected to find key %d, but not found", i)
		}
		if val != i*10 {
			t.Errorf("Expected value %d, got %v", i*10, val)
		}
	}

	// Access some items more to influence eviction (items 3, 4)
	for i := 3; i < int(capacity); i++ {
		for j := 0; j < 5; j++ { // Access multiple times
			lru.Get(i)
		}
	}

	// Add additional items to trigger eviction
	for i := int(capacity); i < int(capacity)+3; i++ {
		lru.Put(i, i*10)
		// Give time for lazy eviction to process
		time.Sleep(time.Millisecond)
	}

	// Force eviction processing by triggering more operations
	for i := 0; i < 3; i++ {
		lru.Get(999) // Non-existent key to trigger traversal
	}

	// Check that some items were evicted
	evictedCount := 0
	for i := 0; i < int(capacity); i++ {
		_, found := lru.Get(i)
		if !found {
			evictedCount++
		}
	}

	// Length should not significantly exceed capacity
	if lru.Length() > capacity+1 {
		t.Errorf("Length significantly exceeds capacity: %d > %d", lru.Length(), capacity+1)
	}

	// At least some eviction should have occurred when we exceed capacity
	if lru.Length() == int64(capacity+3) {
		t.Error("Expected some eviction to occur, but length suggests none happened")
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	// Create a new LRU with large capacity for concurrent testing
	lru := New(20, 0.25, 0.7)

	// Number of goroutines and operations per goroutine
	goroutines := 10
	opsPerGoroutine := 2

	var wg sync.WaitGroup

	// Launch writer goroutines
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := id*opsPerGoroutine + i
				lru.Put(key, key*10)
			}
		}(g)
	}

	// Launch reader goroutines
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := id*opsPerGoroutine + i
				_, _ = lru.Get(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify the length is as expected
	expectedItemCount := goroutines * opsPerGoroutine
	if lru.Length() != int64(expectedItemCount) {
		t.Errorf("Expected length %d, got %d", expectedItemCount, lru.Length())
	}
}

func TestLRUForEach(t *testing.T) {
	// Create a new LRU
	lru := New(10, 0.25, 0.7)

	// Add some items
	testData := map[string]int{
		"key1": 100,
		"key2": 200,
		"key3": 300,
	}

	for k, v := range testData {
		lru.Put(k, v)
	}

	// Access some keys to increase access count
	lru.Get("key1")
	lru.Get("key3")
	lru.Get("key3")

	// Use ForEach to collect and verify data
	visited := make(map[string]int)
	accessCounts := make(map[string]uint64)

	lru.ForEach(func(key, value interface{}, accessCount uint64) bool {
		k := key.(string)
		v := value.(int)
		visited[k] = v
		accessCounts[k] = accessCount
		return true
	})

	// Verify all items were visited
	if len(visited) != len(testData) {
		t.Errorf("ForEach didn't visit all items. Expected %d, got %d", len(testData), len(visited))
	}

	// Verify values match
	for k, v := range testData {
		if visited[k] != v {
			t.Errorf("Value mismatch for key %s. Expected %d, got %d", k, v, visited[k])
		}
	}

	// Verify access counts
	if accessCounts["key1"] != 2 { // Put + 1 Get
		t.Errorf("Expected access count 2 for key1, got %d", accessCounts["key1"])
	}
	if accessCounts["key2"] != 1 { // Put only
		t.Errorf("Expected access count 1 for key2, got %d", accessCounts["key2"])
	}
	if accessCounts["key3"] != 3 { // Put + 2 Gets
		t.Errorf("Expected access count 3 for key3, got %d", accessCounts["key3"])
	}

	// Test ForEach early termination
	earlyTermCount := 0
	lru.ForEach(func(key, value interface{}, accessCount uint64) bool {
		earlyTermCount++
		return earlyTermCount < 2 // Stop after visiting 2 items
	})

	if earlyTermCount != 2 {
		t.Errorf("ForEach early termination failed. Expected to visit 2 items, visited %d", earlyTermCount)
	}
}

func TestLRUClear(t *testing.T) {
	// Create a new LRU
	lru := New(10, 0.25, 0.7)

	// Add some items
	for i := 0; i < 5; i++ {
		lru.Put(i, i*10)
	}

	// Verify items are present
	if lru.Length() != 5 {
		t.Errorf("Expected length 5, got %d", lru.Length())
	}

	// Clear the LRU
	lru.Clear()

	// Verify the LRU is empty
	if lru.Length() != 0 {
		t.Errorf("Expected length 0 after Clear, got %d", lru.Length())
	}

	// Verify no items can be found
	for i := 0; i < 5; i++ {
		_, found := lru.Get(i)
		if found {
			t.Errorf("Found key %d after Clear", i)
		}
	}

	// Verify we can add new items after clearing
	lru.Put("new", "value")
	if lru.Length() != 1 {
		t.Errorf("Expected length 1 after adding new item, got %d", lru.Length())
	}

	val, found := lru.Get("new")
	if !found || val != "value" {
		t.Errorf("Expected to find new item with value 'value', got %v", val)
	}
}

func TestLRUEdgeCases(t *testing.T) {
	// Test with zero or negative capacity
	lru := New(0, 0.25, 0.7)
	// Should default to "unlimited" capacity
	for i := 0; i < 100; i++ {
		lru.Put(i, i)
	}
	if lru.Length() != 100 {
		t.Errorf("Expected length 100, got %d", lru.Length())
	}

	// Test with negative evictRatio
	lru = New(10, -0.1, 0.7)
	// Should default to 25% eviction ratio
	// Fill the LRU
	for i := 0; i < 10; i++ {
		lru.Put(i, i)
	}
	// Add one more to trigger eviction
	lru.Put(10, 10)

	// Give time for eviction to process
	time.Sleep(10 * time.Millisecond)
	// Trigger eviction processing
	for i := 0; i < 5; i++ {
		lru.Get(999) // Trigger traversal
	}

	// Should have evicted some items
	if lru.Length() > 10 {
		t.Errorf("Eviction didn't work properly, length: %d", lru.Length())
	}

	// Test with invalid accessWeight
	lru = New(10, 0.25, 1.5)
	// Should default to 0.7 accessWeight
	// Fill the LRU and access some items more
	for i := 0; i < 10; i++ {
		lru.Put(i, i)
		if i >= 5 {
			for j := 0; j < 5; j++ {
				lru.Get(i)
			}
		}
	}
	// Add items to trigger eviction
	for i := 10; i < 13; i++ {
		lru.Put(i, i)
		time.Sleep(time.Millisecond) // Allow processing
	}

	// Force eviction processing
	for i := 0; i < 5; i++ {
		lru.Get(999) // Trigger traversal
	}

	// Test nil and zero values as keys and values
	lru = New(10, 0.25, 0.7)
	lru.Put(nil, "nil-key")
	lru.Put(0, "zero-key")
	lru.Put("nil-value", nil)
	lru.Put("zero-value", 0)

	val, found := lru.Get(nil)
	if !found || val != "nil-key" {
		t.Error("Failed to retrieve nil key")
	}

	val, found = lru.Get(0)
	if !found || val != "zero-key" {
		t.Error("Failed to retrieve zero key")
	}

	val, found = lru.Get("nil-value")
	if !found || val != nil {
		t.Error("Failed to retrieve nil value")
	}

	val, found = lru.Get("zero-value")
	if !found || val != 0 {
		t.Error("Failed to retrieve zero value")
	}
}

func TestLRUEvictionCallback(t *testing.T) {
	// Create a small capacity LRU to easily trigger evictions
	capacity := int64(3) // Smaller capacity for easier testing

	// Keep track of evicted items
	evictedKeys := make([]interface{}, 0)
	evictedValues := make([]interface{}, 0)

	// Callback function to track evictions
	evictionCallback := func(key, value interface{}) {
		evictedKeys = append(evictedKeys, key)
		evictedValues = append(evictedValues, value)
	}

	// Create LRU
	lru := New(capacity, 0.5, 0.7) // Evict 50% when full

	// Fill the LRU to capacity with callback
	for i := 0; i < int(capacity); i++ {
		lru.Put(i, i*10, evictionCallback)
	}

	// Verify all items are present and no evictions yet
	if len(evictedKeys) != 0 {
		t.Errorf("Expected no evictions yet, but got %d", len(evictedKeys))
	}

	// Access some items more frequently to influence eviction
	// Make items 1, 2 more frequently accessed
	for i := 1; i < int(capacity); i++ {
		for j := 0; j < 5; j++ {
			lru.Get(i)
		}
	}

	// Add more items to trigger eviction
	extraItems := 2
	for i := int(capacity); i < int(capacity)+extraItems; i++ {
		lru.Put(i, i*10, evictionCallback)
		// Give time for processing
		time.Sleep(time.Millisecond)
		// Force eviction processing
		lru.Get(999) // Trigger traversal
	}

	// Force more eviction processing
	for i := 0; i < 3; i++ {
		lru.Get(999) // Trigger traversal
		time.Sleep(time.Millisecond)
	}

	// Verify eviction callback was triggered
	if len(evictedKeys) == 0 {
		t.Error("Expected eviction callback to be triggered, but it wasn't")
	}

	// Verify evicted items are not in the cache
	for _, key := range evictedKeys {
		_, found := lru.Get(key)
		if found {
			t.Errorf("Key %v should have been evicted but is still in cache", key)
		}
	}

	// Verify values in the callback match what we put in
	for i, key := range evictedKeys {
		expectedValue := key.(int) * 10
		if evictedValues[i] != expectedValue {
			t.Errorf("Expected evicted value %v for key %v, got %v",
				expectedValue, key, evictedValues[i])
		}
	}
}

func BenchmarkLRUConcurrentOperations(b *testing.B) {
	// Create a new LRU with large capacity for benchmarking
	lru := New(int64(b.N), 0.80, 0.8)

	// Ensure divisor is not zero
	divisor := b.N / 10
	if divisor == 0 {
		divisor = 1
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := i % divisor // Use the safe divisor
			lru.Put(key, i)
			lru.Get(key)
			//if i%10 == 0 { // Occasionally delete
			//lru.Delete(key)
			//}
			i++
		}
	})
}

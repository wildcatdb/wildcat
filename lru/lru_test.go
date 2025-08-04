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
	"fmt"
	"sync"
	"testing"
)

func TestLRUBasicOperations(t *testing.T) {
	lru := New(10)

	if lru.Len() != 0 {
		t.Errorf("Expected initial length 0, got %d", lru.Len())
	}

	lru.Put("key1", "value1", nil)
	if lru.Len() != 1 {
		t.Errorf("Expected length 1 after Put, got %d", lru.Len())
	}

	val, found := lru.Get("key1")
	if !found {
		t.Error("Expected to find key1, but not found")
	}

	if val != "value1" {
		t.Errorf("Expected value 'value1', got %v", val)
	}

	lru.Put("key1", "value1-updated", nil)
	val, found = lru.Get("key1")
	if !found || val != "value1-updated" {
		t.Errorf("Expected updated value 'value1-updated', got %v", val)
	}

	_, found = lru.Get("nonexistent")
	if found {
		t.Error("Expected not to find nonexistent key, but found")
	}

	result := lru.Remove("key1")
	if !result {
		t.Error("Remove operation failed")
	}

	if lru.Len() != 0 {
		t.Errorf("Expected length 0 after Remove, got %d", lru.Len())
	}

	_, found = lru.Get("key1")
	if found {
		t.Error("Expected not to find removed key, but found")
	}
}

func TestLRUCapacityAndEviction(t *testing.T) {
	capacity := 5
	lru := New(capacity)

	for i := 0; i < capacity; i++ {
		key := fmt.Sprintf("key%d", i)
		lru.Put(key, i*10, nil)
	}

	for i := 0; i < capacity; i++ {
		key := fmt.Sprintf("key%d", i)
		val, found := lru.Get(key)
		if !found {
			t.Errorf("Expected to find key %s, but not found", key)
		}
		if val != i*10 {
			t.Errorf("Expected value %d, got %v", i*10, val)
		}
	}

	if lru.Len() != capacity {
		t.Errorf("Expected length %d, got %d", capacity, lru.Len())
	}

	for i := 3; i < capacity; i++ {
		key := fmt.Sprintf("key%d", i)
		for j := 0; j < 3; j++ {
			lru.Get(key)
		}
	}

	lru.Put("new1", "newvalue1", nil)

	_, found := lru.Get("key0")
	if found {
		t.Error("Expected oldest item (key0) to be evicted, but it's still present")
	}

	for i := 3; i < capacity; i++ {
		key := fmt.Sprintf("key%d", i)
		_, found = lru.Get(key)
		if !found {
			t.Errorf("Expected recently accessed key %s to still be present", key)
		}
	}

	val, found := lru.Get("new1")
	if !found || val != "newvalue1" {
		t.Error("Expected new item to be present")
	}
}

func TestLRUEvictionOrder(t *testing.T) {
	lru := New(3)

	lru.Put("first", "1", nil)
	lru.Put("second", "2", nil)
	lru.Put("third", "3", nil)
	lru.Get("first")
	lru.Put("fourth", "4", nil)

	if _, found := lru.Get("second"); found {
		t.Error("Expected 'second' to be evicted")
	}
	if _, found := lru.Get("first"); !found {
		t.Error("Expected 'first' to still be present")
	}
	if _, found := lru.Get("third"); !found {
		t.Error("Expected 'third' to still be present")
	}
	if _, found := lru.Get("fourth"); !found {
		t.Error("Expected 'fourth' to be present")
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	lru := New(1000)

	goroutines := 10
	opsPerGoroutine := 50

	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("write_%d_%d", id, i)
				lru.Put(key, key+"_value", nil)
			}
		}(g)
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("write_%d_%d", id, i)
				lru.Get(key)
			}
		}(g)
	}

	for g := 0; g < goroutines/2; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("mixed_%d_%d", id, i)
				if i%3 == 0 {
					lru.Put(key, fmt.Sprintf("value_%d", i), nil)
				} else if i%3 == 1 {
					lru.Get(key)
				} else {
					lru.Remove(key)
				}
			}
		}(g)
	}

	wg.Wait()

	lru.Put("test", "value", nil)
	val, found := lru.Get("test")
	if !found || val != "value" {
		t.Error("LRU not functional after concurrent operations")
	}
}

func TestLRUPerKeyEvictionCallback(t *testing.T) {
	var evictedKeys []string
	var evictedValues []interface{}
	var mu sync.Mutex

	evictionCallback1 := func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		evictedKeys = append(evictedKeys, key+"_cb1")
		evictedValues = append(evictedValues, value)
	}

	evictionCallback2 := func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		evictedKeys = append(evictedKeys, key+"_cb2")
		evictedValues = append(evictedValues, value)
	}

	lru := New(3)

	lru.Put("key1", "value1", evictionCallback1)
	lru.Put("key2", "value2", evictionCallback2)
	lru.Put("key3", "value3", nil)

	mu.Lock()
	if len(evictedKeys) != 0 {
		t.Errorf("Expected no evictions yet, but got %d", len(evictedKeys))
	}
	mu.Unlock()

	// This should evict key1 (oldest) which has callback1
	lru.Put("key4", "value4", nil)

	mu.Lock()
	if len(evictedKeys) != 1 {
		t.Errorf("Expected 1 eviction, but got %d", len(evictedKeys))
	}
	if len(evictedKeys) > 0 {
		if evictedKeys[0] != "key1_cb1" {
			t.Errorf("Expected to evict 'key1_cb1', but evicted '%s'", evictedKeys[0])
		}
		if evictedValues[0] != "value1" {
			t.Errorf("Expected evicted value 'value1', but got '%v'", evictedValues[0])
		}
	}
	mu.Unlock()

	// This should evict key2 (next oldest) which has callback2
	lru.Put("key5", "value5", nil)

	mu.Lock()
	if len(evictedKeys) != 2 {
		t.Errorf("Expected 2 evictions, but got %d", len(evictedKeys))
	}
	if len(evictedKeys) > 1 {
		if evictedKeys[1] != "key2_cb2" {
			t.Errorf("Expected to evict 'key2_cb2', but evicted '%s'", evictedKeys[1])
		}
		if evictedValues[1] != "value2" {
			t.Errorf("Expected evicted value 'value2', but got '%v'", evictedValues[1])
		}
	}
	mu.Unlock()

	// This should evict key3 (no callback) - no new entries in evictedKeys
	lru.Put("key6", "value6", nil)

	mu.Lock()
	if len(evictedKeys) != 2 {
		t.Errorf("Expected still 2 evictions (key3 had no callback), but got %d", len(evictedKeys))
	}
	mu.Unlock()

	_, found := lru.Get("key1")
	if found {
		t.Error("Evicted item key1 should not be in cache")
	}
	_, found = lru.Get("key2")
	if found {
		t.Error("Evicted item key2 should not be in cache")
	}
	_, found = lru.Get("key3")
	if found {
		t.Error("Evicted item key3 should not be in cache")
	}
}

func TestLRUUpdateExistingWithCallback(t *testing.T) {
	var evictedKeys []string
	var mu sync.Mutex

	callback1 := func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		evictedKeys = append(evictedKeys, key+"_old")
	}

	callback2 := func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		evictedKeys = append(evictedKeys, key+"_new")
	}

	lru := New(2)

	lru.Put("key1", "value1", callback1)
	lru.Put("key2", "value2", nil)

	lru.Put("key1", "value1_updated", callback2)

	lru.Put("key3", "value3", nil)

	// key2 should be evicted (no callback), key1 should remain with new callback

	mu.Lock()
	if len(evictedKeys) != 0 {
		t.Errorf("Expected no evictions yet (key2 has no callback), but got %d", len(evictedKeys))
	}
	mu.Unlock()

	lru.Put("key4", "value4", nil)

	mu.Lock()
	if len(evictedKeys) != 1 {
		t.Errorf("Expected 1 eviction, but got %d", len(evictedKeys))
	}
	if len(evictedKeys) > 0 && evictedKeys[0] != "key1_new" {
		t.Errorf("Expected key1 to be evicted with new callback, but got '%s'", evictedKeys[0])
	}
	mu.Unlock()
}

func TestLRURemoveWithCallback(t *testing.T) {
	var evictedKeys []string
	var mu sync.Mutex

	callback := func(key string, value interface{}) {
		mu.Lock()
		defer mu.Unlock()
		evictedKeys = append(evictedKeys, key)
	}

	lru := New(3)

	lru.Put("key1", "value1", callback)
	lru.Put("key2", "value2", nil)

	result := lru.Remove("key1")
	if !result {
		t.Error("Remove operation failed")
	}

	mu.Lock()
	if len(evictedKeys) != 1 || evictedKeys[0] != "key1" {
		t.Errorf("Expected key1 to be in evictedKeys, got %v", evictedKeys)
	}
	mu.Unlock()

	result = lru.Remove("key2")
	if !result {
		t.Error("Remove operation failed")
	}

	mu.Lock()
	if len(evictedKeys) != 1 {
		t.Errorf("Expected still 1 eviction (key2 had no callback), but got %d", len(evictedKeys))
	}
	mu.Unlock()
}

func TestLRUClear(t *testing.T) {
	var evictedCount int
	var mu sync.Mutex

	evictionCallback := func(key string, value interface{}) {
		mu.Lock()
		evictedCount++
		mu.Unlock()
	}

	lru := New(10)

	// Add some items with callbacks and some without
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("item%d", i)
		if i%2 == 0 {
			lru.Put(key, i*10, evictionCallback)
		} else {
			lru.Put(key, i*10, nil)
		}
	}

	if lru.Len() != 5 {
		t.Errorf("Expected length 5, got %d", lru.Len())
	}

	lru.Clear()

	if lru.Len() != 0 {
		t.Errorf("Expected length 0 after Clear, got %d", lru.Len())
	}

	mu.Lock()

	// Only items with callbacks should trigger eviction callbacks (items 0, 2, 4)
	if evictedCount != 3 {
		t.Errorf("Expected 3 eviction callbacks, got %d", evictedCount)
	}
	mu.Unlock()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("item%d", i)
		_, found := lru.Get(key)
		if found {
			t.Errorf("Found key %s after Clear", key)
		}
	}

	lru.Put("new", "value", nil)
	if lru.Len() != 1 {
		t.Errorf("Expected length 1 after adding new item, got %d", lru.Len())
	}

	val, found := lru.Get("new")
	if !found || val != "value" {
		t.Errorf("Expected to find new item with value 'value', got %v", val)
	}
}

func TestLRUEdgeCases(t *testing.T) {
	lru := New(1)
	lru.Put("first", "1", nil)
	lru.Put("second", "2", nil)

	_, found := lru.Get("first")
	if found {
		t.Error("Expected first item to be evicted")
	}

	val, found := lru.Get("second")
	if !found || val != "2" {
		t.Error("Expected second item to be present")
	}

	lru = New(10)
	lru.Put("nil-value", nil, nil)
	lru.Put("zero-value", 0, nil)
	lru.Put("empty-string", "", nil)

	val, found = lru.Get("nil-value")
	if !found || val != nil {
		t.Error("Failed to retrieve nil value")
	}

	val, found = lru.Get("zero-value")
	if !found || val != 0 {
		t.Error("Failed to retrieve zero value")
	}

	val, found = lru.Get("empty-string")
	if !found || val != "" {
		t.Error("Failed to retrieve empty string value")
	}

	result := lru.Remove("nonexistent")
	if result {
		t.Error("Expected Remove to return false for non-existent key")
	}

	lru = New(3)
	lru.Put("a", "1", nil)
	lru.Put("b", "2", nil)
	lru.Put("c", "3", nil)

	lru.Put("a", "1-updated", nil)
	lru.Put("d", "4", nil)

	_, found = lru.Get("b")
	if found {
		t.Error("Expected 'b' to be evicted after updating 'a'")
	}

	val, found = lru.Get("a")
	if !found || val != "1-updated" {
		t.Error("Expected updated 'a' to still be present")
	}
}

func TestLRUStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	lru := New(100)

	for i := 0; i < 10000; i++ {
		key := i % 500
		lru.Put(fmt.Sprintf("key-%d", key), i, nil)

		if i%3 == 0 {
			lru.Get(fmt.Sprintf("key-%d", key))
		}

		if i%7 == 0 {
			lru.Remove(fmt.Sprintf("key-%d", key))
		}
	}

	lru.Put("final", "test", nil)
	val, found := lru.Get("final")
	if !found || val != "test" {
		t.Error("LRU not functional after stress test")
	}
}

func BenchmarkLRUPut(b *testing.B) {
	lru := New(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_%d", i)
		lru.Put(key, i, nil)
	}
}

func BenchmarkLRUPutWithCallback(b *testing.B) {
	lru := New(b.N)
	callback := func(key string, value interface{}) {
		// Minimal callback for benchmarking
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_%d", i)
		lru.Put(key, i, callback)
	}
}

func BenchmarkLRUGet(b *testing.B) {
	lru := New(b.N)

	minItems := 100
	itemCount := b.N
	if itemCount < minItems {
		itemCount = minItems
	}

	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("bench_%d", i)
		lru.Put(key, i, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		divisor := b.N / 2
		if divisor < 1 {
			divisor = 1
		}
		key := fmt.Sprintf("bench_%d", i%divisor)
		lru.Get(key)
	}
}

func BenchmarkLRUMixed(b *testing.B) {
	lru := New(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("mixed_%d", i%500)
		if i%3 == 0 {
			lru.Put(key, i, nil)
		} else {
			lru.Get(key)
		}
	}
}

func BenchmarkLRUConcurrent(b *testing.B) {
	lru := New(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent_%d", i%100)
			if i%2 == 0 {
				lru.Put(key, i, nil)
			} else {
				lru.Get(key)
			}
			i++
		}
	})
}

// Package skiplist
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
package skiplist

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSkipListBasicPutGet(t *testing.T) {
	sl := New()

	now := time.Now().UnixNano()
	key := []byte("hello")
	val := []byte("world")

	sl.Put(key, val, now)

	got, _, exists := sl.Get(key, now)
	if !exists {
		t.Errorf("key %s should exist", key)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("expected %v, got %v", val, got)
	}
}

func TestSkipListGetOlderVersion(t *testing.T) {
	sl := New()

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 10
	ts3 := ts2 + 10
	key := []byte("versioned")

	sl.Put(key, []byte("v1"), ts1)
	sl.Put(key, []byte("v2"), ts2)

	// Get value at ts1 (should return v1)
	got, _, exists := sl.Get(key, ts1)
	if !exists {
		t.Errorf("key %s should exist at ts1", key)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Errorf("expected v1, got %v", got)
	}

	// Get value at ts2 (should return v2)
	got, _, exists = sl.Get(key, ts2)
	if !exists {
		t.Errorf("key %s should exist at ts2", key)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("expected v2, got %v", got)
	}

	// Get value at ts3 (should return v2)
	got, _, exists = sl.Get(key, ts3)
	if !exists {
		t.Errorf("key %s should exist at ts3", key)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("expected v2, got %v", got)
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := New()

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 100
	key := []byte("tempkey")

	sl.Put(key, []byte("exists"), ts1)

	// Ensure it's there
	got, _, exists := sl.Get(key, ts1)
	if !exists {
		t.Errorf("key %s should exist at ts1", key)
	}
	if !bytes.Equal(got, []byte("exists")) {
		t.Errorf("expected exists at ts1, got %v", got)
	}

	// Delete at ts2
	ok := sl.Delete(key, ts2)
	if !ok {
		t.Fatalf("Delete should return true for existing key")
	}

	// Check that it still exists before delete timestamp
	got, _, exists = sl.Get(key, ts1)
	if !exists {
		t.Errorf("key %s should still exist at ts1 after delete at ts2", key)
	}
	if !bytes.Equal(got, []byte("exists")) {
		t.Errorf("expected exists at ts1, got %v", got)
	}

	// Check that it is deleted after ts2
	_, _, exists = sl.Get(key, ts2)
	if exists {
		t.Errorf("expected key to be deleted at ts2, but it exists")
	}
}

func TestSkipListInsertOverwrite(t *testing.T) {
	sl := New()

	ts := time.Now().UnixNano()
	key := []byte("dupkey")

	sl.Put(key, []byte("first"), ts)
	sl.Put(key, []byte("second"), ts+10)

	got, _, exists := sl.Get(key, ts)
	if !exists {
		t.Errorf("key %s should exist at ts", key)
	}
	if !bytes.Equal(got, []byte("first")) {
		t.Errorf("expected first at ts, got %v", got)
	}

	got, _, exists = sl.Get(key, ts+10)
	if !exists {
		t.Errorf("key %s should exist at ts+10", key)
	}
	if !bytes.Equal(got, []byte("second")) {
		t.Errorf("expected second at ts+10, got %v", got)
	}
}

func TestSkipListGetNonExistent(t *testing.T) {
	sl := New()

	key := []byte("missing")
	ts := time.Now().UnixNano()

	_, _, exists := sl.Get(key, ts)
	if exists {
		t.Errorf("expected non-existent key to return exists=false")
	}
}

func TestIteratorForwardTraversal(t *testing.T) {
	sl := New()

	// Insert multiple keys
	ts := time.Now().UnixNano()
	sl.Put([]byte("key0"), []byte("value0"), ts)
	sl.Put([]byte("key2"), []byte("value2"), ts+10)
	sl.Put([]byte("key3"), []byte("value3"), ts+20)

	// Create an iterator starting at the first key
	it := sl.NewIterator(nil, ts+20)

	// Traverse forward
	expectedKeys := []string{"key0", "key2", "key3"}
	expectedValues := []string{"value0", "value2", "value3"}

	for i := 0; i < len(expectedKeys); i++ {
		key, value, _, exists := it.Next()
		if !exists {
			t.Errorf("Iterator.Next() returned exists=false, expected true at position %d", i)
			continue
		}

		if string(key) != expectedKeys[i] {
			t.Errorf("Expected key %s at position %d, got %s", expectedKeys[i], i, string(key))
		}

		if string(value) != expectedValues[i] {
			t.Errorf("Expected value %s at position %d, got %s", expectedValues[i], i, string(value))
		}
	}

	// Ensure iterator reaches the end
	_, _, _, exists := it.Next()
	if exists {
		t.Errorf("Iterator.Next() should return exists=false at the end")
	}
}

func TestIteratorBackwardTraversal(t *testing.T) {
	sl := New()

	// Insert multiple keys
	ts := time.Now().UnixNano()
	sl.Put([]byte("key1"), []byte("value1"), ts)
	sl.Put([]byte("key2"), []byte("value2"), ts+10)
	sl.Put([]byte("key3"), []byte("value3"), ts+20)

	// Create an iterator starting at the first key
	it := sl.NewIterator([]byte("key1"), ts+20)

	// Traverse forward to the end first
	expectedKeys := []string{"key1", "key2", "key3"}
	expectedValues := []string{"value1", "value2", "value3"}

	for i := 0; i < len(expectedKeys); i++ {
		key, value, _, exists := it.Next()
		if !exists {
			t.Errorf("Iterator.Next() returned exists=false, expected true at position %d", i)
			continue
		}

		if string(key) != expectedKeys[i] {
			t.Errorf("Expected key %s at position %d, got %s", expectedKeys[i], i, string(key))
		}

		if string(value) != expectedValues[i] {
			t.Errorf("Expected value %s at position %d, got %s", expectedValues[i], i, string(value))
		}
	}

	// Traverse backward
	expectedKeysReverse := []string{"key2", "key1"}
	expectedValuesReverse := []string{"value2", "value1"}

	for i := 0; i < len(expectedKeysReverse); i++ {
		key, value, _, exists := it.Prev()
		if !exists {
			t.Errorf("Iterator.Prev() returned exists=false, expected true at position %d", i)
			continue
		}

		if string(key) != expectedKeysReverse[i] {
			t.Errorf("Expected key %s at position %d, got %s", expectedKeysReverse[i], i, string(key))
		}

		if string(value) != expectedValuesReverse[i] {
			t.Errorf("Expected value %s at position %d, got %s", expectedValuesReverse[i], i, string(value))
		}
	}

	// Ensure iterator reaches the beginning
	_, _, _, exists := it.Prev()
	if exists {
		t.Errorf("Iterator.Prev() should return exists=false at the beginning")
	}
}

func TestIteratorSnapshotIsolation(t *testing.T) {
	sl := New()

	// Insert multiple versions of a key
	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 10
	ts3 := ts2 + 10
	key := []byte("key")

	sl.Put(key, []byte("v1"), ts1)
	sl.Put(key, []byte("v2"), ts2)
	sl.Put(key, []byte("v3"), ts3)

	// Create an iterator with a snapshot at ts2
	it := sl.NewIterator(key, ts2)

	// Ensure the iterator sees the correct version
	k, v, _, exists := it.Next()
	if !exists || string(k) != "key" || string(v) != "v2" {
		t.Errorf("expected key='key', value='v2', got key='%s', value='%s', exists=%v",
			string(k), string(v), exists)
	}

	// Ensure the iterator does not see versions beyond ts2
	_, _, _, exists = it.Next()
	if exists {
		t.Errorf("expected no more entries for versions beyond snapshot timestamp")
	}
}

func TestSkipListConcurrentPutGet(t *testing.T) {
	sl := New()
	var wg sync.WaitGroup

	// Number of goroutines
	numGoroutines := 10
	// Number of operations per goroutine
	opsPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", gid, j))
				value := []byte(fmt.Sprintf("value-%d-%d", gid, j))
				ts := time.Now().UnixNano()
				sl.Put(key, value, ts)
			}
		}(i)
	}

	// Wait for all writes to complete
	wg.Wait()

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", gid, j))
				ts := time.Now().UnixNano()
				_, _, _ = sl.Get(key, ts)
			}
		}(i)
	}

	// Wait for all reads to complete
	wg.Wait()
}

func TestSkipListConcurrentPutDelete(t *testing.T) {
	sl := New()
	var wg sync.WaitGroup

	// Number of goroutines
	numGoroutines := 10
	// Number of operations per goroutine
	opsPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", gid, j))
				value := []byte(fmt.Sprintf("value-%d-%d", gid, j))
				ts := time.Now().UnixNano()
				sl.Put(key, value, ts)
			}
		}(i)
	}

	// Concurrent deletes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", gid, j))
				ts := time.Now().UnixNano()
				sl.Delete(key, ts)
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
}

func TestSkipListConcurrentIterators(t *testing.T) {
	sl := New()
	var wg sync.WaitGroup

	// Insert keys
	ts := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, ts+int64(i))
	}

	// Concurrent iterators
	numIterators := 10
	for i := 0; i < numIterators; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			it := sl.NewIterator([]byte("key-0"), ts+1000)
			var k, v []byte
			var exists bool
			for {
				k, v, _, exists = it.Next()
				if !exists {
					break
				}
				// Simulate work
				time.Sleep(time.Microsecond)
				_ = k
				_ = v
			}
		}()
	}

	// Wait for all iterators to complete
	wg.Wait()
}

func TestSkipListGetMinMax(t *testing.T) {
	sl := New()

	// Test with empty skiplist
	ts := time.Now().UnixNano()

	// Test GetMin on empty list
	minKey, minVal, exists := sl.GetMin(ts)
	if exists {
		t.Errorf("GetMin on empty list should return exists=false, got true with key=%v, val=%v", minKey, minVal)
	}

	// Test GetMax on empty list
	maxKey, maxVal, exists := sl.GetMax(ts)
	if exists {
		t.Errorf("GetMax on empty list should return exists=false, got true with key=%v, val=%v", maxKey, maxVal)
	}

	// Insert some data
	sl.Put([]byte("apple"), []byte("red"), ts)
	sl.Put([]byte("banana"), []byte("yellow"), ts+10)
	sl.Put([]byte("cherry"), []byte("red"), ts+20)
	sl.Put([]byte("date"), []byte("brown"), ts+30)
	sl.Put([]byte("elderberry"), []byte("purple"), ts+40)

	// Test GetMin at current time
	minKey, minVal, exists = sl.GetMin(ts + 50)
	if !exists {
		t.Errorf("GetMin on non-empty list should return exists=true")
	}
	if string(minKey) != "apple" || string(minVal) != "red" {
		t.Errorf("GetMin expected key='apple', val='red', got key='%s', val='%s'", minKey, minVal)
	}

	// Test GetMax at current time
	maxKey, maxVal, exists = sl.GetMax(ts + 50)
	if !exists {
		t.Errorf("GetMax on non-empty list should return exists=true")
	}
	if string(maxKey) != "elderberry" || string(maxVal) != "purple" {
		t.Errorf("GetMax expected key='elderberry', val='purple', got key='%s', val='%s'", maxKey, maxVal)
	}

	// Test with deleted min and max
	sl.Delete([]byte("apple"), ts+60)
	sl.Delete([]byte("elderberry"), ts+60)

	// Test GetMin after deletion
	minKey, minVal, exists = sl.GetMin(ts + 70)
	if !exists {
		t.Errorf("GetMin should return exists=true")
	}
	if string(minKey) != "banana" || string(minVal) != "yellow" {
		t.Errorf("GetMin expected key='banana', val='yellow', got key='%s', val='%s'", minKey, minVal)
	}

	// Test GetMax after deletion
	maxKey, maxVal, exists = sl.GetMax(ts + 70)
	if !exists {
		t.Errorf("GetMax should return exists=true")
	}
	if string(maxKey) != "date" || string(maxVal) != "brown" {
		t.Errorf("GetMax expected key='date', val='brown', got key='%s', val='%s'", maxKey, maxVal)
	}

	// Test timestamp sensitivity
	// Get min/max at ts+15 (should see apple and banana, but not cherry, date, elderberry)
	minKey, minVal, exists = sl.GetMin(ts + 15)
	if !exists || string(minKey) != "apple" || string(minVal) != "red" {
		t.Errorf("GetMin at ts+15 expected key='apple', val='red', got exists=%v, key='%s', val='%s'",
			exists, minKey, minVal)
	}

	maxKey, maxVal, exists = sl.GetMax(ts + 15)
	if !exists || string(maxKey) != "banana" || string(maxVal) != "yellow" {
		t.Errorf("GetMax at ts+15 expected key='banana', val='yellow', got exists=%v, key='%s', val='%s'",
			exists, maxKey, maxVal)
	}

	// Add a new version of an existing key
	sl.Put([]byte("banana"), []byte("green"), ts+80) // Banana ripeness changed

	// Test GetMin with version changes
	minKey, minVal, exists = sl.GetMin(ts + 90)
	if !exists || string(minKey) != "banana" || string(minVal) != "green" {
		t.Errorf("GetMin after version change expected key='banana', val='green', got exists=%v, key='%s', val='%s'",
			exists, minKey, minVal)
	}

	// Delete everything
	sl.Delete([]byte("banana"), ts+100)
	sl.Delete([]byte("cherry"), ts+100)
	sl.Delete([]byte("date"), ts+100)

	// Test empty list after all deletions
	minKey, minVal, exists = sl.GetMin(ts + 110)
	if exists {
		t.Errorf("GetMin after all deletions should return exists=false, got true with key=%v, val=%v",
			minKey, minVal)
	}

	maxKey, maxVal, exists = sl.GetMax(ts + 110)
	if exists {
		t.Errorf("GetMax after all deletions should return exists=false, got true with key=%v, val=%v",
			maxKey, maxVal)
	}
}

func BenchmarkSkipListPut(b *testing.B) {
	sl := New()
	now := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, now+int64(i))
	}
}

func BenchmarkSkipListGet(b *testing.B) {
	sl := New()
	now := time.Now().UnixNano()

	// Prepopulate the skip list
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, now+int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%1000)) // Cycle through existing keys
		_, _, _ = sl.Get(key, now+int64(i))
	}
}

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
	it, _ := sl.NewIterator(nil, ts+20)

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
	it, _ := sl.NewIterator([]byte("key1"), ts+20)

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
	it, _ := sl.NewIterator(key, ts2)

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
			it, _ := sl.NewIterator([]byte("key-0"), ts+1000)
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

// Tests for PrefixIterator and RangeIterator

func TestPrefixIteratorForwardTraversal(t *testing.T) {
	sl := New()

	// Insert keys with different prefixes
	ts := time.Now().UnixNano()
	sl.Put([]byte("user:1"), []byte("alice"), ts)
	sl.Put([]byte("user:2"), []byte("bob"), ts+10)
	sl.Put([]byte("user:3"), []byte("charlie"), ts+20)
	sl.Put([]byte("admin:1"), []byte("root"), ts+30)
	sl.Put([]byte("guest:1"), []byte("visitor"), ts+40)
	sl.Put([]byte("user:4"), []byte("david"), ts+50)

	// Create prefix iterator for "user:" prefix
	it, _ := sl.NewPrefixIterator([]byte("user:"), ts+60)

	// Expected keys and values for "user:" prefix
	expectedKeys := []string{"user:1", "user:2", "user:3", "user:4"}
	expectedValues := []string{"alice", "bob", "charlie", "david"}

	for i := 0; i < len(expectedKeys); i++ {
		key, value, _, exists := it.Next()
		if !exists {
			t.Errorf("PrefixIterator.Next() returned exists=false, expected true at position %d", i)
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
		t.Errorf("PrefixIterator.Next() should return exists=false at the end")
	}
}

func TestPrefixIteratorBackwardTraversal(t *testing.T) {
	sl := New()

	// Insert keys with different prefixes
	ts := time.Now().UnixNano()
	sl.Put([]byte("user:1"), []byte("alice"), ts)
	sl.Put([]byte("user:2"), []byte("bob"), ts+10)
	sl.Put([]byte("user:3"), []byte("charlie"), ts+20)
	sl.Put([]byte("admin:1"), []byte("root"), ts+30)
	sl.Put([]byte("user:4"), []byte("david"), ts+40)

	// Create prefix iterator for "user:" prefix
	it, _ := sl.NewPrefixIterator([]byte("user:"), ts+50)

	// Move to the end first
	for {
		_, _, _, exists := it.Next()
		if !exists {
			break
		}
	}

	// Expected keys and values for "user:" prefix in reverse order
	expectedKeysReverse := []string{"user:4", "user:3", "user:2", "user:1"}
	expectedValuesReverse := []string{"david", "charlie", "bob", "alice"}

	for i := 0; i < len(expectedKeysReverse); i++ {
		key, value, _, exists := it.Prev()
		if !exists {
			t.Errorf("PrefixIterator.Prev() returned exists=false, expected true at position %d", i)
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
		t.Errorf("PrefixIterator.Prev() should return exists=false at the beginning")
	}
}

func TestPrefixIteratorSnapshotIsolation(t *testing.T) {
	sl := New()

	// Insert multiple versions of keys with the same prefix
	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 100
	ts3 := ts2 + 100

	sl.Put([]byte("user:1"), []byte("alice_v1"), ts1)
	sl.Put([]byte("user:2"), []byte("bob_v1"), ts1)
	sl.Put([]byte("user:1"), []byte("alice_v2"), ts2)
	sl.Put([]byte("user:3"), []byte("charlie_v1"), ts3)

	// Create iterator with snapshot at ts2
	it, _ := sl.NewPrefixIterator([]byte("user:"), ts2)

	// Should see user:1 (v2) and user:2 (v1), but not user:3
	expectedData := map[string]string{
		"user:1": "alice_v2",
		"user:2": "bob_v1",
	}

	count := 0
	for {
		key, value, _, exists := it.Next()
		if !exists {
			break
		}
		count++

		keyStr := string(key)
		expectedValue, ok := expectedData[keyStr]
		if !ok {
			t.Errorf("Unexpected key %s in iterator", keyStr)
			continue
		}

		if string(value) != expectedValue {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, keyStr, string(value))
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 keys, got %d", count)
	}
}

func TestPrefixIteratorWithDeletes(t *testing.T) {
	sl := New()

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 100
	ts3 := ts2 + 100

	// Insert keys
	sl.Put([]byte("user:1"), []byte("alice"), ts1)
	sl.Put([]byte("user:2"), []byte("bob"), ts1)
	sl.Put([]byte("user:3"), []byte("charlie"), ts1)

	// Delete user:2 at ts2
	sl.Delete([]byte("user:2"), ts2)

	// Iterator at ts1 should see all three
	it1, _ := sl.NewPrefixIterator([]byte("user:"), ts1)
	count1 := 0
	for {
		_, _, _, exists := it1.Next()
		if !exists {
			break
		}
		count1++
	}
	if count1 != 3 {
		t.Errorf("Iterator at ts1 expected 3 keys, got %d", count1)
	}

	// Iterator at ts3 should see only user:1 and user:3
	it2, _ := sl.NewPrefixIterator([]byte("user:"), ts3)
	expectedKeys := []string{"user:1", "user:3"}
	i := 0
	for {
		key, _, _, exists := it2.Next()
		if !exists {
			break
		}
		if i >= len(expectedKeys) || string(key) != expectedKeys[i] {
			t.Errorf("Unexpected key at position %d: %s", i, string(key))
		}
		i++
	}
	if i != 2 {
		t.Errorf("Iterator at ts3 expected 2 keys, got %d", i)
	}
}

func TestPrefixIteratorEmptyResult(t *testing.T) {
	sl := New()

	ts := time.Now().UnixNano()
	sl.Put([]byte("admin:1"), []byte("root"), ts)
	sl.Put([]byte("guest:1"), []byte("visitor"), ts)

	// Search for non-existent prefix
	it, _ := sl.NewPrefixIterator([]byte("user:"), ts)

	_, _, _, exists := it.Next()
	if exists {
		t.Errorf("Expected no results for non-existent prefix")
	}

	_, _, _, exists = it.Prev()
	if exists {
		t.Errorf("Expected no results for Prev() on empty prefix iterator")
	}
}

func TestRangeIteratorForwardTraversal(t *testing.T) {
	sl := New()

	// Insert keys across different ranges
	ts := time.Now().UnixNano()
	sl.Put([]byte("apple"), []byte("red"), ts)
	sl.Put([]byte("banana"), []byte("yellow"), ts+10)
	sl.Put([]byte("cherry"), []byte("red"), ts+20)
	sl.Put([]byte("date"), []byte("brown"), ts+30)
	sl.Put([]byte("elderberry"), []byte("purple"), ts+40)
	sl.Put([]byte("fig"), []byte("purple"), ts+50)

	// Create range iterator for [banana, elderberry)
	it, _ := sl.NewRangeIterator([]byte("banana"), []byte("elderberry"), ts+60)

	// Expected keys in range [banana, elderberry)
	expectedKeys := []string{"banana", "cherry", "date"}
	expectedValues := []string{"yellow", "red", "brown"}

	for i := 0; i < len(expectedKeys); i++ {
		key, value, _, exists := it.Next()
		if !exists {
			t.Errorf("RangeIterator.Next() returned exists=false, expected true at position %d", i)
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
		t.Errorf("RangeIterator.Next() should return exists=false at the end")
	}
}

func TestRangeIteratorBackwardTraversal(t *testing.T) {
	sl := New()

	// Insert keys
	ts := time.Now().UnixNano()
	sl.Put([]byte("apple"), []byte("red"), ts)
	sl.Put([]byte("banana"), []byte("yellow"), ts+10)
	sl.Put([]byte("cherry"), []byte("red"), ts+20)
	sl.Put([]byte("date"), []byte("brown"), ts+30)
	sl.Put([]byte("elderberry"), []byte("purple"), ts+40)

	// Create range iterator for [banana, elderberry)
	it, _ := sl.NewRangeIterator([]byte("banana"), []byte("elderberry"), ts+50)

	// Move to the end first
	for {
		_, _, _, exists := it.Next()
		if !exists {
			break
		}
	}

	// Expected keys in range [banana, elderberry) in reverse order
	expectedKeysReverse := []string{"date", "cherry", "banana"}
	expectedValuesReverse := []string{"brown", "red", "yellow"}

	for i := 0; i < len(expectedKeysReverse); i++ {
		key, value, _, exists := it.Prev()
		if !exists {
			t.Errorf("RangeIterator.Prev() returned exists=false, expected true at position %d", i)
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
		t.Errorf("RangeIterator.Prev() should return exists=false at the beginning")
	}
}

func TestRangeIteratorOpenRange(t *testing.T) {
	sl := New()

	ts := time.Now().UnixNano()
	sl.Put([]byte("apple"), []byte("red"), ts)
	sl.Put([]byte("banana"), []byte("yellow"), ts+10)
	sl.Put([]byte("cherry"), []byte("red"), ts+20)
	sl.Put([]byte("date"), []byte("brown"), ts+30)

	// Test range with nil start (from beginning)
	it1, _ := sl.NewRangeIterator(nil, []byte("cherry"), ts+40)
	expectedKeys1 := []string{"apple", "banana"}

	for i := 0; i < len(expectedKeys1); i++ {
		key, _, _, exists := it1.Next()
		if !exists || string(key) != expectedKeys1[i] {
			t.Errorf("Expected key %s at position %d, got %s (exists=%v)", expectedKeys1[i], i, string(key), exists)
		}
	}

	// Test range with nil end (to the end)
	it2, _ := sl.NewRangeIterator([]byte("banana"), nil, ts+40)
	expectedKeys2 := []string{"banana", "cherry", "date"}

	for i := 0; i < len(expectedKeys2); i++ {
		key, _, _, exists := it2.Next()
		if !exists || string(key) != expectedKeys2[i] {
			t.Errorf("Expected key %s at position %d, got %s (exists=%v)", expectedKeys2[i], i, string(key), exists)
		}
	}

	// Test range with both nil (entire list)
	it3, _ := sl.NewRangeIterator(nil, nil, ts+40)
	expectedKeys3 := []string{"apple", "banana", "cherry", "date"}

	for i := 0; i < len(expectedKeys3); i++ {
		key, _, _, exists := it3.Next()
		if !exists || string(key) != expectedKeys3[i] {
			t.Errorf("Expected key %s at position %d, got %s (exists=%v)", expectedKeys3[i], i, string(key), exists)
		}
	}
}

func TestRangeIteratorSnapshotIsolation(t *testing.T) {
	sl := New()

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 100
	ts3 := ts2 + 100

	// Insert keys
	sl.Put([]byte("banana"), []byte("yellow_v1"), ts1)
	sl.Put([]byte("cherry"), []byte("red_v1"), ts1)
	sl.Put([]byte("banana"), []byte("yellow_v2"), ts2)
	sl.Put([]byte("date"), []byte("brown_v1"), ts3)

	// Iterator at ts2 should see banana_v2, cherry_v1, but not date
	it, _ := sl.NewRangeIterator([]byte("banana"), []byte("elderberry"), ts2)

	expectedData := map[string]string{
		"banana": "yellow_v2",
		"cherry": "red_v1",
	}

	count := 0
	for {
		key, value, _, exists := it.Next()
		if !exists {
			break
		}
		count++

		keyStr := string(key)
		expectedValue, ok := expectedData[keyStr]
		if !ok {
			t.Errorf("Unexpected key %s in iterator", keyStr)
			continue
		}

		if string(value) != expectedValue {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, keyStr, string(value))
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 keys, got %d", count)
	}
}

func TestRangeIteratorWithDeletes(t *testing.T) {
	sl := New()

	ts1 := time.Now().UnixNano()
	ts2 := ts1 + 100
	ts3 := ts2 + 100

	// Insert keys
	sl.Put([]byte("banana"), []byte("yellow"), ts1)
	sl.Put([]byte("cherry"), []byte("red"), ts1)
	sl.Put([]byte("date"), []byte("brown"), ts1)

	// Delete cherry at ts2
	sl.Delete([]byte("cherry"), ts2)

	// Iterator at ts1 should see all three in range
	it1, _ := sl.NewRangeIterator([]byte("banana"), []byte("elderberry"), ts1)
	count1 := 0
	for {
		_, _, _, exists := it1.Next()
		if !exists {
			break
		}
		count1++
	}
	if count1 != 3 {
		t.Errorf("Iterator at ts1 expected 3 keys, got %d", count1)
	}

	// Iterator at ts3 should see only banana and date
	it2, _ := sl.NewRangeIterator([]byte("banana"), []byte("elderberry"), ts3)
	expectedKeys := []string{"banana", "date"}
	i := 0
	for {
		key, _, _, exists := it2.Next()
		if !exists {
			break
		}
		if i >= len(expectedKeys) || string(key) != expectedKeys[i] {
			t.Errorf("Unexpected key at position %d: %s", i, string(key))
		}
		i++
	}
	if i != 2 {
		t.Errorf("Iterator at ts3 expected 2 keys, got %d", i)
	}
}

func TestRangeIteratorEmptyResult(t *testing.T) {
	sl := New()

	ts := time.Now().UnixNano()
	sl.Put([]byte("apple"), []byte("red"), ts)
	sl.Put([]byte("banana"), []byte("yellow"), ts)

	// Search for range with no matching keys
	it, _ := sl.NewRangeIterator([]byte("cherry"), []byte("date"), ts)

	_, _, _, exists := it.Next()
	if exists {
		t.Errorf("Expected no results for empty range")
	}

	_, _, _, exists = it.Prev()
	if exists {
		t.Errorf("Expected no results for Prev() on empty range iterator")
	}
}

func TestIteratorPeekFunctionality(t *testing.T) {
	sl := New()

	ts := time.Now().UnixNano()
	sl.Put([]byte("user:1"), []byte("alice"), ts)
	sl.Put([]byte("user:2"), []byte("bob"), ts)
	sl.Put([]byte("admin:1"), []byte("root"), ts)

	// Test PrefixIterator Peek
	prefixIt, _ := sl.NewPrefixIterator([]byte("user:"), ts)

	// Move to first element
	key1, val1, ts1, exists1 := prefixIt.Next()
	if !exists1 {
		t.Fatal("Expected first element to exist")
	}

	// Peek should return the same element
	key2, val2, ts2, exists2 := prefixIt.Peek()
	if !exists2 {
		t.Fatal("Peek should return the current element")
	}

	if !bytes.Equal(key1, key2) || !bytes.Equal(val1, val2) || ts1 != ts2 {
		t.Errorf("Peek should return same data as current position")
	}

	// Test RangeIterator Peek
	rangeIt, _ := sl.NewRangeIterator([]byte("admin:"), []byte("user:"), ts)

	// Move to first element
	key3, val3, ts3, exists3 := rangeIt.Next()
	if !exists3 {
		t.Fatal("Expected first element to exist")
	}

	// Peek should return the same element
	key4, val4, ts4, exists4 := rangeIt.Peek()
	if !exists4 {
		t.Fatal("Peek should return the current element")
	}

	if !bytes.Equal(key3, key4) || !bytes.Equal(val3, val4) || ts3 != ts4 {
		t.Errorf("Peek should return same data as current position")
	}
}

func TestPrefixIteratorConcurrency(t *testing.T) {
	sl := New()
	var wg sync.WaitGroup

	// Insert keys with various prefixes
	ts := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("user:%d", i))
		value := []byte(fmt.Sprintf("user_value_%d", i))
		sl.Put(key, value, ts+int64(i))

		key = []byte(fmt.Sprintf("admin:%d", i))
		value = []byte(fmt.Sprintf("admin_value_%d", i))
		sl.Put(key, value, ts+int64(i))
	}

	// Concurrent prefix iterators
	numIterators := 10
	for i := 0; i < numIterators; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			prefix := "user:"
			if id%2 == 0 {
				prefix = "admin:"
			}

			it, _ := sl.NewPrefixIterator([]byte(prefix), ts+1000)
			count := 0
			for {
				_, _, _, exists := it.Next()
				if !exists {
					break
				}
				count++
				// Simulate work
				time.Sleep(time.Microsecond)
			}

			if count != 1000 {
				t.Errorf("Expected 1000 keys for prefix %s, got %d", prefix, count)
			}
		}(i)
	}

	wg.Wait()
}

func TestRangeIteratorConcurrency(t *testing.T) {
	sl := New()
	var wg sync.WaitGroup

	// Insert keys
	ts := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, ts+int64(i))
	}

	// Concurrent range iterators with different ranges
	numIterators := 10
	for i := 0; i < numIterators; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Different ranges for different goroutines
			// Ensure ranges don't overlap and are valid
			startNum := id * 90     // Non-overlapping ranges with gaps
			endNum := startNum + 80 // Each range covers 80 keys

			start := []byte(fmt.Sprintf("key-%04d", startNum))
			end := []byte(fmt.Sprintf("key-%04d", endNum))

			it, _ := sl.NewRangeIterator(start, end, ts+1000)
			count := 0
			for {
				_, _, _, exists := it.Next()
				if !exists {
					break
				}
				count++
				// Simulate work
				time.Sleep(time.Microsecond)
			}

			// Expected count should be 80 for most ranges
			// Last range might have fewer elements if it goes beyond our data
			expectedCount := 80
			if endNum > 1000 {
				expectedCount = 1000 - startNum
				if expectedCount < 0 {
					expectedCount = 0
				}
			}

			if count != expectedCount {
				t.Errorf("Expected %d keys for range [%s, %s), got %d", expectedCount, start, end, count)
			}
		}(i)
	}

	wg.Wait()
}

func TestGetLatestTimestamp(t *testing.T) {
	sl := New()

	// Test with empty skip list
	latestTS := sl.GetLatestTimestamp()
	if latestTS != 0 {
		t.Errorf("GetLatestTimestamp on empty list should return 0, got %d", latestTS)
	}

	// Insert some data with different timestamps
	baseTS := time.Now().UnixNano()

	// Insert first key
	sl.Put([]byte("key1"), []byte("value1"), baseTS+100)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+100 {
		t.Errorf("Expected latest timestamp %d, got %d", baseTS+100, latestTS)
	}

	// Insert key with older timestamp
	sl.Put([]byte("key2"), []byte("value2"), baseTS+50)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+100 {
		t.Errorf("Expected latest timestamp %d after inserting older timestamp, got %d", baseTS+100, latestTS)
	}

	// Insert key with newer timestamp
	sl.Put([]byte("key3"), []byte("value3"), baseTS+200)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+200 {
		t.Errorf("Expected latest timestamp %d after inserting newer timestamp, got %d", baseTS+200, latestTS)
	}

	// Update existing key with newer timestamp
	sl.Put([]byte("key1"), []byte("value1_updated"), baseTS+300)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+300 {
		t.Errorf("Expected latest timestamp %d after updating key, got %d", baseTS+300, latestTS)
	}

	// Update existing key with older timestamp (should not change latest)
	sl.Put([]byte("key2"), []byte("value2_updated"), baseTS+150)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+300 {
		t.Errorf("Expected latest timestamp %d after updating with older timestamp, got %d", baseTS+300, latestTS)
	}

	// Test with delete operations (deletes should also count as timestamps)
	sl.Delete([]byte("key3"), baseTS+400)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+400 {
		t.Errorf("Expected latest timestamp %d after delete operation, got %d", baseTS+400, latestTS)
	}

	// Delete with older timestamp should not change latest
	sl.Delete([]byte("key2"), baseTS+250)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+400 {
		t.Errorf("Expected latest timestamp %d after delete with older timestamp, got %d", baseTS+400, latestTS)
	}

	// Delete non-existent key should not affect timestamp
	sl.Delete([]byte("nonexistent"), baseTS+500)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+400 {
		t.Errorf("Expected latest timestamp %d after deleting non-existent key, got %d", baseTS+400, latestTS)
	}

	// Add another write operation with the highest timestamp yet
	sl.Put([]byte("key4"), []byte("value4"), baseTS+600)
	latestTS = sl.GetLatestTimestamp()
	if latestTS != baseTS+600 {
		t.Errorf("Expected latest timestamp %d after final insert, got %d", baseTS+600, latestTS)
	}
}

func BenchmarkPrefixIterator(b *testing.B) {
	sl := New()
	now := time.Now().UnixNano()

	// Prepopulate with various prefixes
	for i := 0; i < 10000; i++ {
		prefixes := []string{"user:", "admin:", "guest:", "system:"}
		prefix := prefixes[i%len(prefixes)]
		key := []byte(fmt.Sprintf("%s%d", prefix, i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, now+int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, _ := sl.NewPrefixIterator([]byte("user:"), now+10000)
		for {
			_, _, _, exists := it.Next()
			if !exists {
				break
			}
		}
	}
}

func BenchmarkRangeIterator(b *testing.B) {
	sl := New()
	now := time.Now().UnixNano()

	// Prepopulate the skip list
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		sl.Put(key, value, now+int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := []byte("key-02000")
		end := []byte("key-08000")
		it, _ := sl.NewRangeIterator(start, end, now+10000)
		for {
			_, _, _, exists := it.Next()
			if !exists {
				break
			}
		}
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

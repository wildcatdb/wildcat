// Package tree
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
package tree

import (
	"bytes"
	"fmt"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"
)

func TestBTreeBasicInsertSearch(t *testing.T) {
	_ = os.Remove("btree_test.db")

	bm, err := blockmanager.Open("btree_test.db", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, blockmanager.SyncNone)
	if err != nil {
		t.Fatalf("failed to create block manager: %v", err)
	}
	defer func(bm *blockmanager.BlockManager) {
		_ = bm.Close()
	}(bm)
	defer func() {
		_ = os.Remove("btree_test.db")
		if err != nil {

		}
	}()

	// Create B-tree
	tree, err := Open(bm, 3, nil)
	if err != nil {
		t.Fatalf("failed to create B-tree: %v", err)
	}
	defer func(tree *BTree) {
		_ = tree.Close()
	}(tree)

	// Insert some key-value pairs
	entries := map[string]string{
		"apple":     "red",
		"banana":    "yellow",
		"cherry":    "red",
		"date":      "brown",
		"fig":       "purple",
		"grape":     "green",
		"kiwi":      "brown",
		"lemon":     "yellow",
		"mango":     "yellow",
		"nectarine": "orange",
		"orange":    "orange",
		"papaya":    "orange",
		"quince":    "yellow",
	}

	for k, v := range entries {
		err := tree.Put([]byte(k), v)
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", k, err)
		}
	}

	// Search and verify
	for k, expected := range entries {
		val, ok, err := tree.Get([]byte(k))
		if err != nil {
			t.Errorf("search failed for key %s: %v", k, err)
			continue
		}
		if !ok {
			t.Errorf("key %s not found", k)
			continue
		}
		if val.(string) != expected {
			t.Errorf("key %s: expected %s, got %s", k, expected, val.(string))
		}
	}
}

func setupTestBTree(t *testing.T, order int) (*BTree, func()) {
	filename := fmt.Sprintf("btree_test_%d.db", order)
	_ = os.Remove(filename)

	bm, err := blockmanager.Open(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, blockmanager.SyncNone)
	if err != nil {
		t.Fatalf("failed to create block manager: %v", err)
	}

	tree, err := Open(bm, order, nil)
	if err != nil {
		_ = bm.Close()
		_ = os.Remove(filename)
		t.Fatalf("failed to create B-tree: %v", err)
	}

	cleanup := func() {
		_ = tree.Close()

		_ = bm.Close()

		_ = os.Remove(filename)
	}

	return tree, cleanup
}

func TestBTreeNodeStructure(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert some keys to create a simple structure
	keys := []string{"e", "b", "h", "a", "d", "g", "i"}

	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Get root node and examine its structure
	rootNode, err := tree.loadNode(tree.metadata.RootBlockID)

	if err != nil {
		t.Fatalf("failed to load root node: %v", err)
	}

	t.Logf("Root node - IsLeaf: %v, Keys: %d, Children: %d",
		rootNode.IsLeaf, len(rootNode.Keys), len(rootNode.Children))

	for i, key := range rootNode.Keys {
		t.Logf("  Key[%d]: %s", i, string(key))
	}

	if !rootNode.IsLeaf {
		for i, childID := range rootNode.Children {
			t.Logf("  Child[%d]: BlockID %d", i, childID)
		}
	}
}

func TestBTreeOrder2(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 2)
	defer cleanup()

	// Insert keys that will force splits
	keys := []string{"m", "f", "g", "d", "k", "a", "h", "e", "s", "i", "r", "x", "c", "l", "n", "t", "u", "p"}

	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Verify all keys
	for _, key := range keys {
		val, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("search failed for key %s: %v", key, err)
		}
		if !ok {
			t.Errorf("key %s not found", key)
		}
		if val.(string) != key+"_value" {
			t.Errorf("key %s: expected %s, got %s", key, key+"_value", val.(string))
		}
	}
}

func TestBTreeUpdateExistingKey(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	key := []byte("test")

	// Insert initial value
	err := tree.Put(key, "value1")
	if err != nil {
		t.Fatalf("initial insert failed: %v", err)
	}

	// Update with new value
	err = tree.Put(key, "value2")
	if err != nil {
		t.Fatalf("update insert failed: %v", err)
	}

	// Verify updated value
	val, ok, err := tree.Get(key)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if !ok {
		t.Fatal("key not found after update")
	}
	if val.(string) != "value2" {
		t.Errorf("expected value2, got %s", val.(string))
	}
}

func TestBTreeLargeDataset(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 5)
	defer cleanup()

	// Insert 1000 sequential keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d", i)
		value := fmt.Sprintf("value_%d", i)

		err := tree.Put([]byte(key), value)
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Verify all keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d", i)
		expectedValue := fmt.Sprintf("value_%d", i)

		val, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("search failed for key %s: %v", key, err)
		}
		if !ok {
			t.Errorf("key %s not found", key)
		}
		if val.(string) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, val.(string))
		}
	}
}

func TestBTreeBulkPutSortedEmpty(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	err := tree.BulkPutSorted(nil)
	if err != nil {
		t.Fatalf("BulkPutSorted(nil) failed: %v", err)
	}
	err = tree.BulkPutSorted([]KeyValue{})
	if err != nil {
		t.Fatalf("BulkPutSorted(empty) failed: %v", err)
	}
	// Tree should still be empty (Get any key -> not found)
	_, ok, _ := tree.Get([]byte("x"))
	if ok {
		t.Error("expected no key after empty bulk load")
	}
}

func TestBTreeBulkPutSortedNonEmptyTreeReturnsError(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	_ = tree.Put([]byte("a"), "a")
	entries := []KeyValue{{Key: []byte("b"), Value: "b"}}
	err := tree.BulkPutSorted(entries)
	if err == nil {
		t.Error("expected BulkPutSorted on non-empty tree to return error")
	}
}

func TestBTreeBulkPutSortedOneEntry(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	entries := []KeyValue{
		{Key: []byte("k"), Value: "v"},
	}
	err := tree.BulkPutSorted(entries)
	if err != nil {
		t.Fatalf("BulkPutSorted failed: %v", err)
	}
	val, ok, err := tree.Get([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("Get failed: ok=%v err=%v", ok, err)
	}
	if val.(string) != "v" {
		t.Errorf("expected value v, got %v", val)
	}
	iter, err := tree.Iterator(true)
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	if !iter.Valid() || string(iter.Key()) != "k" || iter.Value().(string) != "v" {
		t.Errorf("iterator: key=%s value=%v", iter.Key(), iter.Value())
	}
}

func TestBTreeBulkPutSortedOneLeafFull(t *testing.T) {
	// order 3 -> max 5 keys per leaf; fill one leaf
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	var entries []KeyValue
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("key%02d", i)
		entries = append(entries, KeyValue{Key: []byte(k), Value: k + "_val"})
	}
	err := tree.BulkPutSorted(entries)
	if err != nil {
		t.Fatalf("BulkPutSorted failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("key%02d", i)
		val, ok, err := tree.Get([]byte(k))
		if err != nil || !ok {
			t.Errorf("Get(%s) failed: ok=%v err=%v", k, ok, err)
			continue
		}
		if val.(string) != k+"_val" {
			t.Errorf("Get(%s): expected %s_val, got %v", k, k, val)
		}
	}
	n := 0
	iter, _ := tree.Iterator(true)
	for iter.Valid() {
		n++
		iter.Next()
	}
	if n != 5 {
		t.Errorf("expected 5 keys from iterator, got %d", n)
	}
}

func TestBTreeBulkPutSortedMultipleLeaves(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// More than one leaf: order 3 -> 5 keys/leaf; use 12 keys -> 3 leaves (5+5+2)
	var entries []KeyValue
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("k%03d", i)
		entries = append(entries, KeyValue{Key: []byte(k), Value: k})
	}
	err := tree.BulkPutSorted(entries)
	if err != nil {
		t.Fatalf("BulkPutSorted failed: %v", err)
	}
	// Get at boundaries
	for _, idx := range []int{0, 4, 5, 9, 10, 11} {
		k := fmt.Sprintf("k%03d", idx)
		val, ok, err := tree.Get([]byte(k))
		if err != nil || !ok {
			t.Errorf("Get(%s) failed: ok=%v err=%v", k, ok, err)
			continue
		}
		if val.(string) != k {
			t.Errorf("Get(%s): got %v", k, val)
		}
	}
	// Range iterator [k002, k008] inclusive
	iter, err := tree.RangeIterator([]byte("k002"), []byte("k008"), true)
	if err != nil {
		t.Fatalf("RangeIterator failed: %v", err)
	}
	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}
	expected := []string{"k002", "k003", "k004", "k005", "k006", "k007", "k008"}
	if len(got) != len(expected) {
		t.Errorf("range: expected %v, got %v", expected, got)
	} else {
		for i := range expected {
			if got[i] != expected[i] {
				t.Errorf("range at %d: expected %s, got %s", i, expected[i], got[i])
			}
		}
	}
	// Prefix iterator (if keys share prefix)
	prefixIter, err := tree.PrefixIterator([]byte("k01"), true)
	if err != nil {
		t.Fatalf("PrefixIterator failed: %v", err)
	}
	got = nil
	for prefixIter.Valid() {
		got = append(got, string(prefixIter.Key()))
		prefixIter.Next()
	}
	expectedPrefix := []string{"k010", "k011"}
	if len(got) != len(expectedPrefix) {
		t.Errorf("prefix: expected %v, got %v", expectedPrefix, got)
	} else {
		for i := range expectedPrefix {
			if got[i] != expectedPrefix[i] {
				t.Errorf("prefix at %d: expected %s, got %s", i, expectedPrefix[i], got[i])
			}
		}
	}
}

func TestBTreeBulkPutSortedVsSequentialPut(t *testing.T) {
	order := 5
	N := 50
	var sortedEntries []KeyValue
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key_%04d", i)
		sortedEntries = append(sortedEntries, KeyValue{Key: []byte(k), Value: "val_" + k})
	}
	// Already sorted

	// Tree 1: BulkPutSorted
	tree1, cleanup1 := setupTestBTree(t, order)
	defer cleanup1()
	err := tree1.BulkPutSorted(sortedEntries)
	if err != nil {
		t.Fatalf("BulkPutSorted failed: %v", err)
	}

	// Tree 2: sequential Put (same order)
	tree2, cleanup2 := setupTestBTree(t, order)
	defer cleanup2()
	for _, e := range sortedEntries {
		err := tree2.Put(e.Key, e.Value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Same keys and values
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key_%04d", i)
		key := []byte(k)
		v1, ok1, _ := tree1.Get(key)
		v2, ok2, _ := tree2.Get(key)
		if !ok1 || !ok2 {
			t.Errorf("key %s: bulk ok=%v seq ok=%v", k, ok1, ok2)
			continue
		}
		if v1.(string) != v2.(string) {
			t.Errorf("key %s: bulk=%v seq=%v", k, v1, v2)
		}
	}

	// Same iteration order (ascending)
	it1, _ := tree1.Iterator(true)
	it2, _ := tree2.Iterator(true)
	for it1.Valid() && it2.Valid() {
		if !bytes.Equal(it1.Key(), it2.Key()) {
			t.Errorf("iterator key mismatch: %s vs %s", it1.Key(), it2.Key())
		}
		if it1.Value().(string) != it2.Value().(string) {
			t.Errorf("iterator value mismatch at key %s", it1.Key())
		}
		it1.Next()
		it2.Next()
	}
	if it1.Valid() != it2.Valid() {
		t.Error("iterator length mismatch")
	}
}

func TestBTreeRangeIteratorAscending(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Test range iteration from "cherry" to "fig"
	iter, err := tree.RangeIterator([]byte("cherry"), []byte("fig"), true)
	if err != nil {
		t.Fatalf("failed to create range iterator: %v", err)
	}

	expectedKeys := []string{"cherry", "date", "elderberry", "fig"}
	actualKeys := []string{}

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		actualKeys = append(actualKeys, string(key))
		expectedValue := string(key) + "_value"
		if value.(string) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
		}
		iter.Next()
	}

	if len(actualKeys) != len(expectedKeys) {
		t.Errorf("expected %d keys, got %d", len(expectedKeys), len(actualKeys))
	}

	for i, expected := range expectedKeys {
		if i >= len(actualKeys) || actualKeys[i] != expected {
			t.Errorf("at index %d: expected %s, got %s", i, expected, actualKeys[i])
		}
	}
}

func TestBTreeRangeIteratorDescending(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Test descending range iteration from "fig" to "cherry"
	iter, err := tree.RangeIterator([]byte("cherry"), []byte("fig"), false)
	if err != nil {
		t.Fatalf("failed to create range iterator: %v", err)
	}

	expectedKeys := []string{"fig", "elderberry", "date", "cherry"}
	actualKeys := []string{}

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		actualKeys = append(actualKeys, string(key))
		expectedValue := string(key) + "_value"
		if value.(string) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
		}
		iter.Next()
	}

	if len(actualKeys) != len(expectedKeys) {
		t.Errorf("expected %d keys, got %d", len(expectedKeys), len(actualKeys))
	}

	for i, expected := range expectedKeys {
		if i >= len(actualKeys) || actualKeys[i] != expected {
			t.Errorf("at index %d: expected %s, got %s", i, expected, actualKeys[i])
		}
	}
}

func TestBTreePrefixIteratorAscending(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert keys with common prefixes
	keys := []string{"app", "apple", "application", "apply", "banana", "band", "bandana", "cat", "car", "card"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Test prefix iteration for "app"
	iter, err := tree.PrefixIterator([]byte("app"), true)
	if err != nil {
		t.Fatalf("failed to create prefix iterator: %v", err)
	}

	expectedKeys := []string{"app", "apple", "application", "apply"}
	actualKeys := []string{}

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		keyStr := string(key)

		// Only collect keys that actually have the prefix
		if len(keyStr) >= 3 && keyStr[:3] == "app" {
			actualKeys = append(actualKeys, keyStr)
			expectedValue := keyStr + "_value"
			if value.(string) != expectedValue {
				t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
			}
		} else {
			break // Stop when we reach keys without the prefix
		}
		iter.Next()
	}

	if len(actualKeys) != len(expectedKeys) {
		t.Errorf("expected %d keys, got %d: %v", len(expectedKeys), len(actualKeys), actualKeys)
	}

	for i, expected := range expectedKeys {
		if i >= len(actualKeys) || actualKeys[i] != expected {
			t.Errorf("at index %d: expected %s, got %s", i, expected, actualKeys[i])
		}
	}
}

func TestBTreePrefixIteratorDescending(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert keys with common prefixes
	keys := []string{"app", "apple", "application", "apply", "banana", "band", "bandana"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Test descending prefix iteration for "app"
	iter, err := tree.PrefixIterator([]byte("app"), false)
	if err != nil {
		t.Fatalf("failed to create prefix iterator: %v", err)
	}

	expectedKeys := []string{"apply", "application", "apple", "app"}
	actualKeys := []string{}

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		keyStr := string(key)

		// Only collect keys that actually have the prefix
		if len(keyStr) >= 3 && keyStr[:3] == "app" {
			actualKeys = append(actualKeys, keyStr)
			expectedValue := keyStr + "_value"
			if value.(string) != expectedValue {
				t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
			}
		} else {
			break // Stop when we reach keys without the prefix
		}
		iter.Next()
	}

	if len(actualKeys) != len(expectedKeys) {
		t.Errorf("expected %d keys, got %d: %v", len(expectedKeys), len(actualKeys), actualKeys)
	}

	for i, expected := range expectedKeys {
		if i >= len(actualKeys) || actualKeys[i] != expected {
			t.Errorf("at index %d: expected %s, got %s", i, expected, actualKeys[i])
		}
	}
}

func TestBTreeSearchNonExistentKey(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert some keys
	keys := []string{"apple", "banana", "cherry"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Search for non-existent key
	val, ok, err := tree.Get([]byte("nonexistent"))
	if err != nil {
		t.Errorf("search should not error for non-existent key: %v", err)
	}
	if ok {
		t.Error("search should return false for non-existent key")
	}
	if val != nil {
		t.Errorf("search should return nil value for non-existent key, got: %v", val)
	}
}

func TestBTreeInsertDuplicateValues(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert multiple keys with the same value
	keys := []string{"key1", "key2", "key3"}
	commonValue := "shared_value"

	for _, key := range keys {
		err := tree.Put([]byte(key), commonValue)
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Verify all keys have the same value
	for _, key := range keys {
		val, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("search failed for key %s: %v", key, err)
		}
		if !ok {
			t.Errorf("key %s not found", key)
		}
		if val.(string) != commonValue {
			t.Errorf("key %s: expected %s, got %s", key, commonValue, val.(string))
		}
	}
}

func TestBTreeDifferentValueTypes(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert different value types
	testCases := []struct {
		key   string
		value interface{}
	}{
		{"string_key", "string_value"},
		{"int_key", 42},
		{"float_key", 3.14},
		{"bool_key", true},
		{"slice_key", []int{1, 2, 3}},
	}

	for _, tc := range testCases {
		err := tree.Put([]byte(tc.key), tc.value)
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", tc.key, err)
		}
	}

	// Verify all values
	for _, tc := range testCases {
		val, ok, err := tree.Get([]byte(tc.key))
		if err != nil {
			t.Errorf("search failed for key %s: %v", tc.key, err)
		}
		if !ok {
			t.Errorf("key %s not found", tc.key)
		}

		if val == nil {
			t.Errorf("key %s: got nil value", tc.key)
		}
	}
}

func TestBTreeConcurrentReads(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%03d", i)
		err := tree.Put([]byte(key), i)
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Perform concurrent reads
	numGoroutines := 10
	results := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := int32(0); i < int32(numKeys); i++ {
				key := fmt.Sprintf("key_%03d", i)
				val, ok, err := tree.Get([]byte(key))
				if err != nil {
					results <- fmt.Errorf("goroutine %d: search failed for key %s: %v", goroutineID, key, err)
					return
				}
				if !ok {
					results <- fmt.Errorf("goroutine %d: key %s not found", goroutineID, key)
					return
				}
				if val.(int32) != i {
					results <- fmt.Errorf("goroutine %d: key %s: expected %d, got %v", goroutineID, key, i, val)
					return
				}
			}
			results <- nil
		}(g)
	}

	// Wait for all goroutines to complete
	for g := 0; g < numGoroutines; g++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
}

func TestBTreeInvalidOrder(t *testing.T) {
	filename := "btree_invalid_test.db"
	_ = os.Remove(filename)
	defer func(name string) {
		_ = os.Remove(name)
	}(filename)

	bm, err := blockmanager.Open(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, blockmanager.SyncNone)
	if err != nil {
		t.Fatalf("failed to create block manager: %v", err)
	}
	defer func(bm *blockmanager.BlockManager) {
		_ = bm.Close()
	}(bm)

	// Try to create B-tree with invalid order
	_, err = Open(bm, 1, nil)
	if err == nil {
		t.Error("expected error for order < 2")
	}
}

func TestBTreeReverseSortedInsert(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert keys in reverse sorted order
	keys := []string{"z", "y", "x", "w", "v", "u", "t", "s", "r", "q"}

	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Verify all keys are searchable
	for _, key := range keys {
		val, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("search failed for key %s: %v", key, err)
		}
		if !ok {
			t.Errorf("key %s not found", key)
		}
		if val.(string) != key+"_value" {
			t.Errorf("key %s: expected %s, got %s", key, key+"_value", val.(string))
		}
	}

	// Verify sorted iteration works correctly
	iter, err := tree.RangeIterator([]byte("a"), []byte("z"), true)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	actualKeys := []string{}
	for iter.Valid() {
		key := iter.Key()
		actualKeys = append(actualKeys, string(key))
		iter.Next()
	}

	if len(actualKeys) != len(sortedKeys) {
		t.Errorf("expected %d keys, got %d", len(sortedKeys), len(actualKeys))
	}

	for i, expected := range sortedKeys {
		if i >= len(actualKeys) || actualKeys[i] != expected {
			t.Errorf("at index %d: expected %s, got %s", i, expected, actualKeys[i])
		}
	}
}

func TestBTreeGeneralIterator(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	t.Run("AscendingIteration", func(t *testing.T) {
		iter, err := tree.Iterator(true)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		var result []string
		for iter.Valid() {
			key := iter.Key()
			value := iter.Value()
			result = append(result, string(key))
			expectedValue := string(key) + "_value"
			if value.(string) != expectedValue {
				t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
			}
			iter.Next()
		}

		expected := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
		if len(result) != len(expected) {
			t.Errorf("expected %d keys, got %d", len(expected), len(result))
		}
		for i, exp := range expected {
			if i >= len(result) || result[i] != exp {
				t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
			}
		}
	})

	t.Run("DescendingIteration", func(t *testing.T) {
		iter, err := tree.Iterator(false)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		var result []string
		for iter.Valid() {
			key := iter.Key()
			value := iter.Value()
			result = append(result, string(key))
			expectedValue := string(key) + "_value"
			if value.(string) != expectedValue {
				t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
			}
			iter.Next()
		}

		expected := []string{"grape", "fig", "elderberry", "date", "cherry", "banana", "apple"}
		if len(result) != len(expected) {
			t.Errorf("expected %d keys, got %d", len(expected), len(result))
		}
		for i, exp := range expected {
			if i >= len(result) || result[i] != exp {
				t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
			}
		}
	})
}

func TestBTreeIteratorSeek(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	t.Run("SeekExistingKey", func(t *testing.T) {
		iter, err := tree.Iterator(true)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		// Seek to "date"
		err = iter.Seek([]byte("date"))
		if err != nil {
			t.Fatalf("seek failed: %v", err)
		}

		if !iter.Valid() {
			t.Fatal("iterator should be valid after seek")
		}

		key := iter.Key()
		if string(key) != "date" {
			t.Errorf("expected key 'date', got '%s'", string(key))
		}

		// Continue iteration from "date"
		var result []string
		for iter.Valid() {
			key := iter.Key()
			result = append(result, string(key))
			iter.Next()
		}

		expected := []string{"date", "elderberry", "fig", "grape"}
		if len(result) != len(expected) {
			t.Errorf("expected %d keys, got %d", len(expected), len(result))
		}
		for i, exp := range expected {
			if i >= len(result) || result[i] != exp {
				t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
			}
		}
	})

	t.Run("SeekNonExistentKey", func(t *testing.T) {
		iter, err := tree.Iterator(true)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		// Seek to "coconut" (should position at "date")
		err = iter.Seek([]byte("coconut"))
		if err != nil {
			t.Fatalf("seek failed: %v", err)
		}

		if !iter.Valid() {
			t.Fatal("iterator should be valid after seek")
		}

		key := iter.Key()
		if string(key) != "date" {
			t.Errorf("expected key 'date', got '%s'", string(key))
		}
	})

	t.Run("SeekDescending", func(t *testing.T) {
		iter, err := tree.Iterator(false)
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		// Seek to "date" in descending mode
		err = iter.Seek([]byte("date"))
		if err != nil {
			t.Fatalf("seek failed: %v", err)
		}

		if !iter.Valid() {
			t.Fatal("iterator should be valid after seek")
		}

		key := iter.Key()
		if string(key) != "date" {
			t.Errorf("expected key 'date', got '%s'", string(key))
		}

		// Continue iteration from "date" in descending order
		var result []string
		for iter.Valid() {
			key := iter.Key()
			result = append(result, string(key))
			iter.Next()
		}

		expected := []string{"date", "cherry", "banana", "apple"}
		if len(result) != len(expected) {
			t.Errorf("expected %d keys, got %d", len(expected), len(result))
		}
		for i, exp := range expected {
			if i >= len(result) || result[i] != exp {
				t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
			}
		}
	})
}

func TestBTreeIteratorSeekToFirstLast(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	t.Run("SeekToFirst", func(t *testing.T) {
		iter, err := tree.Iterator(false) // Start with descending
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		err = iter.SeekToFirst()
		if err != nil {
			t.Fatalf("SeekToFirst failed: %v", err)
		}

		if !iter.Valid() {
			t.Fatal("iterator should be valid after SeekToFirst")
		}

		key := iter.Key()
		if string(key) != "apple" {
			t.Errorf("expected first key 'apple', got '%s'", string(key))
		}
	})

	t.Run("SeekToLast", func(t *testing.T) {
		iter, err := tree.Iterator(true) // Start with ascending
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		err = iter.SeekToLast()
		if err != nil {
			t.Fatalf("SeekToLast failed: %v", err)
		}

		if !iter.Valid() {
			t.Fatal("iterator should be valid after SeekToLast")
		}

		key := iter.Key()
		if string(key) != "elderberry" {
			t.Errorf("expected last key 'elderberry', got '%s'", string(key))
		}
	})
}

func TestBTreeIteratorDirectionChange(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	iter, err := tree.Iterator(true)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	// Start ascending, get first key
	if !iter.Valid() {
		t.Fatal("iterator should be valid at start")
	}

	key := iter.Key()
	if string(key) != "apple" {
		t.Errorf("expected first key 'apple', got '%s'", string(key))
	}

	iter.Next() // Move to "banana"

	// Change to descending
	iter.SetDirection(false)

	// Should now go backwards from current position
	var result []string
	for iter.Valid() {
		key := iter.Key()
		result = append(result, string(key))
		iter.Next()
	}

	// Note: After getting to "banana" and changing direction, we should get "banana", then "apple"
	expected := []string{"banana", "apple"}
	if len(result) != len(expected) {
		t.Errorf("expected %d keys, got %d: %v", len(expected), len(result), result)
	}
	for i, exp := range expected {
		if i >= len(result) || result[i] != exp {
			t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
		}
	}
}

func TestBTreeIteratorKeyValue(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	iter, err := tree.Iterator(true)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	// Test Key() and Value() methods without advancing
	if !iter.Valid() {
		t.Fatal("iterator should be valid at start")
	}

	key := iter.Key()
	value := iter.Value()

	if string(key) != "apple" {
		t.Errorf("expected key 'apple', got '%s'", string(key))
	}

	if value.(string) != "apple_value" {
		t.Errorf("expected value 'apple_value', got '%s'", value.(string))
	}

	// Key() and Value() should return the same thing when called again
	key2 := iter.Key()
	value2 := iter.Value()

	if string(key2) != "apple" {
		t.Errorf("expected key 'apple' on second call, got '%s'", string(key2))
	}

	if value2.(string) != "apple_value" {
		t.Errorf("expected value 'apple_value' on second call, got '%s'", value2.(string))
	}

	// Advance iterator and check again
	iter.Next()

	if iter.Valid() {
		key = iter.Key()
		value = iter.Value()

		if string(key) != "banana" {
			t.Errorf("expected current key 'banana', got '%s'", string(key))
		}

		if value.(string) != "banana_value" {
			t.Errorf("expected current value 'banana_value', got '%s'", value.(string))
		}
	}
}

func TestBTreeIteratorPrev(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	iter, err := tree.Iterator(true)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	// Seek to "cherry"
	err = iter.Seek([]byte("cherry"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}

	// Go backward using Prev()
	var result []string

	// Add current key
	if iter.Valid() {
		result = append(result, string(iter.Key()))
	}

	// Go backwards
	for iter.Prev() {
		if iter.Valid() {
			result = append(result, string(iter.Key()))
		}
	}

	expected := []string{"cherry", "banana", "apple"}
	if len(result) != len(expected) {
		t.Errorf("expected %d keys, got %d: %v", len(expected), len(result), result)
	}
	for i, exp := range expected {
		if i >= len(result) || result[i] != exp {
			t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
		}
	}
}

func TestBTreeIteratorEmptyTree(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Test iterator on empty tree
	iter, err := tree.Iterator(true)
	if err != nil {
		t.Fatalf("failed to create iterator on empty tree: %v", err)
	}

	if iter.Valid() {
		t.Error("iterator should not be valid on empty tree")
	}

	key := iter.Key()
	if key != nil {
		t.Error("key should be nil on empty tree")
	}

	value := iter.Value()
	if value != nil {
		t.Error("value should be nil on empty tree")
	}

	success := iter.Next()
	if success {
		t.Error("Next() should return false on empty tree")
	}
}

func TestBTreeIteratorBackwardCompatibility(t *testing.T) {
	tree, cleanup := setupTestBTree(t, 3)
	defer cleanup()

	// Insert test data
	keys := []string{"apple", "banana", "cherry"}
	for _, key := range keys {
		err := tree.Put([]byte(key), key+"_value")
		if err != nil {
			t.Fatalf("insert failed for key %s: %v", key, err)
		}
	}

	// Test backward compatibility with NextItem()
	iter, err := tree.Iterator(true)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	var result []string
	for {
		key, value, ok := iter.NextItem()
		if !ok {
			break
		}
		result = append(result, string(key))
		expectedValue := string(key) + "_value"
		if value.(string) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, value.(string))
		}
	}

	expected := []string{"apple", "banana", "cherry"}
	if len(result) != len(expected) {
		t.Errorf("expected %d keys, got %d", len(expected), len(result))
	}
	for i, exp := range expected {
		if i >= len(result) || result[i] != exp {
			t.Errorf("at index %d: expected %s, got %s", i, exp, result[i])
		}
	}
}

func TestPersistence(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("test.db")

	}()

	// Create new tree
	bm1, _ := blockmanager.Open("test.db", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, blockmanager.SyncNone)
	tree1, _ := Open(bm1, 3, nil)
	err := tree1.Put([]byte("key1"), "value1")
	if err != nil {
		t.Fatalf("failed to insert key1: %v", err)
	}
	_ = tree1.Close()

	// Reopen existing tree
	bm2, _ := blockmanager.Open("test.db", os.O_RDWR, 0644, blockmanager.SyncNone)
	tree2, _ := Open(bm2, 3, nil)
	defer func() {
		_ = tree2.Close()
		_ = bm2.Close()
		_ = os.Remove("test.db")
	}()
	value, found, _ := tree2.Get([]byte("key1"))

	if !found {
		t.Error("key1 not found in reopened tree")
	}

	if value.(string) != "value1" {
		t.Errorf("expected value1, got %s", value.(string))
	}
}

func setupBenchBTree(b *testing.B, order int) (*BTree, func()) {
	filename := fmt.Sprintf("bench_btree_%d_%d.db", order, time.Now().UnixNano())

	bm, err := blockmanager.Open(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, blockmanager.SyncNone)
	if err != nil {
		b.Fatalf("failed to create block manager: %v", err)
	}

	tree, err := Open(bm, order, nil)
	if err != nil {
		_ = bm.Close()
		_ = os.Remove(filename)
		b.Fatalf("failed to create B-tree: %v", err)
	}

	cleanup := func() {
		_ = tree.Close()
		_ = bm.Close()
		_ = os.Remove(filename)
	}

	return tree, cleanup
}

func generateKeys(n int, keySize int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := make([]byte, keySize)
		for j := 0; j < keySize; j++ {
			key[j] = byte(rand.Intn(256))
		}
		keys[i] = key
	}
	return keys
}

func generateSequentialKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%010d", i)
		keys[i] = []byte(key)
	}
	return keys
}

func BenchmarkBTreeInsert(b *testing.B) {
	orders := []int{3, 5, 10, 50, 100}
	sizes := []int{1000, 10000, 100000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				keys := generateSequentialKeys(size)
				values := make([]interface{}, size)
				for i := 0; i < size; i++ {
					values[i] = fmt.Sprintf("value_%d", i)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					for j := 0; j < size; j++ {
						err := tree.Put(keys[j], values[j])
						if err != nil {
							b.Fatalf("failed to insert key %s: %v", keys[j], err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkBTreeInsertRandom(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				keys := generateKeys(size, 16) // 16-byte random keys
				values := make([]interface{}, size)
				for i := 0; i < size; i++ {
					values[i] = fmt.Sprintf("value_%d", i)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					for j := 0; j < size; j++ {
						err := tree.Put(keys[j], values[j])
						if err != nil {
							b.Fatalf("failed to insert key %x: %v", keys[j], err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkBTreeSearch(b *testing.B) {
	orders := []int{3, 5, 10, 50}
	sizes := []int{1000, 10000, 100000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				// Pre-populate the tree
				keys := generateSequentialKeys(size)
				for i := 0; i < size; i++ {
					err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
					if err != nil {
						b.Fatalf("failed to insert key %s: %v", keys[i], err)
					}
				}

				// Random keys for searching
				searchKeys := make([][]byte, b.N)
				for i := 0; i < b.N; i++ {
					searchKeys[i] = keys[rand.Intn(size)]
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					get, b2, err := tree.Get(searchKeys[i])
					if err != nil {
						b.Fatalf("failed to get key %s: %v", searchKeys[i], err)
					}
					if b2 {
						if get == nil {
							b.Fatalf("expected value for key %s, got nil", searchKeys[i])
						}
					} else {
						b.Fatalf("key %s not found", searchKeys[i])
					}
				}
			})
		}
	}
}

func BenchmarkBTreeSearchMiss(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				// Pre-populate the tree
				keys := generateSequentialKeys(size)
				for i := 0; i < size; i++ {
					err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
					if err != nil {
						b.Fatalf("failed to insert key %s: %v", keys[i], err)
					}
				}

				// Generate non-existent keys
				missKeys := make([][]byte, b.N)
				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("miss_key_%d", i)
					missKeys[i] = []byte(key)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					_, b2, err := tree.Get(missKeys[i])
					if err != nil {
						b.Fatalf("failed to get key %s: %v", missKeys[i], err)
					}

					if b2 {
						b.Fatalf("expected key %s to be missing, but it was found", missKeys[i])
					}

				}
			})
		}
	}
}

func BenchmarkBTreeRangeIteration(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000}
	rangePercentages := []float64{0.1, 0.5, 1.0} // 10%, 50%, 100% of data

	for _, order := range orders {
		for _, size := range sizes {
			for _, rangePct := range rangePercentages {
				name := fmt.Sprintf("Order%d/Size%d/Range%.0f%%", order, size, rangePct*100)
				b.Run(name, func(b *testing.B) {
					tree, cleanup := setupBenchBTree(b, order)
					defer cleanup()

					// Pre-populate the tree
					keys := generateSequentialKeys(size)
					for i := 0; i < size; i++ {
						err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
						if err != nil {
							b.Fatalf("failed to insert key %s: %v", keys[i], err)
						}
					}

					rangeSize := int(float64(size) * rangePct)
					startIdx := (size - rangeSize) / 2
					endIdx := startIdx + rangeSize

					startKey := keys[startIdx]
					endKey := keys[endIdx-1]

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						iter, _ := tree.RangeIterator(startKey, endKey, true)
						count := 0
						for iter.Valid() {
							iter.Key()
							iter.Value()
							iter.Next()
							count++
						}
					}
				})
			}
		}
	}
}

func BenchmarkBTreePrefixIteration(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				// Pre-populate with keys that have common prefixes
				prefixes := []string{"user:", "product:", "order:", "invoice:", "session:"}
				for i := 0; i < size; i++ {
					prefix := prefixes[i%len(prefixes)]
					key := fmt.Sprintf("%s%010d", prefix, i)
					err := tree.Put([]byte(key), fmt.Sprintf("value_%d", i))
					if err != nil {
						b.Fatalf("failed to insert key %s: %v", key, err)
					}
				}

				testPrefix := []byte("user:")

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					iter, _ := tree.PrefixIterator(testPrefix, true)
					count := 0
					for iter.Valid() {
						iter.Key()
						iter.Value()
						iter.Next()
						count++
					}
				}
			})
		}
	}
}

func BenchmarkBTreeFullIteration(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000, 100000}

	for _, order := range orders {
		for _, size := range sizes {
			for _, ascending := range []bool{true, false} {
				direction := "Asc"
				if !ascending {
					direction = "Desc"
				}
				name := fmt.Sprintf("Order%d/Size%d/%s", order, size, direction)
				b.Run(name, func(b *testing.B) {
					tree, cleanup := setupBenchBTree(b, order)
					defer cleanup()

					// Pre-populate the tree
					keys := generateSequentialKeys(size)
					for i := 0; i < size; i++ {
						err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
						if err != nil {
							b.Fatalf("failed to insert key %s: %v", keys[i], err)
						}
					}

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						iter, _ := tree.Iterator(ascending)
						count := 0
						for iter.Valid() {
							iter.Key()
							iter.Value()
							iter.Next()
							count++
						}
					}
				})
			}
		}
	}
}

func BenchmarkBTreeIteratorSeek(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				// Pre-populate the tree
				keys := generateSequentialKeys(size)
				for i := 0; i < size; i++ {
					err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
					if err != nil {
						b.Fatalf("failed to insert key %s: %v", keys[i], err)
					}
				}

				// Random seek targets
				seekKeys := make([][]byte, b.N)
				for i := 0; i < b.N; i++ {
					seekKeys[i] = keys[rand.Intn(size)]
				}

				iter, _ := tree.Iterator(true)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					err := iter.Seek(seekKeys[i])
					if err != nil {
						b.Fatalf("failed to seek to key %s: %v", seekKeys[i], err)
					}
				}
			})
		}
	}
}

func BenchmarkBTreeMixed(b *testing.B) {
	orders := []int{5, 10, 50}
	sizes := []int{1000, 10000}

	for _, order := range orders {
		for _, size := range sizes {
			name := fmt.Sprintf("Order%d/Size%d", order, size)
			b.Run(name, func(b *testing.B) {
				tree, cleanup := setupBenchBTree(b, order)
				defer cleanup()

				// Pre-populate half the tree
				halfSize := size / 2
				keys := generateSequentialKeys(size)
				for i := 0; i < halfSize; i++ {
					err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
					if err != nil {
						b.Fatalf("failed to insert key %s: %v", keys[i], err)
					}
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// Mix of operations
					opType := i % 3
					switch opType {
					case 0: // Insert
						idx := halfSize + (i % halfSize)
						if idx < size {
							err := tree.Put(keys[idx], fmt.Sprintf("value_%d", idx))
							if err != nil {
								b.Fatalf("failed to insert key %s: %v", keys[idx], err)
							}
						}
					case 1: // Search
						idx := i % halfSize
						get, b2, err := tree.Get(keys[idx])
						if err != nil {
							b.Fatalf("failed to get key %s: %v", keys[idx], err)
						}
						if b2 {
							if get == nil {
								b.Fatalf("expected value for key %s, got nil", keys[idx])
							}
						} else {
							b.Fatalf("key %s not found", keys[idx])
						}
					case 2: // Short iteration
						iter, _ := tree.Iterator(true)
						count := 0
						for iter.Valid() && count < 10 {
							iter.Key()
							iter.Value()
							iter.Next()
							count++
						}
					}
				}
			})
		}
	}
}

func BenchmarkBTreeKeySize(b *testing.B) {
	keySizes := []int{8, 16, 32, 64, 128, 256}
	size := 10000
	order := 10

	for _, keySize := range keySizes {
		name := fmt.Sprintf("KeySize%d", keySize)
		b.Run(name, func(b *testing.B) {
			tree, cleanup := setupBenchBTree(b, order)
			defer cleanup()

			keys := generateKeys(size, keySize)
			values := make([]interface{}, size)
			for i := 0; i < size; i++ {
				values[i] = fmt.Sprintf("value_%d", i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					err := tree.Put(keys[j], values[j])
					if err != nil {
						b.Fatalf("failed to insert key %x: %v", keys[j], err)
					}
				}
			}
		})
	}
}

func BenchmarkBTreeConcurrentReads(b *testing.B) {
	tree, cleanup := setupBenchBTree(b, 10)
	defer cleanup()

	// Pre-populate the tree
	size := 10000
	keys := generateSequentialKeys(size)
	for i := 0; i < size; i++ {
		err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatalf("failed to insert key %s: %v", keys[i], err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(size)]
			get, b2, err := tree.Get(key)
			if err != nil {
				b.Fatalf("failed to get key %s: %v", key, err)
			}

			if b2 {
				if get == nil {
					b.Fatalf("expected value for key %s, got nil", key)
				}
			} else {
				b.Fatalf("key %s not found", key)
			}
		}
	})
}

func BenchmarkBTreeDeepTree(b *testing.B) {
	tree, cleanup := setupBenchBTree(b, 3) // Small order = deep tree
	defer cleanup()

	size := 10000
	keys := generateSequentialKeys(size)

	// Pre-populate
	for i := 0; i < size; i++ {
		err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatalf("failed to insert key %s: %v", keys[i], err)
		}
	}

	searchKeys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		searchKeys[i] = keys[rand.Intn(size)]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		get, b2, err := tree.Get(searchKeys[i])
		if err != nil {
			b.Fatalf("failed to get key %s: %v", searchKeys[i], err)
		}
		if b2 {
			if get == nil {
				b.Fatalf("expected value for key %s, got nil", searchKeys[i])
			}
		} else {
			b.Fatalf("key %s not found", searchKeys[i])
		}
	}
}

func BenchmarkBTreeWideTree(b *testing.B) {
	tree, cleanup := setupBenchBTree(b, 100) // Large order = wide tree
	defer cleanup()

	size := 10000
	keys := generateSequentialKeys(size)

	// Pre-populate
	for i := 0; i < size; i++ {
		err := tree.Put(keys[i], fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatalf("failed to insert key %s: %v", keys[i], err)
		}
	}

	searchKeys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		searchKeys[i] = keys[rand.Intn(size)]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		get, b2, err := tree.Get(searchKeys[i])
		if err != nil {
			b.Fatalf("failed to get key %s: %v", searchKeys[i], err)
		}

		if b2 {
			if get == nil {
				b.Fatalf("expected value for key %s, got nil", searchKeys[i])
			}
		} else {
			b.Fatalf("key %s not found", searchKeys[i])
		}
	}
}

func BenchmarkBTreeActualMemoryFootprint(b *testing.B) {
	tree, cleanup := setupBenchBTree(b, 10)
	defer cleanup()

	// Insert data once
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%010d", i)
		err := tree.Put([]byte(key), fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatalf("failed to insert key %s: %v", key, err)
		}
	}

	// Force GC to clean up temporary allocations
	runtime.GC()
	runtime.GC()

	// Now measure what's actually in memory
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Do one operation to see incremental cost
	_, _, err := tree.Get([]byte("key_0000005000"))
	if err != nil {
		b.Fatalf("failed to get key: %v", err)
	}

	runtime.ReadMemStats(&m2)

	b.ReportMetric(float64(m2.Alloc-m1.Alloc), "bytes/op")
}

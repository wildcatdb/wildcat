// Package orindb
//
// (C) Copyright OrinDB
//
// Original Author: Alex Gaetano Padula
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
package orindb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Helper function to create a temporary database
func createTempDB(t *testing.T) (*DB, string, func()) {
	tempDir, err := os.MkdirTemp("", "orindb_iter_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create a log channel to capture logs for debugging
	logCh := make(chan string, 100)

	opts := &Options{
		Directory:           tempDir,
		WriteBufferSize:     1024 * 1024, // 1MB for faster testing
		SyncOption:          SyncNone,
		SyncInterval:        time.Second,
		LevelCount:          3,          // Fewer levels for testing
		LevelMultiplier:     2,          // Smaller multiplier for testing
		BlockManagerLRUSize: 16,         // Smaller LRU for testing
		BlockSetSize:        1024 * 256, // Smaller block set for testing
		Permission:          0750,
		LogChannel:          logCh,
	}

	db, err := Open(opts)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to open db: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
		// Drain the log channel
		for len(logCh) > 0 {
			<-logCh
		}
	}

	return db, tempDir, cleanup
}

// TestIteratorBasic tests basic iterator functionality
func TestIteratorBasic(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Add some data
	txn := db.Begin()
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Start a new read transaction
	txn = db.Begin()

	// Test forward iteration
	iter := txn.NewIterator(nil)
	count := 0
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		count++
		expectedKey := fmt.Sprintf("key%02d", count)
		expectedValue := fmt.Sprintf("value%02d", count)

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value.([]byte)) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value.([]byte)))
		}
	}
	if count != 10 {
		t.Errorf("expected 10 items, got %d", count)
	}

	// Test backward iteration
	iter = txn.NewIterator(nil)
	for i := 0; i < 5; i++ {
		_, _, ok := iter.Next()
		if !ok {
			t.Fatalf("failed to get item %d", i)
		}
	}

	// Now go backward
	for i := 5; i > 0; i-- {
		key, value, ok := iter.Prev()
		if !ok {
			t.Fatalf("failed to get previous item %d", i)
		}
		expectedKey := fmt.Sprintf("key%02d", i)
		expectedValue := fmt.Sprintf("value%02d", i)

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value.([]byte)) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value.([]byte)))
		}
	}

	// Test seeking
	iter = txn.NewIterator(nil)
	key, value, ok := iter.Seek([]byte("key05"))
	if !ok {
		t.Fatalf("failed to seek")
	}
	if string(key) != "key05" {
		t.Errorf("expected key key05, got %s", string(key))
	}
	if string(value.([]byte)) != "value05" {
		t.Errorf("expected value value05, got %s", string(value.([]byte)))
	}
}

// TestIteratorMultiSource tests iterator across multiple sources (memtable, immutable, sstables)
func TestIteratorMultiSource(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Set a very small buffer size to force frequent flushing
	db.opts.WriteBufferSize = 512 // Small buffer to force flushing

	// Insert data batch 1 (will go to memtable and then flush to level 0)
	txn := db.Begin()
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("batch1_key%02d", i))
		value := []byte(fmt.Sprintf("batch1_value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Force flush memtable to sstable
	memtable := db.memtable.Load().(*Memtable)
	if err := db.flusher.flushMemtable(memtable); err != nil {
		t.Fatalf("failed to flush memtable: %v", err)
	}

	// Insert data batch 2 (will stay in active memtable)
	txn = db.Begin()
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("batch2_key%02d", i))
		value := []byte(fmt.Sprintf("batch2_value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Insert data batch 3 (will be part of transaction write set)
	txn = db.Begin()
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("batch3_key%02d", i))
		value := []byte(fmt.Sprintf("batch3_value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}

	// Test that iterator merges results from all sources
	iter := txn.NewIterator(nil)

	// Collect all results
	results := make(map[string]string)
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		results[string(key)] = string(value.([]byte))
	}

	// Check batch 1 (from sstable)
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("batch1_key%02d", i)
		expectedValue := fmt.Sprintf("batch1_value%02d", i)
		if value, ok := results[key]; !ok || value != expectedValue {
			t.Errorf("expected %s=%s, got %s=%s", key, expectedValue, key, value)
		}
	}

	// Check batch 2 (from memtable)
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("batch2_key%02d", i)
		expectedValue := fmt.Sprintf("batch2_value%02d", i)
		if value, ok := results[key]; !ok || value != expectedValue {
			t.Errorf("expected %s=%s, got %s=%s", key, expectedValue, key, value)
		}
	}

	// Check batch 3 (from transaction write set)
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("batch3_key%02d", i)
		expectedValue := fmt.Sprintf("batch3_value%02d", i)
		if value, ok := results[key]; !ok || value != expectedValue {
			t.Errorf("expected %s=%s, got %s=%s", key, expectedValue, key, value)
		}
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

// TestIteratorWithVersions tests MVCC isolation with different versions of the same key
func TestIteratorWithVersions(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Create first version of data
	txn1 := db.Begin()
	txn1.Put([]byte("key1"), []byte("value1_v1"))
	txn1.Put([]byte("key2"), []byte("value2_v1"))
	if err := txn1.Commit(); err != nil {
		t.Fatalf("failed to commit txn1: %v", err)
	}
	ts1 := txn1.Timestamp

	// Sleep to ensure different timestamps
	time.Sleep(10 * time.Millisecond)

	// Create second version of data
	txn2 := db.Begin()
	txn2.Put([]byte("key1"), []byte("value1_v2"))
	txn2.Put([]byte("key3"), []byte("value3_v2"))
	if err := txn2.Commit(); err != nil {
		t.Fatalf("failed to commit txn2: %v", err)
	}
	ts2 := txn2.Timestamp

	// Create a reader transaction with timestamp between txn1 and txn2
	readerTxn := &Txn{
		Id:        "reader",
		ReadSet:   make(map[string]int64),
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		Timestamp: (ts1 + ts2) / 2, // Timestamp between txn1 and txn2
		Committed: false,
		db:        db,
	}

	// Iterator should see values as of its timestamp
	iter := readerTxn.NewIterator(nil)

	results := make(map[string]string)
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		results[string(key)] = string(value.([]byte))
	}

	// Should see first version of key1, first version of key2, but not key3
	if val, ok := results["key1"]; !ok || val != "value1_v1" {
		t.Errorf("expected key1=value1_v1, got key1=%s", val)
	}
	if val, ok := results["key2"]; !ok || val != "value2_v1" {
		t.Errorf("expected key2=value2_v1, got key2=%s", val)
	}
	if _, ok := results["key3"]; ok {
		t.Errorf("key3 should not be visible at this timestamp")
	}

	// Create a reader transaction with latest timestamp
	latestTxn := db.Begin()

	// Iterator should see the latest values
	iter = latestTxn.NewIterator(nil)

	results = make(map[string]string)
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		results[string(key)] = string(value.([]byte))
	}

	// Should see second version of key1, first version of key2, and key3
	if val, ok := results["key1"]; !ok || val != "value1_v2" {
		t.Errorf("expected key1=value1_v2, got key1=%s", val)
	}
	if val, ok := results["key2"]; !ok || val != "value2_v1" {
		t.Errorf("expected key2=value2_v1, got key2=%s", val)
	}
	if val, ok := results["key3"]; !ok || val != "value3_v2" {
		t.Errorf("expected key3=value3_v2, got key3=%s", val)
	}
}

// TestIteratorWithDeletedKeys tests iterator behavior with deleted keys
func TestIteratorWithDeletedKeys(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Insert data
	txn := db.Begin()
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Delete some keys
	txn = db.Begin()
	for i := 3; i <= 7; i += 2 { // Delete keys 3, 5, 7
		key := []byte(fmt.Sprintf("key%02d", i))
		if err := txn.Delete(key); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
	}

	// Use an iterator on the transaction with deletes
	iter := txn.NewIterator(nil)

	results := make(map[string]string)
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		results[string(key)] = string(value.([]byte))
	}

	// Check we got the expected results
	expectedKeys := []string{"key01", "key02", "key04", "key06", "key08", "key09", "key10"}
	for _, key := range expectedKeys {
		expectedValue := fmt.Sprintf("value%s", key[3:])
		if val, ok := results[key]; !ok || val != expectedValue {
			t.Errorf("expected %s=%s, got %s=%s", key, expectedValue, key, val)
		}
	}

	// Should not have the deleted keys
	deletedKeys := []string{"key03", "key05", "key07"}
	for _, key := range deletedKeys {
		if _, ok := results[key]; ok {
			t.Errorf("deleted key %s should not be in results", key)
		}
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// A new transaction should also not see the deleted keys
	txn = db.Begin()
	iter = txn.NewIterator(nil)

	results = make(map[string]string)
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		results[string(key)] = string(value.([]byte))
	}

	// Check we got the expected results
	for _, key := range expectedKeys {
		expectedValue := fmt.Sprintf("value%s", key[3:])
		if val, ok := results[key]; !ok || val != expectedValue {
			t.Errorf("expected %s=%s, got %s=%s", key, expectedValue, key, val)
		}
	}

	// Should not have the deleted keys
	for _, key := range deletedKeys {
		if _, ok := results[key]; ok {
			t.Errorf("deleted key %s should not be in results", key)
		}
	}
}

// TestIteratorRangeScan tests iterator using GetRange, Count, and Scan methods
func TestIteratorRangeScan(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Insert data
	txn := db.Begin()
	for i := 1; i <= 20; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Test GetRange
	txn = db.Begin()
	results, err := txn.GetRange([]byte("key05"), []byte("key15"))
	if err != nil {
		t.Fatalf("failed to get range: %v", err)
	}

	for i := 5; i <= 15; i++ {
		key := fmt.Sprintf("key%02d", i)
		expectedValue := fmt.Sprintf("value%02d", i)
		if val, ok := results[key]; !ok || string(val) != expectedValue {
			t.Errorf("expected %s=%s, got %s=%s", key, expectedValue, key, string(val))
		}
	}
	if len(results) != 11 {
		t.Errorf("expected 11 results, got %d", len(results))
	}

	// Test Count
	count, err := txn.Count([]byte("key05"), []byte("key15"))
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}
	if count != 11 {
		t.Errorf("expected count 11, got %d", count)
	}

	// Test Scan
	visitedKeys := 0
	scanErr := txn.Scan([]byte("key05"), []byte("key15"), func(key []byte, value []byte) bool {
		i := visitedKeys + 5
		expectedKey := fmt.Sprintf("key%02d", i)
		expectedValue := fmt.Sprintf("value%02d", i)

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value))
		}

		visitedKeys++
		return true
	})

	if scanErr != nil {
		t.Fatalf("failed to scan: %v", scanErr)
	}
	if visitedKeys != 11 {
		t.Errorf("expected to visit 11 keys, visited %d", visitedKeys)
	}

	// Test ForEach
	visitedKeys = 0
	forEachErr := txn.ForEach(func(key []byte, value []byte) bool {
		visitedKeys++
		return true
	})

	if forEachErr != nil {
		t.Fatalf("failed to forEach: %v", forEachErr)
	}
	if visitedKeys != 20 {
		t.Errorf("expected to visit 20 keys, visited %d", visitedKeys)
	}

	// Test early termination
	visitedKeys = 0
	txn.Scan([]byte("key01"), []byte("key20"), func(key []byte, value []byte) bool {
		visitedKeys++
		return visitedKeys < 5 // Stop after visiting 5 keys
	})

	if visitedKeys != 5 {
		t.Errorf("expected to visit 5 keys with early termination, visited %d", visitedKeys)
	}
}

// TestIteratorWithChangingData tests iterator behavior when data changes during iteration
func TestIteratorWithChangingData(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Insert initial data
	txn := db.Begin()
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Start a read transaction and get an iterator
	readTxn := db.Begin()
	iter := readTxn.NewIterator(nil)

	// Read first two keys
	for i := 0; i < 2; i++ {
		key, value, ok := iter.Next()
		if !ok {
			t.Fatalf("failed to get item %d", i)
		}
		expectedKey := fmt.Sprintf("key%02d", i+1)
		expectedValue := fmt.Sprintf("value%02d", i+1)

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value.([]byte)) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value.([]byte)))
		}
	}

	// Modify data in a separate transaction
	modifyTxn := db.Begin()
	modifyTxn.Put([]byte("key03"), []byte("modified_value03"))
	modifyTxn.Delete([]byte("key04"))
	modifyTxn.Put([]byte("key06"), []byte("value06")) // Add a new key
	if err := modifyTxn.Commit(); err != nil {
		t.Fatalf("failed to commit modify txn: %v", err)
	}

	// Continue reading with the original iterator
	// It should see the original values due to MVCC isolation
	remainingKeys := []string{"key03", "key04", "key05"}
	for i, expectedKey := range remainingKeys {
		key, value, ok := iter.Next()
		if !ok {
			t.Fatalf("failed to get item at index %d", i)
		}

		expectedValue := fmt.Sprintf("value%s", expectedKey[3:])

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value.([]byte)) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value.([]byte)))
		}
	}

	// There should be no more keys for this iterator
	_, _, ok := iter.Next()
	if ok {
		t.Errorf("expected no more keys, but got another key")
	}

	// A new transaction should see the modified data
	newTxn := db.Begin()
	newIter := newTxn.NewIterator(nil)

	results := make(map[string]string)
	for {
		key, value, ok := newIter.Next()
		if !ok {
			break
		}
		results[string(key)] = string(value.([]byte))
	}

	// Check modified values
	expectedResults := map[string]string{
		"key01": "value01",
		"key02": "value02",
		"key03": "modified_value03",
		"key05": "value05",
		"key06": "value06",
	}

	if len(results) != len(expectedResults) {
		t.Errorf("expected %d results, got %d", len(expectedResults), len(results))
	}

	for k, v := range expectedResults {
		if results[k] != v {
			t.Errorf("expected %s=%s, got %s=%s", k, v, k, results[k])
		}
	}

	// key04 should not be present (it was deleted)
	if _, ok := results["key04"]; ok {
		t.Errorf("key04 should not be present (deleted)")
	}
}

// TestIteratorBidirectional tests bidirectional iteration capabilities
func TestIteratorBidirectional(t *testing.T) {
	db, _, cleanup := createTempDB(t)
	defer cleanup()

	// Insert data
	txn := db.Begin()
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("failed to put: %v", err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Test zig-zag iteration
	txn = db.Begin()
	iter := txn.NewIterator(nil)

	// Forward 3 items
	for i := 1; i <= 3; i++ {
		key, value, ok := iter.Next()
		if !ok {
			t.Fatalf("failed to get next item %d", i)
		}
		expectedKey := fmt.Sprintf("key%02d", i)
		expectedValue := fmt.Sprintf("value%02d", i)

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value.([]byte)) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value.([]byte)))
		}
	}

	// Backward 2 items
	for i := 2; i >= 1; i-- {
		key, value, ok := iter.Prev()
		if !ok {
			t.Fatalf("failed to get prev item %d", i)
		}
		expectedKey := fmt.Sprintf("key%02d", i)
		expectedValue := fmt.Sprintf("value%02d", i)

		if string(key) != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, string(key))
		}
		if string(value.([]byte)) != expectedValue {
			t.Errorf("expected value %s, got %s", expectedValue, string(value.([]byte)))
		}
	}

	// Forward to the end
	var lastKey string
	for {
		key, _, ok := iter.Next()
		if !ok {
			break
		}
		lastKey = string(key)
	}

	if lastKey != "key10" {
		t.Errorf("expected last key to be key10, got %s", lastKey)
	}

	// Backward all the way
	var firstKey string
	for {
		key, _, ok := iter.Prev()
		if !ok {
			break
		}
		firstKey = string(key)
	}

	if firstKey != "key01" {
		t.Errorf("expected first key to be key01, got %s", firstKey)
	}

	// Test the Iterate method with both directions
	forwardKeys := []string{}
	txn.Iterate(nil, 1, func(key []byte, value []byte) bool {
		forwardKeys = append(forwardKeys, string(key))
		return true
	})

	if len(forwardKeys) != 10 {
		t.Errorf("expected 10 keys in forward iteration, got %d", len(forwardKeys))
	}

	for i, key := range forwardKeys {
		expectedKey := fmt.Sprintf("key%02d", i+1)
		if key != expectedKey {
			t.Errorf("expected key %s at index %d, got %s", expectedKey, i, key)
		}
	}

	backwardKeys := []string{}
	txn.Iterate(nil, -1, func(key []byte, value []byte) bool {
		backwardKeys = append(backwardKeys, string(key))
		return true
	})

	if len(backwardKeys) != 10 {
		t.Errorf("expected 10 keys in backward iteration, got %d", len(backwardKeys))
	}

	for i, key := range backwardKeys {
		expectedKey := fmt.Sprintf("key%02d", 10-i)
		if key != expectedKey {
			t.Errorf("expected key %s at index %d, got %s", expectedKey, i, key)
		}
	}
}

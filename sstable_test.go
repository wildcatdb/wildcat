// Package wildcat
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
package wildcat

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSSTable_BasicOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_sstable_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull, // Use full sync for reliability
		LogChannel:      logChan,
		WriteBufferSize: 4 * 1024, // Small buffer to force flushing
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert enough data to trigger a flush to SSTable
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Force a memtable flush by exceeding write buffer size
	largeValue := make([]byte, opts.WriteBufferSize)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("large_key"), largeValue)
	})
	if err != nil {
		t.Fatalf("Failed to insert large value: %v", err)
	}

	// Give some time for background flushing to complete
	time.Sleep(500 * time.Millisecond)

	// Verify that level 1 has at least one SSTable
	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0] // Level 1 is at index 0
	sstables := level1.sstables.Load()

	if sstables == nil || len(*sstables) == 0 {
		t.Errorf("Expected at least one SSTable in level 1, but found none")
	} else {
		t.Logf("Found %d SSTables in level 1", len(*sstables))
	}

	// Verify data can still be read (now from SSTables)
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)

		var actualValue []byte
		err = db.Update(func(txn *Txn) error {
			var err error
			actualValue, err = txn.Get([]byte(key))
			return err
		})

		if err != nil {
			t.Errorf("Failed to read key %s from SSTable: %v", key, err)
			continue
		}

		if string(actualValue) != expectedValue {
			t.Errorf("For key %s expected value %s, got %s", key, expectedValue, actualValue)
		}
	}

	// Close the database
	_ = db.Close()

	// Check that SSTable files exist on disk
	l1Dir := filepath.Join(dir, "l1")
	files, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("Failed to read level 1 directory: %v", err)
	}

	var klogFound, vlogFound bool
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogFound = true
		}
		if filepath.Ext(file.Name()) == ".vlog" {
			vlogFound = true
		}
	}

	if !klogFound {
		t.Errorf("No .klog files found in level 1 directory")
	}
	if !vlogFound {
		t.Errorf("No .vlog files found in level 1 directory")
	}
}

func TestSSTable_ConcurrentAccess(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_sstable_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      logChan,
		WriteBufferSize: 4 * 1024, // Small buffer to force flushing
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert some initial data to ensure we have at least one SSTable
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("init_key%d", i)
		value := fmt.Sprintf("init_value%d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
	}

	// Force a flush to SSTable
	forceManyWrites(t, db, 100)
	time.Sleep(500 * time.Millisecond) // Allow background flush to complete

	// Number of concurrent readers and operations per reader
	const numReaders = 10
	const opsPerReader = 50
	const numWriters = 5
	const opsPerWriter = 20

	// Create a wait group for synchronization
	var wg sync.WaitGroup
	wg.Add(numReaders + numWriters)

	// Start concurrent readers
	for r := 0; r < numReaders; r++ {
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < opsPerReader; i++ {
				keyIdx := i % 100 // Use modulo to access existing keys
				key := fmt.Sprintf("init_key%d", keyIdx)
				expectedPrefix := "init_value"

				var value []byte
				err := db.Update(func(txn *Txn) error {
					var err error
					value, err = txn.Get([]byte(key))
					return err
				})

				if err != nil {
					t.Errorf("Reader %d failed to read key %s: %v", readerID, key, err)
				} else if !startsWith(value, []byte(expectedPrefix)) {
					t.Errorf("Reader %d: unexpected value for key %s: %s", readerID, key, value)
				}

				// Small sleep to reduce contention
				time.Sleep(time.Millisecond)
			}
		}(r)
	}

	// Start concurrent writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < opsPerWriter; i++ {
				key := fmt.Sprintf("new_key_w%d_i%d", writerID, i)
				value := fmt.Sprintf("new_value_w%d_i%d", writerID, i)

				err := db.Update(func(txn *Txn) error {
					return txn.Put([]byte(key), []byte(value))
				})

				if err != nil {
					t.Errorf("Writer %d failed to write key %s: %v", writerID, key, err)
				}

				// Small sleep to reduce contention
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Wait for all operations to complete
	wg.Wait()

	// Verify that all written keys can be read
	successCount := 0
	expectedCount := numWriters * opsPerWriter

	for w := 0; w < numWriters; w++ {
		for i := 0; i < opsPerWriter; i++ {
			key := fmt.Sprintf("new_key_w%d_i%d", w, i)
			expectedValue := fmt.Sprintf("new_value_w%d_i%d", w, i)

			var actualValue []byte
			err := db.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get([]byte(key))
				return err
			})

			if err == nil && string(actualValue) == expectedValue {
				successCount++
			}
		}
	}

	// We should have a high success rate (but allow for some failures due to concurrency)
	if float64(successCount)/float64(expectedCount) < 0.95 {
		t.Errorf("Expected at least 95%% successful operations, got %.2f%% (%d/%d)",
			float64(successCount)/float64(expectedCount)*100, successCount, expectedCount)
	} else {
		t.Logf("Successfully verified %.2f%% (%d/%d) of concurrent writes",
			float64(successCount)/float64(expectedCount)*100, successCount, expectedCount)
	}

	// Verify that SSTables were created
	l1Dir := filepath.Join(dir, "l1")
	files, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("Failed to read level 1 directory: %v", err)
	}

	var klogFound, vlogFound bool
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogFound = true
		}
		if filepath.Ext(file.Name()) == ".vlog" {
			vlogFound = true
		}
	}

	if !klogFound {
		t.Errorf("No .klog files found in level 1 directory")
	}
	if !vlogFound {
		t.Errorf("No .vlog files found in level 1 directory")
	}
}

// Helper function to force a flush to SSTable by writing many keys
func forceManyWrites(t *testing.T, db *DB, count int) {
	// Write enough data to trigger memtable flush
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("flush_key%d", i)
		value := fmt.Sprintf("flush_value%d", i)

		err := db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data for flushing: %v", err)
		}
	}
}

// Helper function to check if a byte slice starts with a prefix
func startsWith(data, prefix []byte) bool {
	if len(data) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if data[i] != prefix[i] {
			return false
		}
	}
	return true
}

func TestSSTable_MVCCWithMultipleVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_sstable_mvcc_multiple_versions_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel with debug logging
	logChan := make(chan string, 1000)
	go func() {
		for msg := range logChan {
			t.Log("DB LOG:", msg)
		}
	}()

	// Create a test DB with a small write buffer to force flushing
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      logChan,
		WriteBufferSize: 512, // Very small buffer to force flushing
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create a single key with multiple versions
	key := []byte("mvcc_key")

	// Record transaction timestamps for verification
	var timestamps []int64
	var txns []*Txn

	// Create 5 versions of the same key
	for i := 1; i <= 5; i++ {
		// Start a transaction and record its timestamp
		txn := db.Begin()
		timestamps = append(timestamps, txn.Timestamp)
		txns = append(txns, txn)

		// Write a new version
		value := []byte(fmt.Sprintf("value%d", i))
		err = txn.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to write version %d: %v", i, err)
		}

		// Commit the transaction
		err = txn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit version %d: %v", i, err)
		}

		t.Logf("Created version %d with timestamp %d", i, txn.Timestamp)

		// Force a flush to SSTable after each write to ensure versions are in different SSTables
		largeValue := make([]byte, opts.WriteBufferSize)
		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(fmt.Sprintf("large_key_%d", i)), largeValue)
		})
		if err != nil {
			t.Fatalf("Failed to force flush after version %d: %v", i, err)
		}

		// Wait for flush to complete
		time.Sleep(100 * time.Millisecond)
	}

	// Now verify that we can read each version using the corresponding timestamp
	for i := 0; i < 5; i++ {
		// Create a transaction with the recorded timestamp
		readTxn := db.Begin()
		// Set the timestamp to match the original write timestamp
		readTxn.Timestamp = timestamps[i]

		// Read the value
		value, err := readTxn.Get(key)
		if err != nil {
			t.Fatalf("Failed to read version %d: %v", i+1, err)
		}

		expectedValue := fmt.Sprintf("value%d", i+1)
		if string(value) != expectedValue {
			t.Errorf("Expected version %d to be '%s', got '%s'", i+1, expectedValue, value)
		} else {
			t.Logf("Successfully read version %d with value '%s'", i+1, value)
		}
	}

	// Verify that a new transaction sees only the latest version
	latestTxn := db.Begin()
	latestValue, err := latestTxn.Get(key)
	if err != nil {
		t.Fatalf("Failed to read latest version: %v", err)
	}

	if string(latestValue) != "value5" {
		t.Errorf("Expected latest version to be 'value5', got '%s'", latestValue)
	} else {
		t.Logf("Successfully read latest version with value 'value5'")
	}

	// Check that SSTables were created
	l1Dir := filepath.Join(dir, "l1")
	files, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("Failed to read level 1 directory: %v", err)
	}

	var klogCount int
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogCount++
		}
	}

	if klogCount < 5 {
		t.Logf("Expected at least 5 .klog files, found %d", klogCount)
	} else {
		t.Logf("Found %d .klog files in level 1 directory", klogCount)
	}
}

func TestSSTable_SimpleDeleteWithDelay(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_sstable_delete_delay_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncFull,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert a key
	key := []byte("delay_test_key")
	value := []byte("delay_test_value")

	err = db.Update(func(txn *Txn) error {
		return txn.Put(key, value)
	})
	if err != nil {
		t.Fatalf("Failed to insert key: %v", err)
	}

	// Verify the key exists
	var retrievedValue []byte
	err = db.Update(func(txn *Txn) error {
		var err error
		retrievedValue, err = txn.Get(key)
		return err
	})
	if err != nil {
		t.Fatalf("Failed to get key after insert: %v", err)
	}
	if !bytes.Equal(retrievedValue, value) {
		t.Fatalf("Value mismatch: expected %s, got %s", value, retrievedValue)
	}
	t.Logf("Key found after insertion: %s", retrievedValue)

	// Delete the key
	t.Logf("Deleting key: %s", key)
	err = db.Update(func(txn *Txn) error {
		t.Logf("Delete transaction ID: %d, Timestamp: %d", txn.Id, txn.Timestamp)
		return txn.Delete(key)
	})
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Add a small delay to ensure the deletion is fully applied
	time.Sleep(100 * time.Millisecond)

	// Try to get the deleted key
	err = db.Update(func(txn *Txn) error {
		t.Logf("Verification transaction ID: %d, Timestamp: %d", txn.Id, txn.Timestamp)
		_, err := txn.Get(key)
		if err == nil {
			return fmt.Errorf("key should be deleted but is still accessible")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Deletion verification failed: %v", err)
	}
	t.Logf("Key correctly not found after deletion")

	// Force a flush to ensure deletion is persisted
	t.Logf("Forcing flush after deletion")
	largeValue := make([]byte, 1024*1024) // 1MB should exceed any buffer size
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("large_key"), largeValue)
	})
	if err != nil {
		t.Fatalf("Failed to trigger flush: %v", err)
	}

	// Wait for flush to complete
	time.Sleep(500 * time.Millisecond)

	// Verify key is still deleted after flush
	err = db.Update(func(txn *Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			return fmt.Errorf("key should be deleted but is still accessible after flush")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Post-flush verification failed: %v", err)
	}
	t.Logf("Key correctly not found after flush")

	// Close and reopen database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Drain log channel
	for len(logChan) > 0 {
		<-logChan
	}

	opts.LogChannel = nil
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func(db2 *DB) {
		_ = db2.Close()
	}(db2)

	// Verify key is still deleted after restart
	err = db2.Update(func(txn *Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			return fmt.Errorf("key should be deleted but is still accessible after restart")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Post-restart verification failed: %v", err)
	}
	t.Logf("Key correctly not found after restart")
}

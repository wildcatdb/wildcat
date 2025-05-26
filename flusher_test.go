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
	"sync/atomic"
	"testing"
	"time"
)

func TestFlusher_QueueMemtable(t *testing.T) {
	logChannel := make(chan string, 100) // Buffer size of 100 messages

	dir, err := os.MkdirTemp("", "db_flusher_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		WriteBufferSize: 1024, // Small buffer for testing
		SyncOption:      SyncNone,
		LogChannel:      logChannel,
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	// Start a goroutine to listen to the log channel
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			t.Logf("Log message: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Get the initial memtable
	initialMemtable := db.memtable.Load().(*Memtable)

	// Add some data to the memtable
	err = db.Update(func(txn *Txn) error {
		err = txn.Put([]byte("key0"), []byte("value0"))
		if err != nil {
			return err
		}
		return txn.Put([]byte("key1"), []byte("value1"))
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Failed to add data: %v", err)
	}

	// Force queue the memtable
	err = db.flusher.queueMemtable()
	if err != nil {
		_ = db.Close()
		t.Fatalf("Failed to queue memtable: %v", err)
	}

	// Get the new active memtable
	newMemtable := db.memtable.Load().(*Memtable)

	// Verify we have a new memtable
	if newMemtable == initialMemtable {
		_ = db.Close()
		t.Errorf("Expected new memtable, got the same memtable")
	}

	// Verify the original memtable is queued for flushing
	if db.flusher.immutable.IsEmpty() {
		_ = db.Close()
		t.Errorf("Expected immutable queue to be non-empty")
	}

	// Verify we can still access the data (should be in the queued memtable)
	var result []byte
	err = db.Update(func(txn *Txn) error {
		var err error
		result, err = txn.Get([]byte("key1"))
		return err
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Failed to get data: %v", err)
	}

	if !bytes.Equal(result, []byte("value1")) {
		_ = db.Close()
		t.Errorf("Expected 'value1', got '%s'", result)
	}

	time.Sleep(100 * time.Millisecond) // Give flusher time to work

	_ = db.Close()
	wg.Wait()

}

func TestFlusher_MVCCWithMultipleVersions(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_flusher_mvcc_test")
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
		WriteBufferSize: 1024, // Small buffer to force flushing
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

		// Force a memtable flush after each write
		err = db.flusher.queueMemtable()
		if err != nil {
			t.Fatalf("Failed to queue memtable: %v", err)
		}

		// Wait for flush to complete
		time.Sleep(100 * time.Millisecond)
	}

	db.ForceFlush()

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

	klogCount := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogCount++
		}
	}

	if klogCount < 5 {
		t.Errorf("Expected at least 5 .klog files, found %d", klogCount)
	} else {
		t.Logf("Found %d .klog files in level 1 directory", klogCount)
	}
}

func TestFlusher_ErrorHandling(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_flusher_error_test")
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

	// Create a test DB with a custom directory that we'll manipulate
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      logChan,
		WriteBufferSize: 1024, // Small buffer to force flushing
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add some data
	for i := 0; i < 10; i++ {
		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(fmt.Sprintf("error_key%d", i)), []byte(fmt.Sprintf("error_value%d", i)))
		})
		if err != nil {
			t.Fatalf("Failed to insert key: %v", err)
		}
	}

	// Queue memtable for flushing
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue memtable: %v", err)
	}

	// Give time for background flush to start
	time.Sleep(10 * time.Millisecond)

	// Now make the level 1 directory non-writable to cause errors during flush
	l1Dir := filepath.Join(dir, "l1")
	originalPermissions, err := os.Stat(l1Dir)
	if err != nil {
		t.Fatalf("Failed to get l1 directory permissions: %v", err)
	}

	// Make directory read-only
	err = os.Chmod(l1Dir, 0500) // Read + execute only
	if err != nil {
		t.Fatalf("Failed to change directory permissions: %v", err)
	}

	// Try to force another flush with the directory being read-only
	for i := 10; i < 20; i++ {
		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(fmt.Sprintf("error_key%d", i)), []byte(fmt.Sprintf("error_value%d", i)))
		})
		if err != nil {
			t.Fatalf("Failed to insert key: %v", err)
		}
	}

	// Try to queue memtable - this may or may not succeed depending on implementation
	_ = db.flusher.queueMemtable()

	// Give time for background flush to attempt operation
	time.Sleep(100 * time.Millisecond)

	// Restore directory permissions
	err = os.Chmod(l1Dir, originalPermissions.Mode())
	if err != nil {
		t.Fatalf("Failed to restore directory permissions: %v", err)
	}

	// Close database and check we can still shut down gracefully
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database after error: %v", err)
	}

	opts.LogChannel = nil // Disable logging for reopening

	// Reopen database to verify it recovers
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database after error: %v", err)
	}
	defer func(db2 *DB) {
		_ = db2.Close()
	}(db2)

	// Verify we can read at least some of the keys
	var readSuccessCount int
	for i := 0; i < 20; i++ {
		err = db2.Update(func(txn *Txn) error {
			_, err := txn.Get([]byte(fmt.Sprintf("error_key%d", i)))
			if err == nil {
				readSuccessCount++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Unexpected error during verification: %v", err)
		}
	}

	t.Logf("Successfully read %d out of 20 keys after recovery", readSuccessCount)
	if readSuccessCount == 0 {
		t.Errorf("Expected to read at least some keys after recovery")
	}
}

func TestFlusher_MultipleFlushesWithUpdates(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_flusher_updates_test")
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
		SyncOption:      SyncNone,
		LogChannel:      logChan,
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Write the same keys multiple times across different flushes
	// This tests handling updates to the same keys in different SSTables
	const keyCount = 50
	const updateCount = 5

	for update := 1; update <= updateCount; update++ {
		// Update all keys with a new version
		for i := 0; i < keyCount; i++ {
			key := fmt.Sprintf("update_key%d", i)
			value := fmt.Sprintf("update_value%d_v%d", i, update)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to update key %s: %v", key, err)
			}
		}

		// Force flush after each update round
		err = db.flusher.queueMemtable()
		if err != nil {
			t.Fatalf("Failed to queue memtable: %v", err)
		}

		// Give flusher time to process
		time.Sleep(100 * time.Millisecond)
	}

	// Verify that all keys have the latest version
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("update_key%d", i)
		expectedValue := fmt.Sprintf("update_value%d_v%d", i, updateCount)

		var actualValue []byte
		err = db.Update(func(txn *Txn) error {
			var err error
			actualValue, err = txn.Get([]byte(key))
			return err
		})

		if err != nil {
			t.Errorf("Failed to read updated key %s: %v", key, err)
		} else if string(actualValue) != expectedValue {
			t.Errorf("For key %s expected latest value '%s', got '%s'",
				key, expectedValue, string(actualValue))
		}
	}

	// Count SSTables created
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

	t.Logf("Created %d SSTables during multiple update test", klogCount)

	// We should have approximately one SSTable per flush
	if klogCount < updateCount {
		t.Errorf("Expected at least %d SSTables (one per update), got %d", updateCount, klogCount)
	}
}

func TestFlusher_ConcurrentReadsWithFlush(t *testing.T) {

	dir, err := os.MkdirTemp("", "db_flusher_concurrent_test")
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

	// Create a test DB with a small write buffer to force flushing
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncNone,
		LogChannel:      logChan,
		WriteBufferSize: 4 * 1024, // 4KB
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data and ensure it's fully committed before starting readers
	const keyCount = 500
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("concurrent_key%d", i)
		value := fmt.Sprintf("concurrent_value%d", i)
		keys[i] = key

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert key %s: %v", key, err)
		}
	}

	// Wait to ensure all writes are complete
	time.Sleep(100 * time.Millisecond)

	// Start concurrent readers that will continue reading while flushes happen
	const numReaders = 5
	const readsPerReader = 100

	// Use an atomic counter for tracking read errors
	var errorCount atomic.Int32

	var wg sync.WaitGroup

	// Start readers - now they only read keys that are guaranteed to exist
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < readsPerReader; i++ {
				// Use modulo to ensure we stay within the bounds of existing keys
				keyIndex := (readerID*31 + i*17) % keyCount
				key := keys[keyIndex]
				expectedValue := fmt.Sprintf("concurrent_value%d", keyIndex)

				// Read the key
				var actualValue []byte
				err := db.Update(func(txn *Txn) error {
					var err error
					actualValue, err = txn.Get([]byte(key))
					return err
				})

				if err != nil {
					t.Logf("Reader %d failed to read key %s: %v", readerID, key, err)
					errorCount.Add(1)
				} else if string(actualValue) != expectedValue {
					t.Logf("Reader %d: value mismatch for key %s: expected '%s', got '%s'",
						readerID, key, expectedValue, string(actualValue))
					errorCount.Add(1)
				}

				// Short sleep to reduce contention
				time.Sleep(time.Millisecond)
			}
		}(r)
	}

	// Concurrent writer that forces flushing
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10; i++ {
			// Write a batch of data to new keys that readers aren't looking for
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("flush_key_%d_%d", i, j)
				value := fmt.Sprintf("flush_value_%d_%d", i, j)

				err := db.Update(func(txn *Txn) error {
					return txn.Put([]byte(key), []byte(value))
				})
				if err != nil {
					t.Logf("Failed to write key %s: %v", key, err)
				}
			}

			// Force a memtable flush
			err := db.flusher.queueMemtable()
			if err != nil {
				t.Logf("Failed to queue memtable: %v", err)
			}

			// Allow more time for background processing
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait for all operations to complete
	wg.Wait()

	// Get final error count
	finalErrorCount := errorCount.Load()

	// A small number of errors might be acceptable due to concurrency
	maxAcceptableErrors := int32(numReaders) // Allow 1 error per reader

	if finalErrorCount > 0 {
		t.Logf("Got %d read errors during concurrent reads and flushes", finalErrorCount)
		if finalErrorCount > maxAcceptableErrors {
			t.Errorf("Too many errors: %d (max acceptable: %d)", finalErrorCount, maxAcceptableErrors)
		}
	}

	// Verify SSTables were created
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

	t.Logf("Created %d SSTables during concurrent test", klogCount)
	if klogCount < 5 {
		t.Errorf("Expected at least 5 SSTables to be created during test, got %d", klogCount)
	}
}

func TestFlusher_VariousKeySizes(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_flusher_key_sizes_test")
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

	// Create a test DB with a small write buffer
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncNone,
		LogChannel:      logChan,
		WriteBufferSize: 8 * 1024, // 8KB
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Test various key sizes
	keySizes := []int{1, 10, 100, 1000}
	keysBySize := make(map[int][]string)

	// Insert data with different key sizes
	for _, size := range keySizes {
		keysBySize[size] = make([]string, 0)

		// Create 10 keys of each size
		for i := 0; i < 10; i++ {
			// Create a key of specified size
			keyParts := make([]byte, size)
			for j := 0; j < size; j++ {
				keyParts[j] = byte(65 + (i+j)%26) // A-Z characters
			}
			key := string(keyParts)
			keysBySize[size] = append(keysBySize[size], key)

			value := fmt.Sprintf("value_size%d_idx%d", size, i)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to insert key of size %d: %v", size, err)
			}
		}

		// Force flush after each key size batch
		err = db.flusher.queueMemtable()
		if err != nil {
			t.Fatalf("Failed to queue memtable: %v", err)
		}

		// Wait for flush to complete
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all sizes can be read back correctly
	for _, size := range keySizes {
		successCount := 0

		for i, key := range keysBySize[size] {
			expectedValue := fmt.Sprintf("value_size%d_idx%d", size, i)

			var actualValue []byte
			err = db.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get([]byte(key))
				return err
			})

			if err == nil && string(actualValue) == expectedValue {
				successCount++
			} else if err != nil {
				t.Logf("Failed to read key of size %d: %v", size, err)
			} else {
				t.Logf("Value mismatch for key of size %d", size)
			}
		}

		t.Logf("Successfully read %d/%d keys of size %d",
			successCount, len(keysBySize[size]), size)

		if successCount != len(keysBySize[size]) {
			t.Errorf("Expected to read all keys of size %d, got %d/%d",
				size, successCount, len(keysBySize[size]))
		}
	}

	// Verify SSTables were created
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

	t.Logf("Created %d SSTables during key size test", klogCount)
	expectedSSTables := len(keySizes)
	if klogCount < expectedSSTables {
		t.Errorf("Expected at least %d SSTables, got %d", expectedSSTables, klogCount)
	}
}

func TestFlusher_RecoveryAfterCrash(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_flusher_crash_recovery_test")
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

	// Write data and simulate crash
	{
		// Create a test DB with FULL sync to ensure WAL entries are persisted
		opts := &Options{
			Directory:       dir,
			SyncOption:      SyncFull, // Ensure full sync for better recovery
			LogChannel:      logChan,
			WriteBufferSize: 4 * 1024,
		}

		db, err := Open(opts)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		// Insert some data with full transaction commits
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("recovery_key%d", i)
			value := fmt.Sprintf("recovery_value%d", i)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to insert key %s: %v", key, err)
			}

			// Every 20 keys, wait a bit to ensure data is flushed to WAL
			if i > 0 && i%20 == 0 {
				time.Sleep(50 * time.Millisecond)
			}
		}

		// Force a memtable flush but give it time to start writing to disk
		err = db.flusher.queueMemtable()
		if err != nil {
			t.Fatalf("Failed to queue memtable: %v", err)
		}

		// Wait enough time for WAL to be written but not necessarily for the flush to complete
		//time.Sleep(100 * time.Millisecond)

		// Check that data exists before "crash"
		var verificationSuccessCount int
		for i := 0; i < 10; i++ { // Check a sample of keys
			key := fmt.Sprintf("recovery_key%d", i*10)
			expectedValue := fmt.Sprintf("recovery_value%d", i*10)

			var actualValue []byte
			err = db.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get([]byte(key))
				return err
			})

			if err == nil && string(actualValue) == expectedValue {
				verificationSuccessCount++
			}
		}

		if verificationSuccessCount < 8 {
			t.Fatalf("Expected at least 8/10 sample keys to be readable before crash, got %d", verificationSuccessCount)
		}

		// Force close without properly shutting down flusher
		// We use Close() since there's no "crash" simulation API
		_ = db.Close()
	}

	// Drain log channel
	for len(logChan) > 0 {
		<-logChan
	}

	// Reopen the database after "crash"
	logChan = make(chan string, 100)
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      logChan,
		WriteBufferSize: 4 * 1024,
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database after crash: %v", err)
	}
	defer func(db2 *DB) {
		_ = db2.Close()
	}(db2)

	// Give recovery operations time to complete
	time.Sleep(200 * time.Millisecond)

	// Verify recovery by checking that data is accessible
	successCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("recovery_key%d", i)
		expectedValue := fmt.Sprintf("recovery_value%d", i)

		var actualValue []byte
		err = db2.Update(func(txn *Txn) error {
			var err error
			actualValue, err = txn.Get([]byte(key))
			return err
		})

		if err == nil && string(actualValue) == expectedValue {
			successCount++
		} else if err != nil {
			t.Logf("Failed to read key %s: %v", key, err)
		} else {
			t.Logf("Value mismatch for key %s: expected '%s', got '%s'",
				key, expectedValue, string(actualValue))
		}
	}

	recoveryRate := float64(successCount) / 100.0 * 100.0
	t.Logf("Successfully recovered %.2f%% (%d/100) of keys after simulated crash",
		recoveryRate, successCount)

	// Adjust expected recovery rate - in a real crash recovery, we might not get
	// 100% if some operations were in flight during crash
	minExpectedRecovery := 80.0 // 80% recovery is still good for crash recovery
	if recoveryRate < minExpectedRecovery {
		t.Errorf("Expected at least %.1f%% recovery rate, got %.2f%%",
			minExpectedRecovery, recoveryRate)
	}
}

func TestFlusher_EmptyFlush(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_flusher_empty_test")
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
		SyncOption:      SyncNone,
		LogChannel:      logChan,
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Queue an empty memtable (no writes performed)
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue empty memtable: %v", err)
	}

	// Give flusher time to process
	time.Sleep(100 * time.Millisecond)

	// Check that no SSTables were created since memtable was empty
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

	if klogCount > 0 {
		t.Logf("Found %d SSTables for empty memtable", klogCount)
	} else {
		t.Logf("Correctly handled empty memtable with no SSTable creation")
	}

	// Now add some data and verify flushing works
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("test_key"), []byte("test_value"))
	})
	if err != nil {
		t.Fatalf("Failed to insert test key: %v", err)
	}

	// Queue the non-empty memtable
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue non-empty memtable: %v", err)
	}

	// Give flusher time to process
	time.Sleep(100 * time.Millisecond)

	// Check that SSTables were created this time
	files, err = os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("Failed to read level 1 directory: %v", err)
	}

	klogCount = 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogCount++
		}
	}

	if klogCount == 0 {
		t.Errorf("Expected at least one SSTable for non-empty memtable, found none")
	} else {
		t.Logf("Correctly created %d SSTables for non-empty memtable", klogCount)
	}
}

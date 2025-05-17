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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFlusher_QueueMemtable(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "orindb_flusher_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		WriteBufferSize: 1024, // Small buffer for testing
		SyncOption:      SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get the initial memtable
	initialMemtable := db.memtable.Load().(*Memtable)

	// Add some data to the memtable
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("key1"), []byte("value1"))
	})
	if err != nil {
		t.Fatalf("Failed to add data: %v", err)
	}

	// Force queue the memtable
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue memtable: %v", err)
	}

	// Get the new active memtable
	newMemtable := db.memtable.Load().(*Memtable)

	// Verify we have a new memtable
	if newMemtable == initialMemtable {
		t.Errorf("Expected new memtable, got the same memtable")
	}

	// Verify the original memtable is queued for flushing
	if db.flusher.immutable.IsEmpty() {
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
		t.Fatalf("Failed to get data: %v", err)
	}

	if !bytes.Equal(result, []byte("value1")) {
		t.Errorf("Expected 'value1', got '%s'", result)
	}
}

func TestFlusher_FlushMemtable(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "orindb_flusher_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		WriteBufferSize: 1024, // Small buffer for testing
		SyncOption:      SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add some data to the memtable
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			t.Fatalf("Failed to add data: %v", err)
		}
	}

	// Force queue the memtable
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue memtable: %v", err)
	}

	// Get the queued memtable
	queuedMemtable := db.flusher.immutable.Peek().(*Memtable)

	// Manually flush the memtable
	err = db.flusher.flushMemtable(queuedMemtable)
	if err != nil {
		t.Fatalf("Failed to flush memtable: %v", err)
	}

	// Check that level 1 has an SSTable
	levels := db.levels.Load()
	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	if sstables == nil || len(*sstables) == 0 {
		t.Errorf("Expected at least one SSTable in level 1")
	}

	// Close and reopen the DB to ensure recovery works
	db.Close()

	// Reopen DB
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify we can still access all the data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))

		var result []byte
		err = db.Update(func(txn *Txn) error {
			var err error
			result, err = txn.Get(key)
			return err
		})
		if err != nil {
			t.Fatalf("Failed to get data for key%d: %v", i, err)
		}

		if !bytes.Equal(result, expectedValue) {
			t.Errorf("For key%d expected '%s', got '%s'", i, expectedValue, result)
		}
	}
}

func TestFlusher_BackgroundProcess(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "orindb_flusher_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a log channel to capture logs
	logChannel := make(chan string, 100)

	// Create a test DB with logging
	opts := &Options{
		Directory:       dir,
		WriteBufferSize: 4096, // Small buffer to trigger flushing
		SyncOption:      SyncNone,
		LogChannel:      logChannel,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add enough data to trigger automatic flushing
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d-with-some-extra-data-to-fill-memtable", i))
		err = db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			t.Fatalf("Failed to add data: %v", err)
		}
	}

	// Wait for background flushing to occur
	time.Sleep(500 * time.Millisecond)

	// Check that level 1 has at least one SSTable
	levels := db.levels.Load()
	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	if sstables == nil || len(*sstables) == 0 {
		// Drain the log channel to see what happened
		close(logChannel)
		var logs []string
		for log := range logChannel {
			logs = append(logs, log)
		}
		t.Errorf("Expected at least one SSTable in level 1. Logs: %v", logs)
	}

	// Close the DB
	db.Close()

	// Check for KLog and VLog files in level 1 directory
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
		t.Errorf("Expected to find KLog files in level 1")
	}
	if !vlogFound {
		t.Errorf("Expected to find VLog files in level 1")
	}
}

func TestFlusher_ConcurrentAccess(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "orindb_flusher_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		WriteBufferSize: 8192, // Medium-sized buffer
		SyncOption:      SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Number of concurrent writers
	const numWriters = 10
	// Number of writes per writer
	const writesPerWriter = 100

	// Use a WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Start concurrent writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()

			// Each writer adds its own set of keys
			for i := 0; i < writesPerWriter; i++ {
				key := []byte(fmt.Sprintf("writer%d-key%d", writerID, i))
				value := []byte(fmt.Sprintf("value-from-writer-%d-%d", writerID, i))

				err := db.Update(func(txn *Txn) error {
					return txn.Put(key, value)
				})
				if err != nil {
					t.Errorf("Writer %d failed to write key %d: %v", writerID, i, err)
					return
				}
			}
		}(w)
	}

	// Wait for all writers to finish
	wg.Wait()

	// Force a final flush
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue final memtable: %v", err)
	}

	// Wait for background processing
	time.Sleep(500 * time.Millisecond)

	// Verify all data is accessible
	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			key := []byte(fmt.Sprintf("writer%d-key%d", w, i))
			expectedValue := []byte(fmt.Sprintf("value-from-writer-%d-%d", w, i))

			var result []byte
			err = db.Update(func(txn *Txn) error {
				var err error
				result, err = txn.Get(key)
				return err
			})
			if err != nil {
				t.Fatalf("Failed to get data for writer%d-key%d: %v", w, i, err)
			}

			if !bytes.Equal(result, expectedValue) {
				t.Errorf("For writer%d-key%d expected '%s', got '%s'", w, i, expectedValue, result)
			}
		}
	}

	// Check that we have at least one SSTable from the flushes
	levels := db.levels.Load()
	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	if sstables == nil || len(*sstables) == 0 {
		t.Errorf("Expected at least one SSTable in level 1")
	}
}

func TestFlusher_RecoveryAfterCrash(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "orindb_flusher_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		WriteBufferSize: 4096,     // Small buffer for testing
		SyncOption:      SyncFull, // Use full sync for this test
	}

	// Phase 1: Write data
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write some data
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("recovery-key%d", i))
		value := []byte(fmt.Sprintf("recovery-value%d", i))
		err = db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			t.Fatalf("Failed to add data: %v", err)
		}
	}

	// Force queue the memtable to generate some SSTables
	err = db.flusher.queueMemtable()
	if err != nil {
		t.Fatalf("Failed to queue memtable: %v", err)
	}

	// Wait for background processing
	time.Sleep(200 * time.Millisecond)

	// Add more data to the new memtable
	for i := 500; i < 1000; i++ {
		key := []byte(fmt.Sprintf("recovery-key%d", i))
		value := []byte(fmt.Sprintf("recovery-value%d", i))
		err = db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			t.Fatalf("Failed to add data: %v", err)
		}
	}

	// Close the database (simulating a clean shutdown)
	db.Close()

	// Phase 2: Reopen and verify recovery
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify all data is accessible after recovery
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("recovery-key%d", i))
		expectedValue := []byte(fmt.Sprintf("recovery-value%d", i))

		var result []byte
		err = db.Update(func(txn *Txn) error {
			var err error
			result, err = txn.Get(key)
			return err
		})
		if err != nil {
			t.Fatalf("After recovery, failed to get data for key%d: %v", i, err)
		}

		if !bytes.Equal(result, expectedValue) {
			t.Errorf("After recovery, for key%d expected '%s', got '%s'", i, expectedValue, result)
		}
	}
}

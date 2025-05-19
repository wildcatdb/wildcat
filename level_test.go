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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLevel_BasicOperations(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_level_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

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

	// Insert enough data to create SSTables in level 1
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
	time.Sleep(2 * time.Second)

	// Get the level 1
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

	// Check level properties
	if level1.id != 1 {
		t.Errorf("Expected level ID to be 1, got %d", level1.id)
	}

	if level1.getSize() <= 0 {
		t.Errorf("Expected level size to be greater than 0, got %d", level1.getSize())
	}

	t.Logf("Level 1 size reported as: %d", level1.getSize())

	// Close the DB to ensure all data is flushed
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Check that level directory exists on disk
	levelDir := filepath.Join(dir, "l1")
	if _, err := os.Stat(levelDir); os.IsNotExist(err) {
		t.Fatalf("Level directory does not exist: %s", levelDir)
	}

	// Check for SSTable files in the level directory
	files, err := os.ReadDir(levelDir)
	if err != nil {
		t.Fatalf("Failed to read level directory: %v", err)
	}

	var klogCount, vlogCount int
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogCount++
		}
		if filepath.Ext(file.Name()) == ".vlog" {
			vlogCount++
		}
	}

	if klogCount == 0 {
		t.Errorf("No .klog files found in level directory")
	}
	if vlogCount == 0 {
		t.Errorf("No .vlog files found in level directory")
	}

	t.Logf("Found %d .klog files and %d .vlog files in level directory", klogCount, vlogCount)
}

func TestLevel_Reopen(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_level_reopen_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a log channel that won't be closed in this test
	logChan := make(chan string, 100)

	// First DB instance to create data
	{
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

		// Insert data that will be flushed to SSTable
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("reopen_key%d", i)
			value := fmt.Sprintf("reopen_value%d", i)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Force flush with large value
		largeValue := make([]byte, opts.WriteBufferSize)
		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte("large_key"), largeValue)
		})
		if err != nil {
			t.Fatalf("Failed to insert large value: %v", err)
		}

		// Give time for flush to complete
		time.Sleep(500 * time.Millisecond)

		// Get level info before closing
		levels := db.levels.Load()
		level1 := (*levels)[0]
		sstables := level1.sstables.Load()
		if sstables == nil {
			t.Fatalf("No SSTables found in level 1 before closing")
		}
		originalSSTableCount := len(*sstables)
		originalSize := level1.getSize()

		t.Logf("Before closing: Level 1 has %d SSTables and size %d", originalSSTableCount, originalSize)

		// Close DB to ensure all data is persisted
		err = db.Close()
		if err != nil {
			t.Fatalf("Failed to close first database instance: %v", err)
		}

		// Drain log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}

	// Second DB instance to test reopening
	{
		// Create new log channel for second instance
		logChan = make(chan string, 100)
		defer func() {
			for len(logChan) > 0 {
				<-logChan
			}
		}()

		opts := &Options{
			Directory:       dir,
			SyncOption:      SyncFull,
			LogChannel:      logChan,
			WriteBufferSize: 4 * 1024,
		}

		db2, err := Open(opts)
		if err != nil {
			t.Fatalf("Failed to reopen database: %v", err)
		}

		// Check that level 1 was properly restored
		levels := db2.levels.Load()
		level1 := (*levels)[0]
		sstables := level1.sstables.Load()

		if sstables == nil || len(*sstables) == 0 {
			t.Errorf("Expected SSTables in reopened level 1, but found none")
		} else {
			t.Logf("After reopening: Level 1 has %d SSTables and size %d",
				len(*sstables), level1.getSize())
		}

		// Verify data can be read after reopening
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("reopen_key%d", i)
			expectedValue := fmt.Sprintf("reopen_value%d", i)

			var actualValue []byte
			err = db2.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get([]byte(key))
				return err
			})

			if err != nil {
				t.Errorf("Failed to read key %s after reopening: %v", key, err)
			} else if string(actualValue) != expectedValue {
				t.Errorf("Value mismatch for key %s: expected '%s', got '%s'",
					key, expectedValue, string(actualValue))
			}
		}

		// Close properly
		err = db2.Close()
		if err != nil {
			t.Fatalf("Failed to close second database instance: %v", err)
		}
	}
}

func TestLevel_SizeMethods(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_level_size_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

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

	// Get the level 1
	levels := db.levels.Load()
	level1 := (*levels)[0] // Level 1 is at index 0

	// Check initial size
	initialSize := level1.getSize()
	t.Logf("Initial level 1 size: %d", initialSize)

	// Set size and verify it's updated
	testSize := int64(12345)
	level1.setSize(testSize)

	if level1.getSize() != testSize {
		t.Errorf("Size not updated correctly: expected %d, got %d",
			testSize, level1.getSize())
	}

	// Test with actual data
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

	// Force flush to SSTable with large value
	largeValue := make([]byte, opts.WriteBufferSize)
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("large_key"), largeValue)
	})
	if err != nil {
		t.Fatalf("Failed to insert large value: %v", err)
	}

	// Give time for background operations
	time.Sleep(500 * time.Millisecond)

	// After data insertion and flush, size should have increased
	currentSize := level1.getSize()
	t.Logf("Level 1 size after data insertion: %d", currentSize)

	if currentSize <= testSize {
		t.Errorf("Expected level size to increase after data insertion")
	}

	// Close the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestLevel_ErrorHandling(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_level_error_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

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

	// Add some data and flush to create SSTable files
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("error_key%d", i)
		value := fmt.Sprintf("error_value%d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Force flush
	largeValue := make([]byte, opts.WriteBufferSize)
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("large_key"), largeValue)
	})
	if err != nil {
		t.Fatalf("Failed to insert large value: %v", err)
	}

	// Wait for flush to complete
	time.Sleep(500 * time.Millisecond)

	// Close the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Corrupt a level by removing vlog file but keeping klog file
	levelDir := filepath.Join(dir, "l1")
	files, err := os.ReadDir(levelDir)
	if err != nil {
		t.Fatalf("Failed to read level directory: %v", err)
	}

	// Find a klog file and its corresponding vlog file
	var klogFile, vlogFile string
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".klog" {
			klogFile = file.Name()
			vlogFile = klogFile[:len(klogFile)-5] + ".vlog" // Replace .klog with .vlog
			break
		}
	}

	if klogFile == "" {
		t.Fatalf("No .klog file found to test error handling")
	}

	// Remove the vlog file to create an inconsistent state
	err = os.Remove(filepath.Join(levelDir, vlogFile))
	if err != nil {
		t.Fatalf("Failed to remove vlog file: %v", err)
	}
	t.Logf("Removed vlog file %s to test error handling", vlogFile)

	opts.LogChannel = make(chan string, 100)

	// Try to reopen the database
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database with corrupted level: %v", err)
	}

	// The level.reopen method should skip the corrupted SSTable
	levels := db2.levels.Load()
	level1 := (*levels)[0]
	sstables := level1.sstables.Load()
	sstCount := 0
	if sstables != nil {
		sstCount = len(*sstables)
	}
	t.Logf("Reopened database with %d valid SSTables in level 1", sstCount)

	// Try to read data from non-corrupted SSTables
	successCount := 0
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("error_key%d", i)
		err = db2.Update(func(txn *Txn) error {
			_, err := txn.Get([]byte(key))
			return err
		})
		if err == nil {
			successCount++
		}
	}
	t.Logf("Successfully read %d of 50 keys after corruption", successCount)

	// Close the database
	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close reopened database: %v", err)
	}
}

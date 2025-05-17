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
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTransactionSerialization(t *testing.T) {
	// Create a sample transaction
	tx := &Txn{
		ReadSet:   map[string]int64{"key1": 123456789},
		WriteSet:  map[string][]byte{"key2": []byte("value2")},
		DeleteSet: map[string]bool{"key3": true},
		Timestamp: 987654321,
		Committed: false,
	}

	// Serialize the transaction
	serializedData, err := tx.serializeTransaction()
	if err != nil {
		t.Fatalf("Failed to serialize transaction: %v", err)
	}

	// Deserialize the transaction into a new object
	newTx := &Txn{}
	err = newTx.deserializeTransaction(serializedData)
	if err != nil {
		t.Fatalf("Failed to deserialize transaction: %v", err)
	}

	// Compare the original and deserialized transactions
	if tx.Timestamp != newTx.Timestamp {
		t.Errorf("Timestamps do not match: got %v, want %v", newTx.Timestamp, tx.Timestamp)
	}

	if tx.Committed != newTx.Committed {
		t.Errorf("Committed flags do not match: got %v, want %v", newTx.Committed, tx.Committed)
	}

	if !compareMaps(tx.ReadSet, newTx.ReadSet) {
		t.Errorf("ReadSets do not match: got %v, want %v", newTx.ReadSet, tx.ReadSet)
	}

	if !compareByteMaps(tx.WriteSet, newTx.WriteSet) {
		t.Errorf("WriteSets do not match: got %v, want %v", newTx.WriteSet, tx.WriteSet)
	}

	if !compareBoolMaps(tx.DeleteSet, newTx.DeleteSet) {
		t.Errorf("DeleteSets do not match: got %v, want %v", newTx.DeleteSet, tx.DeleteSet)
	}
}

// Helper function to compare two maps of string to int64
func compareMaps(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Helper function to compare two maps of string to []byte
func compareByteMaps(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !bytes.Equal(b[k], v) {
			return false
		}
	}
	return true
}

// Helper function to compare two maps of string to bool
func compareBoolMaps(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestOpen(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open(&Options{
		Directory: "testdb",
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

}

func TestDB_Begin(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open(&Options{
		Directory: "testdb",
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx := db.Begin()

	defer tx.Rollback()

	if tx == nil {
		t.Fatal("Transaction is nil")
	}
}

func TestTransaction_Commit(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open(&Options{
		Directory: "testdb",
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx := db.Begin()

	tx.ReadSet["key1"] = 123456789
	tx.WriteSet["key2"] = []byte("value2")
	tx.DeleteSet["key3"] = true

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if !tx.Committed {
		t.Fatal("Transaction was not committed")
	}
}

func TestTransaction_Get(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open(&Options{
		Directory: "testdb",
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx := db.Begin()

	err = tx.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)

	}

	value, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("Expected value1, got %s", value)
	}

}

func TestFlush(t *testing.T) {

	keys := make([][]byte, 1000)
	values := make([][]byte, 1000)

	flush := 0

	for i := 0; i < 1000; i++ {
		keys[i] = []byte("key" + fmt.Sprintf("%d", i))
		values[i] = []byte("value" + fmt.Sprintf("%d", i))
		flush += len(values[i]) + len(keys[i])
	}

	defer os.RemoveAll("testdb")
	db, err := Open(&Options{
		Directory:       "testdb",
		WriteBufferSize: int64(flush / 4),
		BlockSetSize:    int64(flush / 8),
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	for i := 0; i < 1000; i++ {
		tx := db.Begin()
		if tx == nil {
			t.Fatal("Transaction is nil")
		}

		err = tx.Put(keys[i], values[i])

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	}

	time.Sleep(time.Second * 2)

	for i := 0; i < 10; i++ {
		tx := db.Begin()
		if tx == nil {
			t.Fatal("Transaction is nil")
		}

		value, err := tx.Get(keys[i])
		if err != nil {
			t.Logf("Failed to get value: %v", err)
			continue
		}

		if !bytes.Equal(value, values[i]) {
			t.Fatalf("Expected %s, got %s", values[i], value)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

	}
}

func TestCompaction(t *testing.T) {
	// Clean up after the test
	defer os.RemoveAll("testdb")

	// Create a DB with small buffer sizes to trigger compactions quickly
	db, err := Open(&Options{
		Directory:       "testdb",
		WriteBufferSize: 1 * 1024 * 1024, // 1MB write buffer (small to trigger compactions faster)
		LevelMultiplier: 4,               // Each level is 4x the size of the previous
		LevelCount:      4,               // Use 4 levels to see multi-level compaction
		BlockSetSize:    256 * 1024,      // 256KB block size
		SyncOption:      SyncNone,        // Don't sync for faster test execution
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create dataset large enough to trigger multiple level compactions
	// We'll create multiple waves of data with different keys to force overlapping SSTables
	entryCount := 100000 // 100K entries should be enough
	waves := 5           // Number of insertion waves to create overlapping SSTables

	t.Logf("Starting compaction test with %d entries in %d waves", entryCount, waves)

	// Function to write a wave of data
	writeWave := func(waveNum int, keyPrefix string) {
		t.Logf("Writing wave %d with prefix %s", waveNum, keyPrefix)

		startTime := time.Now()
		batchSize := 100 // Commit in batches to avoid huge transactions

		for i := 0; i < entryCount; i += batchSize {
			// Use batch updates for efficiency
			err := db.Update(func(tx *Txn) error {
				end := i + batchSize
				if end > entryCount {
					end = entryCount
				}

				for j := i; j < end; j++ {
					key := []byte(fmt.Sprintf("%s%07d", keyPrefix, j))
					// Create values of varying sizes to create more realistic data
					valueSize := 64 + (j % 256) // Values between 64 and 320 bytes
					value := make([]byte, valueSize)

					// Fill with a recognizable pattern
					for k := 0; k < valueSize; k++ {
						value[k] = byte((j + k) % 256)
					}

					err := tx.Put(key, value)
					if err != nil {
						return fmt.Errorf("failed to put key %s: %w", key, err)
					}
				}
				return nil
			})

			if err != nil {
				t.Fatalf("Failed to update batch: %v", err)
			}

			// Log progress
			if i%10000 == 0 && i > 0 {
				t.Logf("  Inserted %d/%d entries", i, entryCount)
			}
		}

		elapsed := time.Since(startTime)
		t.Logf("Completed wave %d in %v", waveNum, elapsed)
	}

	// Write the first wave
	writeWave(1, "a")

	// Wait for background compactions to start
	time.Sleep(1 * time.Second)

	// Write more waves to trigger more compactions with overlapping keys
	// Each wave will have a different key prefix
	prefixes := []string{"b", "c", "d", "e"}
	for i, prefix := range prefixes {
		writeWave(i+2, prefix)

		// Wait for background compactions to process
		time.Sleep(1 * time.Second)

		// Verify we can read data from the previous wave
		previousPrefix := "a"
		if i > 0 {
			previousPrefix = prefixes[i-1]
		}

		t.Logf("Verifying data from wave with prefix %s", previousPrefix)

		// Sample verification - check every 1000th key
		for j := 0; j < entryCount; j += 1000 {
			key := []byte(fmt.Sprintf("%s%07d", previousPrefix, j))

			var value []byte
			err := db.Update(func(tx *Txn) error {
				var err error
				value, err = tx.Get(key)
				return err
			})

			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
				continue
			}

			// Verify the value pattern
			valueSize := 64 + (j % 256)
			if len(value) != valueSize {
				t.Errorf("Value size mismatch for key %s: got %d, want %d", key, len(value), valueSize)
				continue
			}

			// Check a sample byte in the value
			sampleIdx := valueSize / 2
			expectedByte := byte((j + sampleIdx) % 256)
			if value[sampleIdx] != expectedByte {
				t.Errorf("Value mismatch for key %s at position %d: got %d, want %d",
					key, sampleIdx, value[sampleIdx], expectedByte)
			}
		}
	}

	// Force compaction by doing random updates and deletes
	t.Log("Performing random updates and deletes to trigger more compactions")

	// Generate random updates to existing keys
	for i := 0; i < 10000; i++ {
		waveIdx := i % len(prefixes)
		prefix := prefixes[waveIdx]
		keyNum := rand.Intn(entryCount)
		key := []byte(fmt.Sprintf("%s%07d", prefix, keyNum))

		if rand.Intn(10) < 7 { // 70% updates, 30% deletes
			// Update
			db.Update(func(tx *Txn) error {
				value := make([]byte, 128)
				rand.Read(value)
				return tx.Put(key, value)
			})
		} else {
			// Delete
			db.Update(func(tx *Txn) error {
				return tx.Delete(key)
			})
		}

		if i%1000 == 0 {
			t.Logf("  Completed %d random operations", i)
			// Allow time for compactions
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Final verification - check we can still read keys from all waves
	t.Log("Performing final verification")
	allPrefixes := append([]string{"a"}, prefixes...)

	for _, prefix := range allPrefixes {
		t.Logf("Verifying final state for prefix %s", prefix)
		successCount := 0

		// Check every 2000th key
		for j := 0; j < entryCount; j += 2000 {
			key := []byte(fmt.Sprintf("%s%07d", prefix, j))

			var value []byte
			err := db.Update(func(tx *Txn) error {
				var err error
				value, err = tx.Get(key)
				if err != nil {
					// Key might have been deleted, that's OK
					return nil
				}
				return nil
			})

			if err != nil {
				t.Errorf("Error during final verification: %v", err)
				continue
			}

			if value != nil {
				successCount++
			}
		}

		t.Logf("  Successfully retrieved %d sample keys for prefix %s", successCount, prefix)
	}

	// Optional: Analyze the database directory structure
	t.Log("Analyzing final database state")

	// Check each level directory
	for i := 1; i <= db.opts.LevelCount; i++ {
		levelPath := fmt.Sprintf("%s%sl%d", db.opts.Directory, string(os.PathSeparator), i)
		files, err := os.ReadDir(levelPath)
		if err != nil {
			t.Logf("Could not read level directory %s: %v", levelPath, err)
			continue
		}

		// Count SSTable files in this level
		sstCount := 0
		for _, file := range files {
			if !file.IsDir() && strings.HasPrefix(file.Name(), SSTablePrefix) {
				sstCount++
			}
		}

		t.Logf("  Level %d contains approximately %d SSTable files", i, sstCount/2) // Divide by 2 because each SSTable has klog and vlog
	}

	t.Log("Compaction test completed successfully")
}

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
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Tests opening a brand new instance.  Will setup an initial WAL and memory table and disk levels.
func TestOpen(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("testdb")

	}()

	// Create a log channel
	logChannel := make(chan string, 100) // Buffer size of 100 messages

	opts := &Options{
		Directory:  "testdb",
		LogChannel: logChannel,
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

	// Open or create the database
	db, err := Open(opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Verify all l1 to l6 directories exist
	for i := 1; i <= 6; i++ {
		dir := fmt.Sprintf("%s/l%d", opts.Directory, i)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Directory %s does not exist", dir)
		}
	}

	_ = db.Close()

	wg.Wait()
}

func TestDB_RecoveryAfterUncleanShutdown(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// First instance - write data but don't close cleanly
	{
		opts := &Options{
			Directory:  dir,
			SyncOption: SyncFull,
		}

		db, err := Open(opts)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		// Write some committed data
		for i := 0; i < 100; i++ {
			err = db.Update(func(txn *Txn) error {
				key := fmt.Sprintf("recovery_key_%d", i)
				value := fmt.Sprintf("recovery_value_%d", i)
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
		}

		// Start some uncommitted transactions
		txn1, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		_ = txn1.Put([]byte("uncommitted_1"), []byte("should_not_survive"))

		txn2, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		_ = txn2.Put([]byte("uncommitted_2"), []byte("should_not_survive"))

		// Simulate unclean shutdown by NOT calling Close()
		// (database just goes out of scope)
	}

	// Second instance - reopen and verify recovery
	{
		opts := &Options{
			Directory:  dir,
			SyncOption: SyncFull,
		}

		db, err := Open(opts)
		if err != nil {
			t.Fatalf("Failed to reopen database: %v", err)
		}
		defer func(db *DB) {
			_ = db.Close()
		}(db)

		// Verify committed data survived
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("recovery_key_%d", i)
			expectedValue := fmt.Sprintf("recovery_value_%d", i)

			var actualValue []byte
			err = db.View(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get([]byte(key))
				return err
			})

			if err != nil {
				t.Errorf("Failed to read committed key %s: %v", key, err)
			} else if string(actualValue) != expectedValue {
				t.Errorf("Value mismatch for key %s: expected %s, got %s",
					key, expectedValue, actualValue)
			}
		}

		// Verify uncommitted data did NOT survive
		err = db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("uncommitted_1"))
			if err == nil {
				return fmt.Errorf("uncommitted data should not have survived")
			}
			return nil
		})
		if err != nil {
			t.Errorf("Uncommitted transaction cleanup failed: %v", err)
		}
	}
}

func TestDB_Stats(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_stats_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	opts := &Options{
		Directory:   dir,
		SyncOption:  SyncNone,
		BloomFilter: true,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Test stats on empty database
	stats := db.Stats()
	if stats == "" {
		t.Errorf("Expected non-empty stats string")
	}

	// Check that stats contain expected sections
	expectedSections := []string{
		"Wildcat DB Stats and Configuration",
		"ID Generator State",
		"Runtime Statistics",
		"Write Buffer Size",
		"Total Entries",
	}

	for _, section := range expectedSections {
		if !strings.Contains(stats, section) {
			t.Errorf("Stats missing expected section: %s", section)
		}
	}

	// Add some data and verify stats change
	for i := 0; i < 50; i++ {
		err = db.Update(func(txn *Txn) error {
			key := fmt.Sprintf("stats_key_%d", i)
			value := fmt.Sprintf("stats_value_%d", i)
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	newStats := db.Stats()
	if newStats == stats {
		t.Errorf("Expected stats to change after inserting data")
	}

	t.Logf("Database stats:\n%s", newStats)
}

func TestDB_ErrorConditions(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_error_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Test operations on nil keys/values
	err = db.Update(func(txn *Txn) error {
		return txn.Put(nil, []byte("value"))
	})
	if err == nil {
		t.Errorf("Expected error putting nil key")
	}

	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("key"), nil)
	})
	if err == nil {
		t.Errorf("Expected error putting nil value")
	}

	// Test reading non-existent key
	err = db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("non_existent_key"))
		if err == nil {
			return fmt.Errorf("expected error reading non-existent key")
		}
		return nil
	})
	if err != nil {
		t.Errorf("Non-existent key test failed: %v", err)
	}

	// Test empty key
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte(""), []byte("empty_key_value"))
	})
	if err == nil {
		t.Errorf("Expected error putting empty key")
	}

	err = db.View(func(txn *Txn) error {
		var err error
		_, err = txn.Get([]byte(""))
		return err
	})
	if err == nil {
		t.Errorf("Expected error reading empty key")

	}

}

// **These are more internal benchmarks than actual database benchmarks.  They are included for completeness and work on optimizations**

func BenchmarkSinglePut(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_single_put")
	}()

	opts := &Options{
		Directory:  "benchdb_single_put",
		SyncOption: SyncNone, // Fastest for benchmarking
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	key := []byte("benchmark_key")
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkBatchPut(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_batch_put")
	}()

	opts := &Options{
		Directory:  "benchdb_batch_put",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	batchSize := 100
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(txn *Txn) error {
			for j := 0; j < batchSize; j++ {
				key := []byte(fmt.Sprintf("key_%d_%d", i, j))
				if err := txn.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("Batch put failed: %v", err)
		}
	}
}

func BenchmarkRandomWrites(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_random_writes")
	}()

	opts := &Options{
		Directory:  "benchdb_random_writes",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	rand.Seed(time.Now().UnixNano())
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNum := rand.Intn(10000) // Random key from 0-9999
		key := []byte(fmt.Sprintf("random_key_%d", keyNum))

		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Random write failed: %v", err)
		}
	}
}

func BenchmarkRandomReads(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_random_reads")
	}()

	opts := &Options{
		Directory:  "benchdb_random_reads",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Pre-populate with 10k keys
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("random_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}
		// Flush every 5000 keys
		if i%5000 == 0 {
			_ = db.ForceFlush()

		}
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNum := rand.Intn(10000)
		key := []byte(fmt.Sprintf("random_key_%d", keyNum))

		err := db.View(func(txn *Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if err != nil {
			b.Fatalf("Random read failed: %v", err)
		}
	}
}

func BenchmarkRandomReads_Bloom(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_random_reads")
	}()

	opts := &Options{
		Directory:   "benchdb_random_reads",
		SyncOption:  SyncNone,
		BloomFilter: true,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Pre-populate with 10k keys
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("random_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}

		// Flush every 5000 keys
		if i%5000 == 0 {
			_ = db.ForceFlush()

		}
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyNum := rand.Intn(10000)
		key := []byte(fmt.Sprintf("random_key_%d", keyNum))

		err := db.View(func(txn *Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if err != nil {
			b.Fatalf("Random read failed: %v", err)
		}
	}
}

func BenchmarkConcurrentWrites(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_concurrent_writes")
	}()

	opts := &Options{
		Directory:  "benchdb_concurrent_writes",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	goroutines := 10

	b.ResetTimer()

	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			<-start // Wait for signal to start

			opsPerGoroutine := b.N / goroutines
			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("concurrent_key_%d_%d", goroutineID, i))
				err := db.Update(func(txn *Txn) error {
					return txn.Put(key, value)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
					return
				}
			}
		}(g)
	}

	close(start) // Signal all goroutines to start
	wg.Wait()
}

func BenchmarkConcurrentReads(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_concurrent_reads")
	}()

	opts := &Options{
		Directory:  "benchdb_concurrent_reads",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Pre-populate with data
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("read_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}

		// Flush every 5000 keys
		if i%5000 == 0 {
			_ = db.ForceFlush()

		}
	}

	goroutines := 10
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()

	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // Wait for signal to start

			opsPerGoroutine := b.N / goroutines
			for i := 0; i < opsPerGoroutine; i++ {
				keyNum := rand.Intn(1000)
				key := []byte(fmt.Sprintf("read_key_%d", keyNum))
				err := db.View(func(txn *Txn) error {
					_, err := txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
					return
				}
			}
		}()
	}

	close(start) // Signal all goroutines to start
	wg.Wait()
}

func BenchmarkConcurrentReads_Bloom(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_concurrent_reads")
	}()

	opts := &Options{
		Directory:   "benchdb_concurrent_reads",
		SyncOption:  SyncNone,
		BloomFilter: true,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Pre-populate with data
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("read_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}

		// Flush every 5000 keys
		if i%5000 == 0 {
			_ = db.ForceFlush()

		}
	}

	goroutines := 10
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()

	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // Wait for signal to start

			opsPerGoroutine := b.N / goroutines
			for i := 0; i < opsPerGoroutine; i++ {
				keyNum := rand.Intn(1000)
				key := []byte(fmt.Sprintf("read_key_%d", keyNum))
				err := db.View(func(txn *Txn) error {
					_, err := txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
					return
				}
			}
		}()
	}

	close(start) // Signal all goroutines to start
	wg.Wait()
}

func BenchmarkMixedWorkload(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_mixed_workload")
	}()

	opts := &Options{
		Directory:  "benchdb_mixed_workload",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Pre-populate with some data
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("mixed_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}

		// Flush every 5000 keys
		if i%5000 == 0 {
			_ = db.ForceFlush()
		}
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.3 { // 30% writes
			key := []byte(fmt.Sprintf("mixed_key_%d", rand.Intn(2000)))
			err := db.Update(func(txn *Txn) error {
				return txn.Put(key, value)
			})
			if err != nil {
				b.Fatalf("Mixed workload write failed: %v", err)
			}
		} else { // 70% reads
			key := []byte(fmt.Sprintf("mixed_key_%d", rand.Intn(1000)))
			err := db.View(func(txn *Txn) error {
				_, err := txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Mixed workload read failed: %v", err)
			}
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_delete")
	}()

	opts := &Options{
		Directory:  "benchdb_delete",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Pre-populate with keys to delete
	value := []byte("benchmark_value_with_some_data_to_make_it_realistic")
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("delete_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Failed to populate: %v", err)
		}
	}

	_ = db.ForceFlush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("delete_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Delete(key)
		})
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

func BenchmarkLargeValues(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb_large_values")
	}()

	opts := &Options{
		Directory:  "benchdb_large_values",
		SyncOption: SyncNone,
	}

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create a 10KB value
	value := make([]byte, 10*1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_key_%d", i))
		err := db.Update(func(txn *Txn) error {
			return txn.Put(key, value)
		})
		if err != nil {
			b.Fatalf("Large value put failed: %v", err)
		}
	}
}

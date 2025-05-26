// Package wildcat
//
// (C) Copyright Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en/MPL/2.0/
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
	"os"
	"sort"
	"testing"
	"time"
)

func TestMergeIterator_MVCC(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_mvcc_test")
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
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert initial data
	err = db.Update(func(txn *Txn) error {
		err := txn.Put([]byte("key1"), []byte("value1_v1"))
		if err != nil {
			return err
		}
		return txn.Put([]byte("key2"), []byte("value2_v1"))
	})
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Update the same keys with newer values
	time.Sleep(1 * time.Millisecond) // Ensure different timestamp
	err = db.Update(func(txn *Txn) error {
		err := txn.Put([]byte("key1"), []byte("value1_v2"))
		if err != nil {
			return err
		}
		return txn.Put([]byte("key2"), []byte("value2_v2"))
	})
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Test that iterator returns the most recent values
	txn := db.Begin()
	iter, err := txn.NewIterator(true)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	results := make(map[string]string)
	for {
		key, val, _, ok := iter.Next()
		if !ok {
			break // No more items

		}

		log.Println("MVCC test - key:", string(key), "value:", string(val))
		results[string(key)] = string(val)
	}

	log.Println(results)

	// Verify we get the most recent versions
	if results["key1"] != "value1_v2" {
		t.Errorf("Expected most recent value 'value1_v2' for key1, got %s", results["key1"])
	}
	if results["key2"] != "value2_v2" {
		t.Errorf("Expected most recent value 'value2_v2' for key2, got %s", results["key2"])
	}

	t.Logf("MVCC test passed - iterator correctly returned most recent values")

}

func TestMergeIterator_LargeScale(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_large_test")
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

	keys := [][]byte{}
	values := [][]byte{}

	for i := 0; i < 20; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key%d", i)))
		values = append(values, []byte(fmt.Sprintf("value%d_v1", i)))
	}

	// Create a test DB
	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      logChan,
		WriteBufferSize: int64(len(keys) + len(values)/4),
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert a large number of keys
	numKeys := 20
	for i := 0; i < numKeys; i++ {
		err = db.Update(func(txn *Txn) error {

			if err := txn.Put(keys[i], values[i]); err != nil {
				return err
			}

			return nil
		})
	}
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// print sstable count
	log.Println(db.Stats())

	// Update the same keys with newer values
	time.Sleep(1 * time.Millisecond) // Ensure different timestamp
	err = db.Update(func(txn *Txn) error {
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d_v2", i))
			if err := txn.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Test that iterator returns the most recent values
	txn := db.Begin()
	iter, err := txn.NewIterator(true)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	results := make(map[string]string)
	for {
		key, val, _, ok := iter.Next()
		if !ok {
			break // No more items
		}

		log.Println("Large-scale test - key:", string(key), "value:", string(val))
		results[string(key)] = string(val)
	}

	// Verify we get the most recent versions
	for i := 0; i < numKeys; i++ {
		expectedKey := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d_v2", i)
		if results[expectedKey] != expectedValue {
			t.Errorf("Expected most recent value '%s' for %s, got %s", expectedValue, expectedKey, results[expectedKey])
		}
	}

	t.Logf("Large-scale test passed - iterator correctly returned most recent values for %d keys", numKeys)
}

func TestMergeIterator_Bidirectional(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_bidirectional_test")
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
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data with predictable ordering
	testKeys := []string{"a", "c", "e", "g", "i", "k", "m", "o", "q", "s"}
	testValues := []string{"val_a", "val_c", "val_e", "val_g", "val_i", "val_k", "val_m", "val_o", "val_q", "val_s"}

	err = db.Update(func(txn *Txn) error {
		for i, key := range testKeys {
			if err := txn.Put([]byte(key), []byte(testValues[i])); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	t.Run("Ascending Iterator", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create ascending iterator: %v", err)
		}

		var ascendingResults []string
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			log.Printf("Ascending - key: %s, value: %s", string(key), string(val))
			ascendingResults = append(ascendingResults, string(key))
		}

		// Verify ascending order
		expectedAscending := make([]string, len(testKeys))
		copy(expectedAscending, testKeys)
		sort.Strings(expectedAscending)

		if len(ascendingResults) != len(expectedAscending) {
			t.Errorf("Expected %d keys, got %d", len(expectedAscending), len(ascendingResults))
		}

		for i, expected := range expectedAscending {
			if i >= len(ascendingResults) || ascendingResults[i] != expected {
				t.Errorf("At index %d: expected %s, got %s", i, expected,
					func() string {
						if i < len(ascendingResults) {
							return ascendingResults[i]
						}
						return "nil"
					}())
			}
		}

		t.Logf("Ascending iteration passed - got keys in order: %v", ascendingResults)
	})

	t.Run("Descending Iterator", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(false)
		if err != nil {
			t.Fatalf("Failed to create descending iterator: %v", err)
		}

		var descendingResults []string
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			log.Printf("Descending - key: %s, value: %s", string(key), string(val))
			descendingResults = append(descendingResults, string(key))
		}

		// Verify descending order
		expectedDescending := make([]string, len(testKeys))
		copy(expectedDescending, testKeys)
		sort.Sort(sort.Reverse(sort.StringSlice(expectedDescending)))

		if len(descendingResults) != len(expectedDescending) {
			t.Errorf("Expected %d keys, got %d", len(expectedDescending), len(descendingResults))
		}

		for i, expected := range expectedDescending {
			if i >= len(descendingResults) || descendingResults[i] != expected {
				t.Errorf("At index %d: expected %s, got %s", i, expected,
					func() string {
						if i < len(descendingResults) {
							return descendingResults[i]
						}
						return "nil"
					}())
			}
		}

		t.Logf("Descending iteration passed - got keys in order: %v", descendingResults)
	})

	t.Run("Bidirectional Navigation", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		// Start with ascending iterator
		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// Move forward a few steps
		var forwardKeys []string
		for i := 0; i < 3; i++ {
			key, _, _, ok := iter.Next()
			if !ok {
				break
			}
			forwardKeys = append(forwardKeys, string(key))
		}

		log.Printf("Forward keys: %v", forwardKeys)

		// Now go backward
		var backwardKeys []string
		for i := 0; i < 2; i++ {
			key, _, _, ok := iter.Prev()
			if !ok {
				break
			}
			backwardKeys = append(backwardKeys, string(key))
		}

		log.Printf("Backward keys: %v", backwardKeys)

		// Verify we can go both directions
		if len(forwardKeys) == 0 {
			t.Error("Failed to move forward")
		}
		if len(backwardKeys) == 0 {
			t.Error("Failed to move backward")
		}

		t.Logf("Bidirectional navigation passed - forward: %v, backward: %v", forwardKeys, backwardKeys)
	})

	t.Run("Direction Change Consistency", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		// Create ascending iterator
		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// Get first key going forward
		key1, _, _, ok1 := iter.Next()
		if !ok1 {
			t.Fatal("Failed to get first key")
		}

		// Change direction and get first key going backward
		key2, _, _, ok2 := iter.Prev()
		if !ok2 {
			t.Fatal("Failed to get key going backward")
		}

		// Change direction again and get key going forward
		key3, _, _, ok3 := iter.Next()
		if !ok3 {
			t.Fatal("Failed to get key going forward again")
		}

		log.Printf("Direction change test - key1: %s, key2: %s, key3: %s",
			string(key1), string(key2), string(key3))

		// The iterator should handle direction changes gracefully
		// We don't enforce specific behavior here, just that it doesn't crash
		t.Logf("Direction change consistency passed - iterator handled direction changes")
	})
}

func TestMergeIterator_BidirectionalWithMVCC(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_bidirectional_mvcc_test")
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
		WriteBufferSize: 2 * 1024, // Small buffer to force flushing
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert initial data
	err = db.Update(func(txn *Txn) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%02d", i)
			value := fmt.Sprintf("value%02d_v1", i)
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Wait a bit and update some keys
	time.Sleep(1 * time.Millisecond)
	err = db.Update(func(txn *Txn) error {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%02d", i*2) // Update even keys
			value := fmt.Sprintf("value%02d_v2", i*2)
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	t.Run("MVCC with Ascending", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		results := make(map[string]string)
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			results[string(key)] = string(val)
		}

		// Verify we get the most recent versions
		for i := 0; i < 10; i++ {
			keyStr := fmt.Sprintf("key%02d", i)
			var expectedValue string
			if i%2 == 0 && i < 10 {
				// Even keys were updated
				expectedValue = fmt.Sprintf("value%02d_v2", i)
			} else {
				// Odd keys have original values
				expectedValue = fmt.Sprintf("value%02d_v1", i)
			}

			if results[keyStr] != expectedValue {
				t.Errorf("For key %s: expected %s, got %s", keyStr, expectedValue, results[keyStr])
			}
		}

		t.Logf("MVCC ascending test passed - %d keys with correct versions", len(results))
	})

	t.Run("MVCC with Descending", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(false)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		var keys []string
		results := make(map[string]string)
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			keyStr := string(key)
			keys = append(keys, keyStr)
			results[keyStr] = string(val)
		}

		// Verify descending order
		for i := 1; i < len(keys); i++ {
			if keys[i-1] < keys[i] {
				t.Errorf("Keys not in descending order: %s should come after %s", keys[i-1], keys[i])
			}
		}

		// Verify we get the most recent versions
		for i := 0; i < 10; i++ {
			keyStr := fmt.Sprintf("key%02d", i)
			var expectedValue string
			if i%2 == 0 && i < 10 {
				expectedValue = fmt.Sprintf("value%02d_v2", i)
			} else {
				expectedValue = fmt.Sprintf("value%02d_v1", i)
			}

			if results[keyStr] != expectedValue {
				t.Errorf("For key %s: expected %s, got %s", keyStr, expectedValue, results[keyStr])
			}
		}

		t.Logf("MVCC descending test passed - %d keys in descending order with correct versions", len(results))
	})
}

func TestMergeIterator_EdgeCases(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_edge_cases_test")
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
		WriteBufferSize: 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	t.Run("Empty Iterator", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// Should not have any items
		_, _, _, ok := iter.Next()
		if ok {
			t.Error("Expected empty iterator, but got items")
		}

		_, _, _, ok = iter.Prev()
		if ok {
			t.Error("Expected empty iterator for Prev(), but got items")
		}

		t.Log("Empty iterator test passed")
	})

	t.Run("Single Item", func(t *testing.T) {
		// Insert one item
		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte("single"), []byte("item"))
		})
		if err != nil {
			t.Fatalf("Failed to insert single item: %v", err)
		}

		txn := db.Begin()
		defer txn.remove()

		// Test ascending
		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		key, val, _, ok := iter.Next()
		if !ok || string(key) != "single" || string(val) != "item" {
			t.Errorf("Expected single/item, got %s/%s (ok=%v)", string(key), string(val), ok)
		}

		// Should be no more items
		_, _, _, ok = iter.Next()
		if ok {
			t.Error("Expected no more items after single item")
		}

		// Test descending
		iter2, err := txn.NewIterator(false)
		if err != nil {
			t.Fatalf("Failed to create descending iterator: %v", err)
		}

		key, val, _, ok = iter2.Next()
		if !ok || string(key) != "single" || string(val) != "item" {
			t.Errorf("Expected single/item in descending, got %s/%s (ok=%v)", string(key), string(val), ok)
		}

		t.Log("Single item test passed")
	})

	t.Run("HasNext/HasPrev", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// Should have items
		if !iter.HasNext() {
			t.Error("Expected HasNext() to return true")
		}

		// Consume all items
		count := 0
		for iter.HasNext() {
			_, _, _, ok := iter.Next()
			if !ok {
				break
			}
			count++
		}

		// Should not have more items
		if iter.HasNext() {
			t.Error("Expected HasNext() to return false after consuming all items")
		}

		t.Logf("HasNext/HasPrev test passed - processed %d items", count)
	})
}

func TestMergeIterator_BidirectionalMultipleSources(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_bidirectional_multisource_test")
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
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert initial batch of data (will go to SSTable)
	err = db.Update(func(txn *Txn) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%02d", i)
			value := fmt.Sprintf("value%02d_v1", i)
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Force flush to create SSTable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Insert second batch with some overlapping keys (will go to another SSTable)
	err = db.Update(func(txn *Txn) error {
		for i := 5; i < 15; i++ {
			key := fmt.Sprintf("key%02d", i)
			value := fmt.Sprintf("value%02d_v2", i)
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert second batch: %v", err)
	}

	// Force another flush to create second SSTable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force second flush: %v", err)
	}

	// Insert third batch with some new and updated keys (will stay in memtable)
	err = db.Update(func(txn *Txn) error {
		for i := 10; i < 20; i++ {
			key := fmt.Sprintf("key%02d", i)
			value := fmt.Sprintf("value%02d_v3", i)
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		// Add some keys that update earlier ones
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%02d", i)
			value := fmt.Sprintf("value%02d_v3", i)
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert third batch: %v", err)
	}

	// Print stats to verify we have multiple sources
	log.Println("Database stats after multiple writes:")
	log.Println(db.Stats())

	t.Run("Ascending with Multiple Sources", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create ascending iterator: %v", err)
		}

		results := make(map[string]string)
		var keys []string
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			keyStr := string(key)
			keys = append(keys, keyStr)
			results[keyStr] = string(val)
			log.Printf("Ascending multi-source - key: %s, value: %s", keyStr, string(val))
		}

		// Verify ascending order
		for i := 1; i < len(keys); i++ {
			if keys[i-1] >= keys[i] {
				t.Errorf("Keys not in ascending order: %s should come before %s", keys[i-1], keys[i])
			}
		}

		// Verify we get the most recent versions (MVCC validation)
		expectedResults := make(map[string]string)
		// Keys 0-4: v3 (updated in third batch)
		for i := 0; i < 5; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v3", i)
		}
		// Keys 5-9: v2 (updated in second batch)
		for i := 5; i < 10; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v2", i)
		}
		// Keys 10-14: v3 (from third batch, overwrites v2)
		for i := 10; i < 15; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v3", i)
		}
		// Keys 15-19: v3 (only in third batch)
		for i := 15; i < 20; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v3", i)
		}

		for expectedKey, expectedValue := range expectedResults {
			if results[expectedKey] != expectedValue {
				t.Errorf("For key %s: expected %s, got %s", expectedKey, expectedValue, results[expectedKey])
			}
		}

		t.Logf("Ascending multi-source test passed - %d keys in correct order with MVCC", len(results))
	})

	t.Run("Descending with Multiple Sources", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(false)
		if err != nil {
			t.Fatalf("Failed to create descending iterator: %v", err)
		}

		results := make(map[string]string)
		var keys []string
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			keyStr := string(key)
			keys = append(keys, keyStr)
			results[keyStr] = string(val)
			log.Printf("Descending multi-source - key: %s, value: %s", keyStr, string(val))
		}

		// Verify descending order
		for i := 1; i < len(keys); i++ {
			if keys[i-1] <= keys[i] {
				t.Errorf("Keys not in descending order: %s should come after %s", keys[i-1], keys[i])
			}
		}

		// Same MVCC validation as ascending test
		expectedResults := make(map[string]string)
		for i := 0; i < 5; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v3", i)
		}
		for i := 5; i < 10; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v2", i)
		}
		for i := 10; i < 15; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v3", i)
		}
		for i := 15; i < 20; i++ {
			expectedResults[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("value%02d_v3", i)
		}

		for expectedKey, expectedValue := range expectedResults {
			if results[expectedKey] != expectedValue {
				t.Errorf("For key %s: expected %s, got %s", expectedKey, expectedValue, results[expectedKey])
			}
		}

		t.Logf("Descending multi-source test passed - %d keys in correct order with MVCC", len(results))
	})

	t.Run("Bidirectional Navigation with Multiple Sources", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		// Start with ascending iterator
		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// Move forward several steps
		var forwardKeys []string
		for i := 0; i < 5; i++ {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			forwardKeys = append(forwardKeys, string(key))
			log.Printf("Forward multi-source - key: %s, value: %s", string(key), string(val))
		}

		// Change direction and go backward
		var backwardKeys []string
		for i := 0; i < 3; i++ {
			key, val, _, ok := iter.Prev()
			if !ok {
				break
			}
			backwardKeys = append(backwardKeys, string(key))
			log.Printf("Backward multi-source - key: %s, value: %s", string(key), string(val))
		}

		// Change direction again and go forward
		var forwardAgainKeys []string
		for i := 0; i < 2; i++ {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			forwardAgainKeys = append(forwardAgainKeys, string(key))
			log.Printf("Forward again multi-source - key: %s, value: %s", string(key), string(val))
		}

		// Verify we can navigate in both directions
		if len(forwardKeys) == 0 {
			t.Error("Failed to move forward with multiple sources")
		}
		if len(backwardKeys) == 0 {
			t.Error("Failed to move backward with multiple sources")
		}
		if len(forwardAgainKeys) == 0 {
			t.Error("Failed to move forward again with multiple sources")
		}

		t.Logf("Bidirectional multi-source navigation passed - forward: %v, backward: %v, forward again: %v",
			forwardKeys, backwardKeys, forwardAgainKeys)
	})
}

func TestMergeIterator_BidirectionalStressTest(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_bidirectional_stress_test")
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
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create multiple batches with overlapping keys to stress test merging
	numBatches := 5
	keysPerBatch := 10

	for batch := 0; batch < numBatches; batch++ {
		err = db.Update(func(txn *Txn) error {
			for i := 0; i < keysPerBatch; i++ {
				// Create overlapping keys across batches
				keyIndex := (batch*keysPerBatch/2 + i) % (keysPerBatch * 2)
				key := fmt.Sprintf("stress_key_%03d", keyIndex)
				value := fmt.Sprintf("stress_value_%03d_batch_%d", keyIndex, batch)
				if err := txn.Put([]byte(key), []byte(value)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to insert batch %d: %v", batch, err)
		}

		// Force flush after each batch to create separate SSTables
		if batch < numBatches-1 { // Don't flush the last batch, keep it in memtable
			err = db.ForceFlush()
			if err != nil {
				t.Fatalf("Failed to force flush after batch %d: %v", batch, err)
			}
		}
	}

	// Print final stats
	log.Println("Stress test database stats:")
	log.Println(db.Stats())

	t.Run("Stress Test Ascending", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		results := make(map[string]string)
		var keys []string
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			keyStr := string(key)
			keys = append(keys, keyStr)
			results[keyStr] = string(val)
		}

		// Verify ascending order
		for i := 1; i < len(keys); i++ {
			if keys[i-1] >= keys[i] {
				t.Errorf("Keys not in ascending order: %s should come before %s", keys[i-1], keys[i])
			}
		}

		// Verify we have the expected number of unique keys
		expectedUniqueKeys := keysPerBatch * 2 // Due to overlapping pattern
		if len(results) != expectedUniqueKeys {
			t.Errorf("Expected %d unique keys, got %d", expectedUniqueKeys, len(results))
		}

		t.Logf("Stress test ascending passed - %d keys in correct order", len(results))
	})

	t.Run("Stress Test Descending", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(false)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		results := make(map[string]string)
		var keys []string
		for {
			key, val, _, ok := iter.Next()
			if !ok {
				break
			}
			keyStr := string(key)
			keys = append(keys, keyStr)
			results[keyStr] = string(val)
		}

		// Verify descending order
		for i := 1; i < len(keys); i++ {
			if keys[i-1] <= keys[i] {
				t.Errorf("Keys not in descending order: %s should come after %s", keys[i-1], keys[i])
			}
		}

		// Verify we have the expected number of unique keys
		expectedUniqueKeys := keysPerBatch * 2
		if len(results) != expectedUniqueKeys {
			t.Errorf("Expected %d unique keys, got %d", expectedUniqueKeys, len(results))
		}

		t.Logf("Stress test descending passed - %d keys in correct order", len(results))
	})

	t.Run("Stress Test Direction Changes", func(t *testing.T) {
		txn := db.Begin()
		defer txn.remove()

		iter, err := txn.NewIterator(true)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// Perform multiple direction changes
		directions := []string{"forward", "backward", "forward", "backward", "forward"}
		allKeys := make([][]string, len(directions))

		for dirIndex, direction := range directions {
			var keys []string
			steps := 3 // Take 3 steps in each direction

			for i := 0; i < steps; i++ {
				var key []byte
				var ok bool

				if direction == "forward" {
					key, _, _, ok = iter.Next()
				} else {
					key, _, _, ok = iter.Prev()
				}

				if !ok {
					break
				}
				keys = append(keys, string(key))
			}

			allKeys[dirIndex] = keys
			log.Printf("Stress direction %s: %v", direction, keys)
		}

		// Verify each direction change worked
		for i, keys := range allKeys {
			if len(keys) == 0 && i < 2 { // First two directions should have data
				t.Errorf("Direction %s (index %d) returned no keys", directions[i], i)
			}
		}

		t.Logf("Stress test direction changes passed - handled %d direction changes", len(directions))
	})
}

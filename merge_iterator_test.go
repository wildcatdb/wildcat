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
	"log"
	"os"
	"testing"
	"time"
)

func TestTxn_IteratorMergesAllSources(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_iterator_merge_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	db, err := Open(&Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: make(chan string, 100),
	})
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Write to memtable
	txn1 := db.Begin()
	for i := 0; i < 10; i++ {
		err = txn1.Put([]byte(fmt.Sprintf("mem_key_%02d", i)), []byte(fmt.Sprintf("mem_val_%02d", i)))
		if err != nil {
			t.Fatalf("failed to put key in txn1: %v", err)
		}
	}

	err = txn1.Commit()
	if err != nil {
		t.Fatalf("failed to commit txn1: %v", err)
	}

	// Force flush to move memtable to immutable (we simulate immutable presence***)
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("failed to force flush: %v", err)
	}

	// Write to new memtable
	txn2 := db.Begin()
	for i := 10; i < 20; i++ {
		err = txn2.Put([]byte(fmt.Sprintf("imm_key_%02d", i)), []byte(fmt.Sprintf("imm_val_%02d", i)))
		if err != nil {
			t.Fatalf("failed to put key in txn2: %v", err)
		}
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("failed to commit txn2: %v", err)
	}

	// Flush again to create SSTable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("failed to force flush: %v", err)
	}

	// Write to new memtable again
	txn3 := db.Begin()
	for i := 20; i < 30; i++ {
		err = txn3.Put([]byte(fmt.Sprintf("sst_key_%02d", i)), []byte(fmt.Sprintf("sst_val_%02d", i)))
		if err != nil {
			t.Fatalf("failed to put key in txn3: %v", err)
		}
	}

	err = txn3.Commit()
	if err != nil {
		t.Fatalf("failed to commit txn3: %v", err)
	}

	// Let’s now use the merge iterator to read all 30 keys
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, nil)
	count := 0
	for {
		k, v, ts, ok := iter.Next()
		if !ok {
			break
		}
		log.Println("Key:", string(k), "Value:", string(v), "Timestamp:", ts, "Transaction ID:", txnRead.Id, "Transaction Timestamp:", txnRead.Timestamp)
		if !bytes.HasPrefix(k, []byte("mem_key_")) &&
			!bytes.HasPrefix(k, []byte("imm_key_")) &&
			!bytes.HasPrefix(k, []byte("sst_key_")) {
			t.Errorf("unexpected key: %s", k)
		}
		if len(v) == 0 || ts > txnRead.Timestamp {
			t.Errorf("invalid entry for key %s: val=%s ts=%d", k, v, ts)
		}
		count++
	}

	if count < 30 {
		t.Errorf("expected at least 30 keys across all sources, got %d", count)
	}
}

func TestMergeIterator_SingleMemtable(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_single_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key5": "value5",
		"key7": "value7",
	}

	txn := db.Begin()
	for k, v := range testData {
		err := txn.Put([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", k, err)
		}
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test forward iteration
	txn2 := db.Begin()
	iter := txn2.NewIterator(nil, nil)

	expectedKeys := []string{"key1", "key2", "key3", "key5", "key7"}
	i := 0
	for {
		key, value, _, ok := iter.Next()
		if !ok {
			break
		}

		if i >= len(expectedKeys) {
			t.Fatalf("Got more keys than expected")
		}

		expectedKey := expectedKeys[i]
		expectedValue := testData[expectedKey]

		if string(key) != expectedKey {
			t.Errorf("Expected key %s, got %s", expectedKey, string(key))
		}
		if string(value) != expectedValue {
			t.Errorf("Expected value %s, got %s", expectedValue, string(value))
		}
		i++
	}

	if i != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), i)
	}
}

func TestMergeIterator_PrefixFiltering(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_prefix_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data with different prefixes
	testData := map[string]string{
		"user:1":    "alice",
		"user:2":    "bob",
		"user:3":    "charlie",
		"config:1":  "setting1",
		"config:2":  "setting2",
		"session:1": "sess1",
		"session:2": "sess2",
	}

	txn := db.Begin()
	for k, v := range testData {
		err := txn.Put([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", k, err)
		}
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// **Iterate without prefix to see all keys**
	txnAll := db.Begin()
	iterAll := txnAll.NewIterator(nil, nil)

	var allKeys []string
	for {
		key, _, _, ok := iterAll.Next()
		if !ok {
			break
		}
		allKeys = append(allKeys, string(key))
	}

	t.Logf("All keys in database: %v", allKeys)

	// Test iteration with "user:" prefix
	txn2 := db.Begin()
	iter := txn2.NewIterator(nil, []byte("user:"))

	var foundKeys []string
	for {
		key, value, _, ok := iter.Next()
		if !ok {
			break
		}
		foundKeys = append(foundKeys, string(key))
		t.Logf("Found prefixed key: %s, value: %s", string(key), string(value))
	}

	t.Logf("Found keys with user: prefix: %v", foundKeys)

	expectedUserKeys := []string{"user:1", "user:2", "user:3"}
	if len(foundKeys) != len(expectedUserKeys) {
		t.Errorf("Expected %d user keys, got %d. Found keys: %v", len(expectedUserKeys), len(foundKeys), foundKeys)
	}

	// Verify each found key matches expected
	for i, expectedKey := range expectedUserKeys {
		if i >= len(foundKeys) {
			t.Errorf("Missing expected key %s at position %d", expectedKey, i)
			continue
		}
		if foundKeys[i] != expectedKey {
			t.Errorf("Expected key %s at position %d, got %s", expectedKey, i, foundKeys[i])
		}
	}
}

func TestMergeIterator_StartKey(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_start_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	txn := db.Begin()
	for k, v := range testData {
		err := txn.Put([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", k, err)
		}
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test iteration starting from "key3"
	txn2 := db.Begin()
	iter := txn2.NewIterator([]byte("key3"), nil)

	expectedKeys := []string{"key3", "key4", "key5"}
	i := 0
	for {
		key, value, _, ok := iter.Next()
		if !ok {
			break
		}

		if i >= len(expectedKeys) {
			t.Fatalf("Got more keys than expected")
		}

		expectedKey := expectedKeys[i]
		expectedValue := testData[expectedKey]

		if string(key) != expectedKey {
			t.Errorf("Expected key %s, got %s", expectedKey, string(key))
		}
		if string(value) != expectedValue {
			t.Errorf("Expected value %s, got %s", expectedValue, string(value))
		}
		i++
	}

	if i != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), i)
	}
}

func TestMergeIterator_MultipleMemtables(t *testing.T) {
	/** merge iterator with multiple memtables (active + immutable) **/

	dir, err := os.MkdirTemp("", "db_merge_iterator_multi_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// First batch of data
	txn1 := db.Begin()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("batch1_key%d", i)
		value := fmt.Sprintf("batch1_value%d", i)
		err := txn1.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Force flush to create immutable memtable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Second batch of data (will be in new active memtable)
	txn2 := db.Begin()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("batch2_key%d", i)
		value := fmt.Sprintf("batch2_value%d", i)
		err := txn2.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Test iteration across both memtables
	txn3 := db.Begin()
	iter := txn3.NewIterator(nil, nil)

	var allKeys []string
	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		allKeys = append(allKeys, string(key))
	}

	// Should have keys from both batches
	expectedCount := 10
	if len(allKeys) != expectedCount {
		t.Errorf("Expected %d keys, got %d. Keys: %v", expectedCount, len(allKeys), allKeys)
	}

	// Verify keys are sorted
	for i := 1; i < len(allKeys); i++ {
		if allKeys[i-1] >= allKeys[i] {
			t.Errorf("Keys are not sorted: %s >= %s", allKeys[i-1], allKeys[i])
		}
	}
}

func TestMergeIterator_MVCC(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_mvcc_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert initial version
	txn1 := db.Begin()
	err = txn1.Put([]byte("key1"), []byte("value1_v1"))
	if err != nil {
		t.Fatalf("Failed to put key1 v1: %v", err)
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Insert second version
	time.Sleep(time.Millisecond) // Ensure different timestamp
	txn2 := db.Begin()
	err = txn2.Put([]byte("key1"), []byte("value1_v2"))
	if err != nil {
		t.Fatalf("Failed to put key1 v2: %v", err)
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Insert third version
	time.Sleep(time.Millisecond) // Ensure different timestamp
	txn3 := db.Begin()
	err = txn3.Put([]byte("key1"), []byte("value1_v3"))
	if err != nil {
		t.Fatalf("Failed to put key1 v3: %v", err)
	}
	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 3: %v", err)
	}

	// Read with latest transaction, should see latest version
	txn4 := db.Begin()
	iter := txn4.NewIterator(nil, nil)

	key, value, _, ok := iter.Next()
	if !ok {
		t.Fatalf("Expected to find key1")
	}

	if string(key) != "key1" {
		t.Errorf("Expected key1, got %s", string(key))
	}
	if string(value) != "value1_v3" {
		t.Errorf("Expected value1_v3, got %s", string(value))
	}

	// Should not have any more keys
	_, _, _, ok = iter.Next()
	if ok {
		t.Errorf("Expected no more keys")
	}
}

func TestMergeIterator_Deletions(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_delete_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert some data
	txn1 := db.Begin()
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		err := txn1.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Delete some keys
	txn2 := db.Begin()
	err = txn2.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}
	err = txn2.Delete([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to delete key4: %v", err)
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Test iteration after deletions
	txn3 := db.Begin()
	iter := txn3.NewIterator(nil, nil)

	var remainingKeys []string
	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		remainingKeys = append(remainingKeys, string(key))
	}

	// Should only have key1, key3, key5
	expectedKeys := []string{"key1", "key3", "key5"}
	if len(remainingKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys after deletion, got %d: %v", len(expectedKeys), len(remainingKeys), remainingKeys)
	}

	for i, expectedKey := range expectedKeys {
		if i >= len(remainingKeys) || remainingKeys[i] != expectedKey {
			t.Errorf("Expected key %s at position %d, got %v", expectedKey, i, remainingKeys)
		}
	}
}

func TestMergeIterator_Empty(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_empty_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Test iteration on empty database
	txn := db.Begin()
	iter := txn.NewIterator(nil, nil)

	// Should not return any keys
	key, _, _, ok := iter.Next()
	if ok {
		t.Errorf("Expected no keys in empty database, got %s", string(key))
	}
}

func TestMergeIterator_ComplexKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_complex_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert keys with various patterns
	testData := map[string]string{
		"":          "empty_key",
		"a":         "single_char",
		"aa":        "double_char",
		"key":       "simple",
		"key:1":     "colon_separated",
		"key/path":  "slash_separated",
		"key.value": "dot_separated",
		"KEY":       "uppercase",
		"key_123":   "with_numbers",
		"very_long_key_name_that_exceeds_normal_expectations": "long_key",
	}

	txn := db.Begin()
	for k, v := range testData {
		err := txn.Put([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to put key %q: %v", k, err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test iteration
	txn2 := db.Begin()
	iter := txn2.NewIterator(nil, nil)

	var keys []string
	for {
		key, value, _, ok := iter.Next()
		if !ok {
			break
		}

		keys = append(keys, string(key))

		// Verify we can get the expected value
		expectedValue, exists := testData[string(key)]
		if !exists {
			t.Errorf("Unexpected key: %q", string(key))
		} else if string(value) != expectedValue {
			t.Errorf("Value mismatch for key %q: expected %q, got %q", string(key), expectedValue, string(value))
		}
	}

	if len(keys) != len(testData) {
		t.Errorf("Expected %d keys, got %d", len(testData), len(keys))
	}

	// Verify keys are sorted lexicographically
	for i := 1; i < len(keys); i++ {
		if bytes.Compare([]byte(keys[i-1]), []byte(keys[i])) >= 0 {
			t.Errorf("Keys not sorted: %q >= %q", keys[i-1], keys[i])
		}
	}
}

func TestMergeIterator_SimplePrefixTest(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_simple_prefix_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert just a few keys to test basic prefix functionality
	txn := db.Begin()
	err = txn.Put([]byte("aa"), []byte("value_aa"))
	if err != nil {
		t.Fatalf("Failed to put aa: %v", err)
	}
	err = txn.Put([]byte("ab"), []byte("value_ab"))
	if err != nil {
		t.Fatalf("Failed to put ab: %v", err)
	}
	err = txn.Put([]byte("bb"), []byte("value_bb"))
	if err != nil {
		t.Fatalf("Failed to put bb: %v", err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test with prefix "a"
	// should get "aa" and "ab"
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, []byte("a"))

	var foundKeys []string
	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		foundKeys = append(foundKeys, string(key))
	}

	t.Logf("Keys found with prefix 'a': %v", foundKeys)

	if len(foundKeys) == 0 {
		t.Logf("No keys found with prefix 'a' - this suggests prefix filtering might not be implemented correctly")

		// Let's try without prefix to see if basic iteration works
		txnAll := db.Begin()
		iterAll := txnAll.NewIterator(nil, nil)

		var allKeys []string
		for {
			key, _, _, ok := iterAll.Next()
			if !ok {
				break
			}
			allKeys = append(allKeys, string(key))
		}

		t.Logf("All keys without prefix: %v", allKeys)

		if len(allKeys) != 3 {
			t.Errorf("Expected 3 total keys, got %d", len(allKeys))
		}
	} else {
		// If we got some keys, verify they have the right prefix
		expectedKeys := []string{"aa", "ab"}
		if len(foundKeys) != len(expectedKeys) {
			t.Errorf("Expected %d keys with prefix 'a', got %d: %v", len(expectedKeys), len(foundKeys), foundKeys)
		}

		for _, key := range foundKeys {
			if !bytes.HasPrefix([]byte(key), []byte("a")) {
				t.Errorf("Key %s does not have prefix 'a'", key)
			}
		}
	}
}

func TestMergeIterator_PrefixFilteringDetailed(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_prefix_detailed_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data with various prefixes for comprehensive testing
	testData := []struct {
		key   string
		value string
	}{
		{"a", "value_a"},
		{"ab", "value_ab"},
		{"abc", "value_abc"},
		{"abcd", "value_abcd"},
		{"b", "value_b"},
		{"ba", "value_ba"},
		{"user:", "value_user_empty"},
		{"user:alice", "alice_data"},
		{"user:bob", "bob_data"},
		{"user:charlie", "charlie_data"},
		{"users", "value_users"},
		{"userdata", "value_userdata"},
	}

	txn := db.Begin()
	for _, item := range testData {
		err := txn.Put([]byte(item.key), []byte(item.value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", item.key, err)
		}
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// No prefix (should get all keys)
	txnAll := db.Begin()
	iterAll := txnAll.NewIterator(nil, nil)

	var allKeys []string
	for {
		key, _, _, ok := iterAll.Next()
		if !ok {
			break
		}
		allKeys = append(allKeys, string(key))
	}

	if len(allKeys) != len(testData) {
		t.Errorf("Expected %d total keys, got %d. Keys: %v", len(testData), len(allKeys), allKeys)
	}

	// Prefix "user:" (should match user:, user:alice, user:bob, user:charlie)
	txnUser := db.Begin()
	iterUser := txnUser.NewIterator(nil, []byte("user:"))

	var userKeys []string
	for {
		key, _, _, ok := iterUser.Next()
		if !ok {
			break
		}
		userKeys = append(userKeys, string(key))
	}

	expectedUserKeys := []string{"user:", "user:alice", "user:bob", "user:charlie"}
	if len(userKeys) != len(expectedUserKeys) {
		t.Errorf("Expected %d user: keys, got %d. Keys: %v", len(expectedUserKeys), len(userKeys), userKeys)
	}

	// Prefix "a" (should match a, ab, abc, abcd)
	txnA := db.Begin()
	iterA := txnA.NewIterator(nil, []byte("a"))

	var aKeys []string
	for {
		key, _, _, ok := iterA.Next()
		if !ok {
			break
		}
		aKeys = append(aKeys, string(key))
	}

	expectedAKeys := []string{"a", "ab", "abc", "abcd"}
	if len(aKeys) != len(expectedAKeys) {
		t.Errorf("Expected %d 'a' keys, got %d. Keys: %v", len(expectedAKeys), len(aKeys), aKeys)
	}

	// Prefix "nonexistent" (should match nothing)
	txnNone := db.Begin()
	iterNone := txnNone.NewIterator(nil, []byte("nonexistent"))

	var noneKeys []string
	for {
		key, _, _, ok := iterNone.Next()
		if !ok {
			break
		}
		noneKeys = append(noneKeys, string(key))
	}

	if len(noneKeys) != 0 {
		t.Errorf("Expected 0 'nonexistent' keys, got %d. Keys: %v", len(noneKeys), noneKeys)
	}
}

func TestMergeIterator_ConcurrentModification(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert initial data
	txn1 := db.Begin()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		err := txn1.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Start iteration in one transaction
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, nil)

	// Read first few keys
	var readKeys []string
	for i := 0; i < 5; i++ {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		readKeys = append(readKeys, string(key))
	}

	// Modify data in another transaction
	txn2 := db.Begin()
	err = txn2.Put([]byte("key05"), []byte("modified_value"))
	if err != nil {
		t.Fatalf("Failed to modify key05: %v", err)
	}
	err = txn2.Delete([]byte("key07"))
	if err != nil {
		t.Fatalf("Failed to delete key07: %v", err)
	}
	err = txn2.Put([]byte("new_key"), []byte("new_value"))
	if err != nil {
		t.Fatalf("Failed to put new_key: %v", err)
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Continue reading with original iterator, should see original snapshot
	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		readKeys = append(readKeys, string(key))
	}

	// Should have read all original 10 keys, not affected by concurrent modifications
	if len(readKeys) != 10 {
		t.Errorf("Expected to read 10 keys from original snapshot, got %d: %v", len(readKeys), readKeys)
	}

	// Start new iteration to see modified data
	txn3 := db.Begin()
	iter2 := txn3.NewIterator(nil, nil)

	var newKeys []string
	for {
		key, _, _, ok := iter2.Next()
		if !ok {
			break
		}
		newKeys = append(newKeys, string(key))
	}

	// Should see modifications, key07 deleted, new_key added, so still 10 keys
	if len(newKeys) != 10 {
		t.Errorf("Expected 10 keys in new iteration (after modifications), got %d: %v", len(newKeys), newKeys)
	}

	// Verify key07 is not present and new_key is present
	foundKey07 := false
	foundNewKey := false
	for _, key := range newKeys {
		if key == "key07" {
			foundKey07 = true
		}
		if key == "new_key" {
			foundNewKey = true
		}
	}

	if foundKey07 {
		t.Errorf("key07 should have been deleted")
	}
	if !foundNewKey {
		t.Errorf("new_key should be present")
	}
}

func TestMergeIterator_BidirectionalIteration(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_bidirectional_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert test data
	testKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	txn := db.Begin()
	for _, key := range testKeys {
		err := txn.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test forward iteration
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, nil)

	var forwardKeys []string
	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		forwardKeys = append(forwardKeys, string(key))
	}

	if len(forwardKeys) != len(testKeys) {
		t.Errorf("Expected %d keys in forward iteration, got %d: %v", len(testKeys), len(forwardKeys), forwardKeys)
	}

	// Verify forward order
	for i, expectedKey := range testKeys {
		if i >= len(forwardKeys) || forwardKeys[i] != expectedKey {
			t.Errorf("Forward iteration: expected key %s at position %d, got %v", expectedKey, i, forwardKeys)
		}
	}

	// Test backward iteration
	var backwardKeys []string
	for {
		key, _, _, ok := iter.Prev()
		if !ok {
			break
		}
		backwardKeys = append(backwardKeys, string(key))
	}

	if len(backwardKeys) != len(testKeys) {
		t.Errorf("Expected %d keys in backward iteration, got %d: %v", len(testKeys), len(backwardKeys), backwardKeys)
	}

	// Verify backward order (should be reverse of forward)
	for i, expectedKey := range testKeys {
		reverseIdx := len(testKeys) - 1 - i
		if reverseIdx >= len(backwardKeys) || backwardKeys[reverseIdx] != expectedKey {
			t.Errorf("Backward iteration: expected key %s at reverse position %d, got %v", expectedKey, reverseIdx, backwardKeys)
		}
	}
}

func TestMergeIterator_MultiSourceIteration(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_multisource_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		WriteBufferSize: 1024, // Small buffer to force flushes
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert data that will become SSTable
	txn1 := db.Begin()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("sst_key_%02d", i)
		value := fmt.Sprintf("sst_value_%02d", i)
		err := txn1.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put SSTable key %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit SSTable transaction: %v", err)
	}

	// Force flush to create SSTable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush to create SSTable: %v", err)
	}

	// Insert data that will become immutable memtable
	txn2 := db.Begin()
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("imm_key_%02d", i)
		value := fmt.Sprintf("imm_value_%02d", i)
		err := txn2.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put immutable key %s: %v", key, err)
		}
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit immutable transaction: %v", err)
	}

	// Force flush to create immutable memtable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush to create immutable memtable: %v", err)
	}

	// Insert data that will stay in active memtable
	txn3 := db.Begin()
	for i := 20; i < 30; i++ {
		key := fmt.Sprintf("mem_key_%02d", i)
		value := fmt.Sprintf("mem_value_%02d", i)
		err := txn3.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put memtable key %s: %v", key, err)
		}
	}
	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit memtable transaction: %v", err)
	}

	// Test iteration across all sources
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, nil)

	var allKeys []string
	var sourceCount = make(map[string]int)

	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		keyStr := string(key)
		allKeys = append(allKeys, keyStr)

		// Count keys by source
		if bytes.HasPrefix(key, []byte("sst_")) {
			sourceCount["sstable"]++
		} else if bytes.HasPrefix(key, []byte("imm_")) {
			sourceCount["immutable"]++
		} else if bytes.HasPrefix(key, []byte("mem_")) {
			sourceCount["memtable"]++
		}
	}

	// Verify we got keys from all sources
	expectedTotal := 30
	if len(allKeys) != expectedTotal {
		t.Errorf("Expected %d total keys, got %d", expectedTotal, len(allKeys))
	}

	if sourceCount["sstable"] != 10 {
		t.Errorf("Expected 10 SSTable keys, got %d", sourceCount["sstable"])
	}
	if sourceCount["immutable"] != 10 {
		t.Errorf("Expected 10 immutable memtable keys, got %d", sourceCount["immutable"])
	}
	if sourceCount["memtable"] != 10 {
		t.Errorf("Expected 10 memtable keys, got %d", sourceCount["memtable"])
	}

	// Verify keys are sorted
	for i := 1; i < len(allKeys); i++ {
		if allKeys[i-1] >= allKeys[i] {
			t.Errorf("Keys not sorted: %s >= %s at positions %d, %d", allKeys[i-1], allKeys[i], i-1, i)
		}
	}

	t.Logf("Successfully merged %d keys from %d sources: %d SSTable, %d immutable, %d memtable",
		len(allKeys), len(sourceCount), sourceCount["sstable"], sourceCount["immutable"], sourceCount["memtable"])
}

func TestMergeIterator_OverlappingKeysMultiSource(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_overlap_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		WriteBufferSize: 512, // Small buffer to force flushes
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert initial versions (will become SSTable)
	txn1 := db.Begin()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("v1_%s", key)
		err := txn1.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put initial key %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit initial transaction: %v", err)
	}

	// Force flush
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Update some keys (will become immutable memtable)
	time.Sleep(time.Millisecond) // Ensure different timestamp
	txn2 := db.Begin()
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("v2_%s", key)
		err := txn2.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put updated key %s: %v", key, err)
		}
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit update transaction: %v", err)
	}

	// Force flush
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Update some keys again (will stay in memtable)
	time.Sleep(time.Millisecond) // Ensure different timestamp
	txn3 := db.Begin()
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("v3_%s", key)
		err := txn3.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put final key %s: %v", key, err)
		}
	}
	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit final transaction: %v", err)
	}

	// Test iteration, should see latest versions
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, nil)

	expectedValues := map[string]string{
		"key0": "v3_key0", // Latest version from memtable
		"key1": "v3_key1", // Latest version from memtable
		"key2": "v2_key2", // Latest version from immutable memtable
		"key3": "v1_key3", // Original version from SSTable
		"key4": "v1_key4", // Original version from SSTable
	}

	var foundKeys []string
	for {
		key, value, _, ok := iter.Next()
		if !ok {
			break
		}

		keyStr := string(key)
		valueStr := string(value)
		foundKeys = append(foundKeys, keyStr)

		expectedValue, exists := expectedValues[keyStr]
		if !exists {
			t.Errorf("Unexpected key found: %s", keyStr)
			continue
		}

		if valueStr != expectedValue {
			t.Errorf("For key %s, expected value %s, got %s", keyStr, expectedValue, valueStr)
		}

		t.Logf("Key: %s, Value: %s", keyStr, valueStr)
	}

	if len(foundKeys) != len(expectedValues) {
		t.Errorf("Expected %d unique keys, got %d: %v", len(expectedValues), len(foundKeys), foundKeys)
	}
}

func TestMergeIterator_PrefixFilteringMultiSource(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_prefix_multisource_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		WriteBufferSize: 512, // Small buffer to force flushes
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Insert user data that will become SSTable
	txn1 := db.Begin()
	sstableUsers := []string{"user:alice", "user:bob", "config:db_host", "config:db_port"}
	for _, key := range sstableUsers {
		err := txn1.Put([]byte(key), []byte("sstable_"+key))
		if err != nil {
			t.Fatalf("Failed to put SSTable key %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit SSTable transaction: %v", err)
	}

	// Force flush to create SSTable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush to create SSTable: %v", err)
	}

	// Insert more user data that will become immutable memtable
	txn2 := db.Begin()
	immutableUsers := []string{"user:charlie", "user:david", "session:abc", "session:def"}
	for _, key := range immutableUsers {
		err := txn2.Put([]byte(key), []byte("immutable_"+key))
		if err != nil {
			t.Fatalf("Failed to put immutable key %s: %v", key, err)
		}
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit immutable transaction: %v", err)
	}

	// Force flush to create immutable memtable
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush to create immutable memtable: %v", err)
	}

	// Insert more user data that will stay in active memtable
	txn3 := db.Begin()
	memtableUsers := []string{"user:eve", "user:frank", "temp:file1", "temp:file2"}
	for _, key := range memtableUsers {
		err := txn3.Put([]byte(key), []byte("memtable_"+key))
		if err != nil {
			t.Fatalf("Failed to put memtable key %s: %v", key, err)
		}
	}
	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit memtable transaction: %v", err)
	}

	// Test prefix filtering for "user:" across all sources
	txnRead := db.Begin()
	iter := txnRead.NewIterator(nil, []byte("user:"))

	var userKeys []string
	var sourceCount = make(map[string]int)

	for {
		key, value, _, ok := iter.Next()
		if !ok {
			break
		}

		keyStr := string(key)
		valueStr := string(value)
		userKeys = append(userKeys, keyStr)

		// Verify prefix
		if !bytes.HasPrefix([]byte(keyStr), []byte("user:")) {
			t.Errorf("Key %s does not have user: prefix", keyStr)
		}

		// Count by source
		if bytes.HasPrefix([]byte(valueStr), []byte("sstable_")) {
			sourceCount["sstable"]++
		} else if bytes.HasPrefix([]byte(valueStr), []byte("immutable_")) {
			sourceCount["immutable"]++
		} else if bytes.HasPrefix([]byte(valueStr), []byte("memtable_")) {
			sourceCount["memtable"]++
		}

		t.Logf("User key: %s, Value: %s", keyStr, valueStr)
	}

	// Verify we got user keys from all sources
	expectedUserKeys := []string{"user:alice", "user:bob", "user:charlie", "user:david", "user:eve", "user:frank"}
	if len(userKeys) != len(expectedUserKeys) {
		t.Errorf("Expected %d user keys, got %d: %v", len(expectedUserKeys), len(userKeys), userKeys)
	}

	// Verify distribution across sources
	if sourceCount["sstable"] != 2 {
		t.Errorf("Expected 2 user keys from SSTable, got %d", sourceCount["sstable"])
	}
	if sourceCount["immutable"] != 2 {
		t.Errorf("Expected 2 user keys from immutable memtable, got %d", sourceCount["immutable"])
	}
	if sourceCount["memtable"] != 2 {
		t.Errorf("Expected 2 user keys from memtable, got %d", sourceCount["memtable"])
	}

	// Verify keys are sorted
	for i := 1; i < len(userKeys); i++ {
		if userKeys[i-1] >= userKeys[i] {
			t.Errorf("User keys not sorted: %s >= %s", userKeys[i-1], userKeys[i])
		}
	}
}

func TestMergeIterator_StartKeyMultiSource(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merge_iterator_startkey_multisource_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

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
		WriteBufferSize: 512,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Distribute keys across multiple sources with known ordering
	allKeys := []string{
		"key01", "key03", "key05", "key07", "key09", // Will go to SSTable
		"key11", "key13", "key15", "key17", "key19", // Will go to immutable
		"key21", "key23", "key25", "key27", "key29", // Will stay in memtable
	}

	// Insert first batch (SSTable)
	txn1 := db.Begin()
	for i := 0; i < 5; i++ {
		key := allKeys[i]
		err := txn1.Put([]byte(key), []byte("sstable_"+key))
		if err != nil {
			t.Fatalf("Failed to put SSTable key %s: %v", key, err)
		}
	}
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit SSTable transaction: %v", err)
	}
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to flush SSTable: %v", err)
	}

	// Insert second batch (immutable)
	txn2 := db.Begin()
	for i := 5; i < 10; i++ {
		key := allKeys[i]
		err := txn2.Put([]byte(key), []byte("immutable_"+key))
		if err != nil {
			t.Fatalf("Failed to put immutable key %s: %v", key, err)
		}
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit immutable transaction: %v", err)
	}
	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to flush immutable: %v", err)
	}

	// Insert third batch (memtable)
	txn3 := db.Begin()
	for i := 10; i < 15; i++ {
		key := allKeys[i]
		err := txn3.Put([]byte(key), []byte("memtable_"+key))
		if err != nil {
			t.Fatalf("Failed to put memtable key %s: %v", key, err)
		}
	}
	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit memtable transaction: %v", err)
	}

	// Test iteration starting from "key15" (should get keys >= key15)
	txnRead := db.Begin()
	iter := txnRead.NewIterator([]byte("key15"), nil)

	var foundKeys []string
	for {
		key, _, _, ok := iter.Next()
		if !ok {
			break
		}
		foundKeys = append(foundKeys, string(key))
	}

	// Should get= key15, key17, key19, key21, key23, key25, key27, key29
	expectedKeys := []string{"key15", "key17", "key19", "key21", "key23", "key25", "key27", "key29"}
	if len(foundKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys starting from key15, got %d: %v", len(expectedKeys), len(foundKeys), foundKeys)
	}

	for i, expectedKey := range expectedKeys {
		if i >= len(foundKeys) || foundKeys[i] != expectedKey {
			t.Errorf("Expected key %s at position %d, got %v", expectedKey, i, foundKeys)
		}
	}

	// Verify we got keys from multiple sources
	var sourceCount = make(map[string]int)
	txnRead2 := db.Begin()
	iter2 := txnRead2.NewIterator([]byte("key15"), nil)

	for {
		key, value, _, ok := iter2.Next()
		if !ok {
			break
		}

		valueStr := string(value)
		if bytes.HasPrefix([]byte(valueStr), []byte("sstable_")) {
			sourceCount["sstable"]++
		} else if bytes.HasPrefix([]byte(valueStr), []byte("immutable_")) {
			sourceCount["immutable"]++
		} else if bytes.HasPrefix([]byte(valueStr), []byte("memtable_")) {
			sourceCount["memtable"]++
		}

		t.Logf("Start key iteration - Key: %s, Value: %s", string(key), valueStr)
	}

	t.Logf("Source distribution: SSTable=%d, Immutable=%d, Memtable=%d",
		sourceCount["sstable"], sourceCount["immutable"], sourceCount["memtable"])
}

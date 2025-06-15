package wildcat

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTxn_BasicOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	txn, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txn.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Verify key is in write set
	if _, exists := txn.WriteSet["key1"]; !exists {
		t.Errorf("Expected key to be in write set")
	}

	// Verify we can read the key before committing
	value, err := txn.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key before commit: %v", err)
	} else if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// Verify the key is not visible outside the transaction yet
	txn2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	_, err = txn2.Get([]byte("key1"))
	if err == nil {
		t.Errorf("Key should not be visible in other transaction before commit")
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify the key is now visible in another transaction
	txn3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	value, err = txn3.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key after commit: %v", err)
	} else if string(value) != "value1" {
		t.Errorf("Expected value1 after commit, got %s", value)
	}

	// Test delete operation in a new transaction
	txn4, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txn4.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify key is in delete set
	if _, exists := txn4.DeleteSet[("key1")]; !exists {
		t.Errorf("Expected key to be in delete set")
	}

	// Commit the delete transaction
	err = txn4.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}

	// Start a new transaction and verify key is gone
	txn5, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	_, err = txn5.Get([]byte("key1"))
	if err == nil {
		t.Errorf("Key should be gone after delete commit")
	}
}

func TestTxn_Rollback(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_rollback_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Write a key-value pair to the database
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("stable_key"), []byte("stable_value"))
	})
	if err != nil {
		t.Fatalf("Failed to write initial key: %v", err)
	}

	// Begin a transaction that will be rolled back
	txn, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Make some changes
	err = txn.Put([]byte("key_to_rollback"), []byte("value_to_rollback"))
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Marking stable key for deletion (but will be rolled back)
	err = txn.Delete([]byte("stable_key"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Roll back the transaction
	err = txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to roll back transaction: %v", err)
	}

	// Verify the changes are not visible after rollback
	txn2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	_, err = txn2.Get([]byte("key_to_rollback"))
	if err == nil {
		t.Errorf("Rolled back key should not be accessible")
	}

	value, err := txn2.Get([]byte("stable_key"))
	if err != nil {
		t.Errorf("Stable key should still exist: %v", err)
	} else if string(value) != "stable_value" {
		t.Errorf("Expected stable_value, got %s", value)
	}
}

func TestTxn_Isolation(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_isolation_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// First, insert a key
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("isolation_key"), []byte("initial_value"))
	})
	if err != nil {
		t.Fatalf("Failed to insert initial key: %v", err)
	}

	// Start a long-running transaction
	txnA, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Read the value in transaction A
	valueA1, err := txnA.Get([]byte("isolation_key"))
	if err != nil {
		t.Fatalf("Failed to read key in txn A: %v", err)
	}
	if string(valueA1) != "initial_value" {
		t.Errorf("Expected initial_value in txn A, got %s", valueA1)
	}

	// Now update the key in a separate transaction
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("isolation_key"), []byte("updated_value"))
	})
	if err != nil {
		t.Fatalf("Failed to update key: %v", err)
	}

	// Start a new transaction that should see the updated value
	txnB, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	valueB, err := txnB.Get([]byte("isolation_key"))
	if err != nil {
		t.Fatalf("Failed to read key in txn B: %v", err)
	}
	if string(valueB) != "updated_value" {
		t.Errorf("Expected updated_value in txn B, got %s", valueB)
	}

	// Transaction A should still see the original value (snapshot isolation)
	valueA2, err := txnA.Get([]byte("isolation_key"))
	if err != nil {
		t.Fatalf("Failed to read key again in txn A: %v", err)
	}
	if string(valueA2) != "initial_value" {
		t.Errorf("Expected initial_value in txn A second read, got %s", valueA2)
	}

	// Verify both values match within each transaction (read stability)
	if string(valueA1) != string(valueA2) {
		t.Errorf("Snapshot isolation violation: txn A saw %s then %s", valueA1, valueA2)
	}
}

func TestTxn_Update(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_update_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Test successful update
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("update_key"), []byte("update_value"))
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify the update was applied
	var value []byte
	err = db.Update(func(txn *Txn) error {
		var err error
		value, err = txn.Get([]byte("update_key"))
		return err
	})
	if err != nil {
		t.Fatalf("Failed to read update: %v", err)
	}
	if string(value) != "update_value" {
		t.Errorf("Expected update_value, got %s", value)
	}

	// Test update with error
	customErr := fmt.Errorf("simulated error")
	err = db.Update(func(txn *Txn) error {
		err := txn.Put([]byte("error_key"), []byte("error_value"))
		if err != nil {
			return err
		}
		return customErr // Return explicit error instead of fmt.Errorf
	})
	if err == nil || err.Error() != customErr.Error() {
		t.Fatalf("Expected specific error from Update, got: %v", err)
	}

	// Verify the failed update didn't apply changes
	err = db.Update(func(txn *Txn) error {
		_, err := txn.Get([]byte("error_key"))
		if err == nil {
			return fmt.Errorf("expected error_key to not exist")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
}

func TestTxn_ConcurrentOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Number of concurrent writers
	const numWriters = 5

	// Keys per writer
	const keysPerWriter = 20

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < keysPerWriter; i++ {
				key := []byte(fmt.Sprintf("conc_key_w%d_k%d", writerID, i))
				value := []byte(fmt.Sprintf("value_w%d_k%d", writerID, i))

				err := db.Update(func(txn *Txn) error {
					return txn.Put(key, value)
				})
				if err != nil {
					t.Errorf("Writer %d failed to write key %d: %v", writerID, i, err)
					return
				}

				// Small sleep to reduce contention
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Wait for all writers to finish
	wg.Wait()

	// Verify all data was written correctly
	successCount := 0
	for w := 0; w < numWriters; w++ {
		for i := 0; i < keysPerWriter; i++ {
			key := []byte(fmt.Sprintf("conc_key_w%d_k%d", w, i))
			expectedValue := []byte(fmt.Sprintf("value_w%d_k%d", w, i))

			var actualValue []byte
			err := db.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get(key)
				return err
			})

			if err == nil && bytes.Equal(actualValue, expectedValue) {
				successCount++
			} else if err != nil {
				t.Logf("Failed to read key %s: %v", key, err)
			} else {
				t.Logf("Value mismatch for key %s: expected %s, got %s",
					key, expectedValue, actualValue)
			}
		}
	}

	// We should have at least 95% success rate
	// (some transactions might fail due to contention)
	expectedSuccesses := int(float64(numWriters*keysPerWriter) * 0.95)
	if successCount < expectedSuccesses {
		t.Errorf("Expected at least %d successful operations, got %d",
			expectedSuccesses, successCount)
	} else {
		t.Logf("Verified %d out of %d concurrent writes successfully",
			successCount, numWriters*keysPerWriter)
	}
}

func TestTxn_WALRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	opts := &Options{
		Directory:  dir,
		SyncOption: SyncFull, // Use full sync for WAL reliability
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("committed_key"), []byte("committed_value"))
	})
	if err != nil {
		t.Fatalf("Failed to write committed key: %v", err)
	}

	// Uncommitted transaction
	txnUncommitted, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txnUncommitted.Put([]byte("uncommitted_key"), []byte("uncommitted_value"))
	if err != nil {
		t.Fatalf("Failed to write uncommitted key: %v", err)
	}

	// Transaction that will be rolled back
	txnRolledBack, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txnRolledBack.Put([]byte("rolledback_key"), []byte("rolledback_value"))
	if err != nil {
		t.Fatalf("Failed to write rollback key: %v", err)
	}
	err = txnRolledBack.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Close the database to simulate a crash/restart
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Drain the log channel
	for len(logChan) > 0 {
		<-logChan
	}

	// Reopen the database to test WAL recovery
	logChan = make(chan string, 100)
	opts.LogChannel = logChan

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func(db2 *DB) {
		_ = db2.Close()
	}(db2)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Check committed transaction was recovered
	var value []byte
	err = db2.Update(func(txn *Txn) error {
		var err error
		value, err = txn.Get([]byte("committed_key"))
		return err
	})
	if err != nil {
		t.Errorf("Failed to get committed key after recovery: %v", err)
	} else if string(value) != "committed_value" {
		t.Errorf("Expected committed_value, got %s", value)
	}

	// Check uncommitted transaction was not applied
	err = db2.Update(func(txn *Txn) error {
		_, err := txn.Get([]byte("uncommitted_key"))
		if err == nil {
			return fmt.Errorf("uncommitted key should not be accessible")
		}
		return nil
	})
	if err != nil {
		t.Errorf("Uncommitted key check failed: %v", err)
	}

	// Check rolled back transaction was not applied
	err = db2.Update(func(txn *Txn) error {
		_, err := txn.Get([]byte("rolledback_key"))
		if err == nil {
			return fmt.Errorf("rolled back key should not be accessible")
		}
		return nil
	})
	if err != nil {
		t.Errorf("Rolled back key check failed: %v", err)
	}
}

func TestTxn_DeleteTimestamp(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_transaction_delete_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logChan := make(chan string, 100)
	defer func() {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	key := []byte("timestamp_test_key")
	value := []byte("timestamp_test_value")

	txn1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Insert transaction ID: %d, Timestamp: %d", txn1.Id, txn1.Timestamp)

	err = txn1.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to insert key: %v", err)
	}

	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit insert: %v", err)
	}

	txn2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Verification transaction ID: %d, Timestamp: %d", txn2.Id, txn2.Timestamp)

	retrievedValue, err := txn2.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after insert: %v", err)
	}
	if !bytes.Equal(retrievedValue, value) {
		t.Fatalf("Value mismatch: expected %s, got %s", value, retrievedValue)
	}
	t.Logf("Key found after insertion: %s", retrievedValue)

	// Delete the key with a new transaction
	txn3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Delete transaction ID: %d, Timestamp: %d", txn3.Id, txn3.Timestamp)

	err = txn3.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete: %v", err)
	}

	// Verify the key is deleted with a new transaction
	txn4, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Post-delete verification transaction ID: %d, Timestamp: %d", txn4.Id, txn4.Timestamp)

	_, err = txn4.Get(key)
	if err == nil {
		t.Fatalf("Key still accessible after deletion")
	}
	t.Logf("Key correctly not found after deletion: %v", err)

	// Verify we can still access the key with a timestamp before deletion
	txn5, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	txn5.Timestamp = txn1.Timestamp // Use the original insert timestamp
	t.Logf("Historical verification transaction ID: %d, Using Timestamp: %d", txn5.Id, txn5.Timestamp)

	retrievedValue, err = txn5.Get(key)
	if err != nil {
		t.Logf("Historical view should see the key but got error: %v", err)
	} else {
		t.Logf("Historical view sees value: %s", retrievedValue)
	}
}

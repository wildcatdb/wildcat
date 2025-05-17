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

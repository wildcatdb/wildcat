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
	"reflect"
	"sync"
	"testing"
)

// TestSSTableSerialization tests the serialization and deserialization of SSTable
func TestSSTableSerialization(t *testing.T) {
	// Create a test SSTable
	original := &SSTable{
		Id:         12345,
		Min:        []byte("aaaaa"),
		Max:        []byte("zzzzz"),
		isMerging:  0,
		Size:       98765,
		EntryCount: 1000,
		Level:      2,
		db:         nil, // We can't compare functions, so leaving this nil
	}

	// Serialize the SSTable
	data, err := original.serializeSSTable()
	if err != nil {
		t.Fatalf("Failed to serialize SSTable: %v", err)
	}

	// Deserialize the SSTable
	result := &SSTable{}
	err = result.deserializeSSTable(data)
	if err != nil {
		t.Fatalf("Failed to deserialize SSTable: %v", err)
	}

	// Compare the original and deserialized SSTable
	if original.Id != result.Id {
		t.Errorf("Id mismatch: expected %d, got %d", original.Id, result.Id)
	}
	if !bytes.Equal(original.Min, result.Min) {
		t.Errorf("Min mismatch: expected %v, got %v", original.Min, result.Min)
	}
	if !bytes.Equal(original.Max, result.Max) {
		t.Errorf("Max mismatch: expected %v, got %v", original.Max, result.Max)
	}
	if original.isMerging != result.isMerging {
		t.Errorf("isMerging mismatch: expected %d, got %d", original.isMerging, result.isMerging)
	}
	if original.Size != result.Size {
		t.Errorf("Size mismatch: expected %d, got %d", original.Size, result.Size)
	}
	if original.EntryCount != result.EntryCount {
		t.Errorf("EntryCount mismatch: expected %d, got %d", original.EntryCount, result.EntryCount)
	}
	if original.Level != result.Level {
		t.Errorf("Level mismatch: expected %d, got %d", original.Level, result.Level)
	}
	// We don't compare db field as it's a pointer to DB
}

// TestTxnSerialization tests the serialization and deserialization of Txn
func TestTxnSerialization(t *testing.T) {
	// Create a test transaction
	original := &Txn{
		Id:        123,
		db:        nil, // We can't compare functions, so leaving this nil
		ReadSet:   map[string]int64{"key1": 100, "key2": 200},
		WriteSet:  map[string][]byte{"key3": []byte("value3"), "key4": []byte("value4")},
		DeleteSet: map[string]bool{"key5": true, "key6": false},
		Timestamp: 1621234567,
		mutex:     sync.Mutex{},
		Committed: true,
	}

	// Serialize the transaction
	data, err := original.serializeTransaction()
	if err != nil {
		t.Fatalf("Failed to serialize transaction: %v", err)
	}

	// Deserialize the transaction
	result := &Txn{}
	err = result.deserializeTransaction(data)
	if err != nil {
		t.Fatalf("Failed to deserialize transaction: %v", err)
	}

	// Compare the original and deserialized transaction
	if original.Id != result.Id {
		t.Errorf("id mismatch: expected %d, got %d", original.Id, result.Id)
	}
	if !reflect.DeepEqual(original.ReadSet, result.ReadSet) {
		t.Errorf("ReadSet mismatch: expected %v, got %v", original.ReadSet, result.ReadSet)
	}

	// Compare WriteSet - need to check each byte array
	if len(original.WriteSet) != len(result.WriteSet) {
		t.Errorf("WriteSet length mismatch: expected %d, got %d", len(original.WriteSet), len(result.WriteSet))
	} else {
		for k, v := range original.WriteSet {
			if rv, ok := result.WriteSet[k]; !ok {
				t.Errorf("WriteSet missing key: %s", k)
			} else if !bytes.Equal(v, rv) {
				t.Errorf("WriteSet value mismatch for key %s: expected %v, got %v", k, v, rv)
			}
		}
	}

	if !reflect.DeepEqual(original.DeleteSet, result.DeleteSet) {
		t.Errorf("DeleteSet mismatch: expected %v, got %v", original.DeleteSet, result.DeleteSet)
	}
	if original.Timestamp != result.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", original.Timestamp, result.Timestamp)
	}
	if original.Committed != result.Committed {
		t.Errorf("Committed mismatch: expected %t, got %t", original.Committed, result.Committed)
	}
	// We don't compare db field as it's a pointer to DB
	// We don't compare mutex as it's not easily comparable
}

// TestBlockSetSerialization tests the serialization and deserialization of BlockSet
func TestBlockSetSerialization(t *testing.T) {
	// Create a test block set
	original := &BlockSet{
		Entries: []*KLogEntry{
			{Key: []byte("key1"), ValueBlockID: 1, Timestamp: 1000},
			{Key: []byte("key2"), ValueBlockID: 2, Timestamp: 2000},
			{Key: []byte("key3"), ValueBlockID: 3, Timestamp: 3000},
		},
		Size: 12345,
	}

	// Serialize the block set
	data, err := original.serializeBlockSet()
	if err != nil {
		t.Fatalf("Failed to serialize block set: %v", err)
	}

	// Deserialize the block set
	result := &BlockSet{}
	err = result.deserializeBlockSet(data)
	if err != nil {
		t.Fatalf("Failed to deserialize block set: %v", err)
	}

	// Compare the original and deserialized block set
	if original.Size != result.Size {
		t.Errorf("Size mismatch: expected %d, got %d", original.Size, result.Size)
	}

	if len(original.Entries) != len(result.Entries) {
		t.Errorf("Entries length mismatch: expected %d, got %d", len(original.Entries), len(result.Entries))
	} else {
		for i, entry := range original.Entries {
			resultEntry := result.Entries[i]
			if !bytes.Equal(entry.Key, resultEntry.Key) {
				t.Errorf("Entry %d Key mismatch: expected %v, got %v", i, entry.Key, resultEntry.Key)
			}
			if entry.ValueBlockID != resultEntry.ValueBlockID {
				t.Errorf("Entry %d Value mismatch: expected %v, got %v", i, entry.ValueBlockID, resultEntry.ValueBlockID)
			}
			if entry.Timestamp != resultEntry.Timestamp {
				t.Errorf("Entry %d Timestamp mismatch: expected %d, got %d", i, entry.Timestamp, resultEntry.Timestamp)
			}
		}
	}
}

// TestSSTableSerializationError tests error handling in SSTable serialization
func TestSSTableSerializationError(t *testing.T) {
	// Create an invalid SSTable that would cause serialization to fail
	// This is difficult to simulate with gob, but we can test error handling

	// Test deserialize with empty data
	sst := &SSTable{}
	err := sst.deserializeSSTable([]byte{})
	if err == nil {
		t.Errorf("Expected error when deserializing empty data, got nil")
	}
}

// TestTxnSerializationError tests error handling in Txn serialization
func TestTxnSerializationError(t *testing.T) {
	// Test deserialize with empty data
	txn := &Txn{}
	err := txn.deserializeTransaction([]byte{})
	if err == nil {
		t.Errorf("Expected error when deserializing empty data, got nil")
	}
}

// TestBlockSetSerializationError tests error handling in BlockSet serialization
func TestBlockSetSerializationError(t *testing.T) {
	// Test deserialize with empty data
	bs := &BlockSet{}
	err := bs.deserializeBlockSet([]byte{})
	if err == nil {
		t.Errorf("Expected error when deserializing empty data, got nil")
	}
}

// Additional test for all three serialization functions with corrupted data
func TestCorruptedDataDeserialization(t *testing.T) {
	// Create corrupted data (just some random bytes)
	corruptedData := []byte{0x1, 0x2, 0x3, 0x4, 0x5}

	// Test SSTable
	sst := &SSTable{}
	err := sst.deserializeSSTable(corruptedData)
	if err == nil {
		t.Errorf("Expected error when deserializing corrupted SSTable data, got nil")
	}

	// Test Txn
	txn := &Txn{}
	err = txn.deserializeTransaction(corruptedData)
	if err == nil {
		t.Errorf("Expected error when deserializing corrupted Txn data, got nil")
	}

	// Test BlockSet
	bs := &BlockSet{}
	err = bs.deserializeBlockSet(corruptedData)
	if err == nil {
		t.Errorf("Expected error when deserializing corrupted BlockSet data, got nil")
	}
}

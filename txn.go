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
	"github.com/google/uuid"
	"orindb/blockmanager"
	"sync"
	"sync/atomic"
	"time"
)

// Txn represents a transaction in OrinDB
type Txn struct {
	id        string
	db        *DB
	ReadSet   map[string]int64
	WriteSet  map[string][]byte
	DeleteSet map[string]bool
	Timestamp int64
	mutex     sync.Mutex
	Committed bool
}

// Begin starts a new transaction
func (db *DB) Begin() *Txn {
	txn := &Txn{
		id:        uuid.New().String(),
		db:        db,
		ReadSet:   make(map[string]int64),
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		Timestamp: time.Now().UnixNano(),
		Committed: false,
		mutex:     sync.Mutex{},
	}

	// Add the transaction to the list of transactions, do a swap to make it atomic
	txnList := db.txns.Load()
	if txnList == nil {
		txns := make([]*Txn, 0)

		txns = append(txns, txn)
		db.txns.Store(&txns)
	}

	return txn
}

// GetTxn retrieves a transaction by ID
func (db *DB) GetTxn(id string) (*Txn, error) {
	// Find the transaction by ID
	txns := db.txns.Load()
	if txns == nil {
		return nil, fmt.Errorf("transaction not found")
	}

	for _, txn := range *txns {
		if txn.id == id {
			return txn, nil
		}
	}

	return nil, fmt.Errorf("transaction not found")
}

// Put adds key-value pair to database
func (txn *Txn) Put(key []byte, value []byte) error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add to write set
	txn.WriteSet[string(key)] = value
	delete(txn.DeleteSet, string(key)) // Remove from delete set if exists

	// We append to the WAL here
	err := txn.appendWal()
	if err == nil {
		return err
	}

	return nil
}

// Delete removes a key from database
func (txn *Txn) Delete(key []byte) error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add to delete set
	txn.DeleteSet[string(key)] = true
	delete(txn.WriteSet, string(key)) // Remove from write set if exists

	// We append to the WAL here
	err := txn.appendWal()
	if err == nil {
		return err
	}

	return nil
}

// Commit commits the transaction
func (txn *Txn) Commit() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	if txn.Committed {
		return nil // Already committed
	}

	// Apply writes
	for key, value := range txn.WriteSet {
		txn.db.memtable.Load().(*Memtable).skiplist.Put([]byte(key), value, txn.Timestamp)
		// Increment the size of the memtable
		atomic.AddInt64(&txn.db.memtable.Load().(*Memtable).size, int64(len(key)+len(value)))
	}

	// Apply deletes
	for key := range txn.DeleteSet {
		txn.db.memtable.Load().(*Memtable).skiplist.Delete([]byte(key), txn.Timestamp)
		// Decrement the size of the memtable
		atomic.AddInt64(&txn.db.memtable.Load().(*Memtable).size, -int64(len(key)))
	}

	txn.Committed = true

	// We append to the WAL here
	err := txn.appendWal()
	if err != nil {
		return err
	}

	// Check if we need to flush the memtable to stack
	if atomic.LoadInt64(&txn.db.memtable.Load().(*Memtable).size) > txn.db.opts.WriteBufferSize {
		err = txn.db.flusher.queueMemtable()
		if err != nil {
			return fmt.Errorf("failed to queue memtable: %w", err)
		}
	}

	return nil
}

// Rollback rolls back the transaction
func (txn *Txn) Rollback() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()
	defer txn.remove()

	// Clear all pending changes
	txn.WriteSet = make(map[string][]byte)
	txn.DeleteSet = make(map[string]bool)
	txn.ReadSet = make(map[string]int64)

	txn.Committed = false

	// We append to the WAL here
	err := txn.appendWal()
	if err == nil {
		return err
	}

	return nil
}

// Get retrieves a value by key
func (txn *Txn) Get(key []byte) ([]byte, error) {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Check write set first
	if val, exists := txn.WriteSet[string(key)]; exists {
		return val, nil
	}

	// Record read for conflict detection
	txn.ReadSet[string(key)] = txn.Timestamp

	// Fetch from skiplist
	val := txn.db.memtable.Load().(*Memtable).skiplist.Get(key, txn.Timestamp)
	if val != nil {

		return val.([]byte), nil
	}

	// Check immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		val = memt.skiplist.Get(key, txn.Timestamp)
		if val != nil {
			return false // Found in immutable memtable
		}
		return true // Continue searching
	})

	if val != nil {
		return val.([]byte), nil
	}

	// Check levels
	levels := txn.db.levels.Load()
	for _, level := range *levels {
		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		for _, sstable := range *sstables {
			val = sstable.get(key, txn.Timestamp)
			if val != nil {
				return val.([]byte), nil
			}
		}
	}

	return nil, fmt.Errorf("key not found")
}

// NewIterator creates a bidirectional iterator for the transaction
func (txn *Txn) NewIterator(startKey []byte) *Iterator {
	return txn.newIterator(startKey)
}

// GetRange retrieves all key-value pairs in the given key range
func (txn *Txn) GetRange(startKey, endKey []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// Get an iterator starting at the start key
	iter := txn.NewIterator(startKey)

	// Iterate until we reach the end key or run out of keys
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}

		// Check if we've gone past the end key
		if endKey != nil && bytes.Compare(key, endKey) > 0 {
			break
		}

		// Add to result
		result[string(key)] = value.([]byte)
	}

	return result, nil
}

// Count returns the number of entries in the given key range
func (txn *Txn) Count(startKey, endKey []byte) (int, error) {
	count := 0

	// Get an iterator starting at the start key
	iter := txn.NewIterator(startKey)

	// Iterate until we reach the end key or run out of keys
	for {
		key, _, ok := iter.Next()
		if !ok {
			break
		}

		// Check if we've gone past the end key
		if endKey != nil && bytes.Compare(key, endKey) > 0 {
			break
		}

		count++
	}

	return count, nil
}

// Scan executes a function on each key-value pair in the given range
func (txn *Txn) Scan(startKey, endKey []byte, fn func(key []byte, value []byte) bool) error {
	// Get an iterator starting at the start key
	iter := txn.NewIterator(startKey)

	// Iterate until we reach the end key or run out of keys
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}

		// Check if we've gone past the end key
		if endKey != nil && bytes.Compare(key, endKey) > 0 {
			break
		}

		// Apply the function
		// If it returns false, stop scanning
		if !fn(key, value.([]byte)) {
			break
		}
	}

	return nil
}

// ForEach iterates through all entries in the database
func (txn *Txn) ForEach(fn func(key []byte, value []byte) bool) error {
	return txn.Scan(nil, nil, fn)
}

// Iterate is a higher-level bidirectional iterator using callbacks
func (txn *Txn) Iterate(startKey []byte, direction int, fn func(key []byte, value []byte) bool) error {
	// Get a merge iterator starting at the given key
	iter := txn.NewIterator(startKey)

	// Iterate in the specified direction
	var key []byte
	var value interface{}
	var ok bool

	for {
		if direction >= 0 {
			// Forward iteration
			key, value, ok = iter.Next()
		} else {
			// Backward iteration
			key, value, ok = iter.Prev()
		}

		if !ok {
			break
		}

		// Apply the function
		// If it returns false, stop iterating
		if !fn(key, value.([]byte)) {
			break
		}
	}

	return nil
}

// Update performs an atomic update using a transaction
func (db *DB) Update(fn func(txn *Txn) error) error {

	txn := db.Begin()
	err := fn(txn)
	if err != nil {
		err = txn.Rollback()
		if err != nil {
			return err
		}
		return err
	}
	return txn.Commit()
}

// remove removes the transaction from the database
func (txn *Txn) remove() {

	// Clear all sets
	txn.ReadSet = make(map[string]int64)
	txn.WriteSet = make(map[string][]byte)
	txn.DeleteSet = make(map[string]bool)

	txn.Committed = false

	// Remove from the transaction list
	txns := txn.db.txns.Load()
	if txns != nil {
		for i, t := range *txns {
			if t.id == txn.id {
				*txns = append((*txns)[:i], (*txns)[i+1:]...)
				break
			}
		}
		txn.db.txns.Store(txns)

	}
}

// appendWal appends the transaction to a Write-Ahead Log (WAL)
func (txn *Txn) appendWal() error {
	// serialize the transaction
	data, err := txn.serializeTransaction()
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}

	wal, ok := txn.db.lru.Get(txn.db.memtable.Load().(*Memtable).wal.path)
	if !ok {
		return fmt.Errorf("failed to get WAL from LRU cache")
	}

	// Append the serialized transaction to the WAL
	_, err = wal.(*blockmanager.BlockManager).Append(data)
	if err != nil {
		return err
	}

	return nil
}

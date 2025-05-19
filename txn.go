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
	"orindb/blockmanager"
	"sync"
	"sync/atomic"
	"time"
)

// Txn represents a transaction in OrinDB
type Txn struct {
	Id        int64
	ReadSet   map[string]int64
	WriteSet  map[string][]byte
	DeleteSet map[string]bool
	Timestamp int64
	Committed bool
	db        *DB        // Not exported
	mutex     sync.Mutex // Not exported
}

// Begin starts a new transaction
func (db *DB) Begin() *Txn {
	txn := &Txn{
		Id:        db.txnIdGenerator.nextID(),
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
func (db *DB) GetTxn(id int64) (*Txn, error) {
	// Find the transaction by ID
	txns := db.txns.Load()
	if txns == nil {
		return nil, fmt.Errorf("transaction not found")
	}

	for _, txn := range *txns {
		if txn.Id == id {
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

	err := txn.appendWal()
	if err != nil {
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

	err := txn.appendWal()
	if err != nil {
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

	err := txn.appendWal()
	if err != nil {
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

	if txn.DeleteSet[string(key)] {
		return nil, fmt.Errorf("key not found")
	}

	// Track the best value found so far and its timestamp
	var bestValue []byte = nil
	var bestTimestamp int64 = 0

	// Fetch from active memtable
	val, ts, ok := txn.db.memtable.Load().(*Memtable).skiplist.Get(key, txn.Timestamp)
	if ok && ts > bestTimestamp {
		bestValue = val
		bestTimestamp = ts
	}

	// Check immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		val, ts, ok = memt.skiplist.Get(key, txn.Timestamp)
		if ok && ts > bestTimestamp {
			bestValue = val
			bestTimestamp = ts
		}
		return true // Continue searching
	})

	if val != nil && ts <= txn.Timestamp {
		if ts > bestTimestamp {
			bestValue = val
			bestTimestamp = ts
			if ts == txn.Timestamp {
				return bestValue, nil // Early return
			}
		}
	}

	// Check levels for SSTables
	levels := txn.db.levels.Load()
	for _, level := range *levels {
		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		for _, sstable := range *sstables {
			// Check if the key might be in this SSTable's range
			if bytes.Compare(key, sstable.Min) < 0 || bytes.Compare(key, sstable.Max) > 0 {
				continue // Key outside SSTable range
			}

			// Try to get the value from this SSTable
			val, ts := sstable.get(key, txn.Timestamp)

			// If we found a value and it's newer than what we have so far
			// but still not newer than our read timestamp
			if val != nil && ts <= txn.Timestamp && ts > bestTimestamp {
				bestValue = val
				bestTimestamp = ts
			}
		}
	}

	if bestValue != nil {
		return bestValue, nil
	}

	return nil, fmt.Errorf("key not found")
}

// Update performs an atomic update using a transaction
func (db *DB) Update(fn func(txn *Txn) error) error {
	txn := db.Begin()
	fnErr := fn(txn)
	if fnErr != nil {
		rollbackErr := txn.Rollback()
		if rollbackErr != nil {
			return fmt.Errorf("transaction failed: %v, rollback failed: %w", fnErr, rollbackErr)
		}
		return fnErr // Return the original error from the function
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
			if t.Id == txn.Id {
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

func (txn *Txn) NewIterator(startKey []byte, prefix []byte) *MergeIterator {
	return NewMergeIterator(txn, startKey, prefix)
}

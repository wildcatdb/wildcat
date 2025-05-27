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
	"errors"
	"fmt"
	"github.com/guycipher/wildcat/blockmanager"
	"github.com/guycipher/wildcat/tree"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Txn represents a transaction in Wildcat
type Txn struct {
	Id        int64             // The transactions id, can be recovered
	ReadSet   map[string]int64  // Key -> Timestamp
	WriteSet  map[string][]byte // Key -> Value
	DeleteSet map[string]bool   // Key -> Deleted
	Timestamp int64             // The timestamp of the transaction
	Committed bool              // Whether the transaction is committed
	db        *DB               // Not exported for serialization
	mutex     sync.Mutex        // Not exported for serialization
}

// Begin starts a new transaction
func (db *DB) Begin() *Txn {
	txn := &Txn{
		Id:        db.txnIdGenerator.nextID(),
		db:        db,
		ReadSet:   make(map[string]int64),
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		Timestamp: db.txnTSGenerator.nextID(), // Monotonic ordering and no timestamp collisions even under extreme load
		Committed: false,
		mutex:     sync.Mutex{},
	}

	// Add the transaction to the list of transactions, do a swap to make it atomic
	txnList := db.txns.Load()
	if txnList == nil {
		txns := make([]*Txn, 0)

		txns = append(txns, txn)
		db.txns.Store(&txns)
	} else {
		txns := *txnList
		txns = append(txns, txn)
		db.txns.Store(&txns)

	}

	return txn
}

// GetTxn retrieves a transaction by ID.
// Can be used on system recovery.  You can recover an incomplete transaction.
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

	// Check if we need to enqueue the memtable for flush
	if atomic.LoadInt64(&txn.db.memtable.Load().(*Memtable).size) > txn.db.opts.WriteBufferSize {

		// Enqueue the memtable for flush and swap
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

	// We always update oldestActiveRead to latest read which could be the oldest if that makes sense :)
	// On background compactions we don't just want to blindly merge sstables that could be part of active reads transactions.
	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

	// Check write set first (transaction's own writes)
	if val, exists := txn.WriteSet[string(key)]; exists {
		return val, nil
	}

	// Check delete set (transaction's own deletes)
	if txn.DeleteSet[string(key)] {
		return nil, fmt.Errorf("key not found")
	}

	// Track the best value found so far and its timestamp
	var bestValue []byte = nil
	var bestTimestamp int64 = 0

	// Fetch from active memtable
	val, ts, ok := txn.db.memtable.Load().(*Memtable).skiplist.Get(key, txn.Timestamp)
	if ok && ts <= txn.Timestamp && ts > bestTimestamp {
		bestValue = val
		bestTimestamp = ts
	}

	// Check currently flushing memtable if any
	// We do this as depending on write buffer the flushing memory table can take a while to persist to an immutable btree (sstable)
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		val, ts, ok = fmem.skiplist.Get(key, txn.Timestamp)
		if ok && ts <= txn.Timestamp && ts > bestTimestamp {
			bestValue = val
			bestTimestamp = ts
		}
	}

	// Check immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		val, ts, ok = memt.skiplist.Get(key, txn.Timestamp)
		if ok && ts <= txn.Timestamp && ts > bestTimestamp {
			bestValue = val
			bestTimestamp = ts
		}
		return true // Continue searching
	})

	// Check levels for SSTables
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}

			// Check all SSTables in this level
			for _, sstable := range *sstables {
				val, ts = sstable.get(key, txn.Timestamp)

				if ts == 0 {
					continue // Key not found in this SSTable or timestamp not visible
				}

				// Only update if we found a better (more recent) version
				// that's still visible to our read timestamp
				if ts <= txn.Timestamp && ts > bestTimestamp {
					bestValue = val // This could be nil for deletions
					bestTimestamp = ts
				}
			}
		}
	}

	// Check if we found a value
	if bestTimestamp > 0 {
		if bestValue == nil {
			return nil, fmt.Errorf("key not found") // This was a deletion
		}
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

// View performs a read-only transaction
func (db *DB) View(fn func(txn *Txn) error) error {
	txn := db.Begin()
	defer txn.remove() // Ensure transaction is cleaned up

	fnErr := fn(txn)
	if fnErr != nil {
		return fnErr
	}

	// We don't commit - this is a read-only transaction
	return nil
}

// NewIterator creates a new bidirectional iterator
func (txn *Txn) NewIterator(asc bool) (*MergeIterator, error) {
	var items []*iterator

	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

	// Active memtable
	active := txn.db.memtable.Load().(*Memtable)
	iter, err := active.skiplist.NewIterator(nil, txn.Timestamp)
	if err != nil {
		return nil, err
	}
	if !asc {
		iter.ToLast()
	}

	items = append(items, &iterator{
		underlyingIterator: iter,
	})

	// Currently flushing memtable if any
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		it, err := fmem.skiplist.NewIterator(nil, txn.Timestamp)
		if err == nil {
			if !asc {
				it.ToLast()
			}
			items = append(items, &iterator{
				underlyingIterator: it,
			})
		}
	}

	// Immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		it, err := memt.skiplist.NewIterator(nil, txn.Timestamp)
		if err != nil {
			return false // Error occurred, stop iteration
		}
		if !asc {
			it.ToLast()
		}

		items = append(items, &iterator{
			underlyingIterator: it,
		})
		return true
	})

	// SSTables from each level
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}
			for _, sst := range *sstables {
				klogPath := sst.kLogPath()
				klogBm, ok := txn.db.lru.Get(klogPath)
				if !ok {
					var err error
					klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY,
						txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
					if err != nil {
						continue
					}
					txn.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
						if bm, ok := value.(*blockmanager.BlockManager); ok {
							_ = bm.Close()
						}
					})
				} else if v, ok := klogBm.(*blockmanager.BlockManager); !ok || v == nil {
					continue
				}

				t, err := tree.Open(klogBm.(*blockmanager.BlockManager), sst.db.opts.SSTableBTreeOrder, sst)
				if err != nil {
					continue
				}
				treeIter, err := t.Iterator(asc)
				if err != nil {
					continue
				}
				items = append(items, &iterator{
					underlyingIterator: treeIter,
					sst:                sst,
				})
			}
		}
	}

	return NewMergeIterator(items, txn.Timestamp, asc)
}

// NewRangeIterator creates a new range bidirectional iterator
func (txn *Txn) NewRangeIterator(startKey []byte, endKey []byte, asc bool) (*MergeIterator, error) {
	var items []*iterator

	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

	// Active memtable
	active := txn.db.memtable.Load().(*Memtable)
	iter, err := active.skiplist.NewRangeIterator(startKey, endKey, txn.Timestamp)
	if err != nil {
		return nil, err
	}
	if !asc {
		iter.ToLast()
	}

	items = append(items, &iterator{
		underlyingIterator: iter,
	})

	// Currently flushing memtable if any
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		it, err := fmem.skiplist.NewRangeIterator(startKey, endKey, txn.Timestamp)
		if err == nil {
			if !asc {
				it.ToLast()
			}
			items = append(items, &iterator{
				underlyingIterator: it,
			})
		}
	}

	// Immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		it, err := memt.skiplist.NewRangeIterator(startKey, endKey, txn.Timestamp)
		if err != nil {
			return false // Error occurred, stop iteration
		}
		if !asc {
			it.ToLast()
		}

		items = append(items, &iterator{
			underlyingIterator: it,
		})
		return true
	})

	// SSTables from each level
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}
			for _, sst := range *sstables {
				klogPath := sst.kLogPath()
				klogBm, ok := txn.db.lru.Get(klogPath)
				if !ok {
					var err error
					klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY,
						txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
					if err != nil {
						continue
					}
					txn.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
						if bm, ok := value.(*blockmanager.BlockManager); ok {
							_ = bm.Close()
						}
					})
				} else if v, ok := klogBm.(*blockmanager.BlockManager); !ok || v == nil {
					continue
				}

				t, err := tree.Open(klogBm.(*blockmanager.BlockManager), sst.db.opts.SSTableBTreeOrder, sst)
				if err != nil {
					continue
				}
				treeIter, err := t.RangeIterator(startKey, endKey, asc)
				if err != nil {
					continue
				}
				items = append(items, &iterator{
					underlyingIterator: treeIter,
					sst:                sst,
				})
			}
		}
	}

	return NewMergeIterator(items, txn.Timestamp, asc)
}

// NewPrefixIterator creates a new prefix bidirectional iterator
func (txn *Txn) NewPrefixIterator(prefix []byte, asc bool) (*MergeIterator, error) {
	var items []*iterator

	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

	// Active memtable
	active := txn.db.memtable.Load().(*Memtable)
	iter, err := active.skiplist.NewPrefixIterator(prefix, txn.Timestamp)
	if err != nil {
		return nil, err
	}
	if !asc {
		iter.ToLast()
	}

	items = append(items, &iterator{
		underlyingIterator: iter,
	})

	// Currently flushing memtable if any
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		it, err := fmem.skiplist.NewPrefixIterator(prefix, txn.Timestamp)
		if err == nil {
			if !asc {
				it.ToLast()
			}
			items = append(items, &iterator{
				underlyingIterator: it,
			})
		}
	}

	// Immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		it, err := memt.skiplist.NewPrefixIterator(prefix, txn.Timestamp)
		if err != nil {
			return false // Error occurred, stop iteration
		}
		if !asc {
			it.ToLast()
		}

		items = append(items, &iterator{
			underlyingIterator: it,
		})
		return true
	})

	// SSTables from each level
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}
			for _, sst := range *sstables {
				klogPath := sst.kLogPath()
				klogBm, ok := txn.db.lru.Get(klogPath)
				if !ok {
					var err error
					klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY,
						txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
					if err != nil {
						continue
					}
					txn.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
						if bm, ok := value.(*blockmanager.BlockManager); ok {
							_ = bm.Close()
						}
					})
				} else if v, ok := klogBm.(*blockmanager.BlockManager); !ok || v == nil {
					continue
				}

				t, err := tree.Open(klogBm.(*blockmanager.BlockManager), sst.db.opts.SSTableBTreeOrder, sst)
				if err != nil {
					continue
				}
				treeIter, err := t.PrefixIterator(prefix, asc)
				if err != nil {
					continue
				}
				items = append(items, &iterator{
					underlyingIterator: treeIter,
					sst:                sst,
				})
			}
		}
	}

	return NewMergeIterator(items, txn.Timestamp, asc)
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

// appendWal appends the transaction state to a Write-Ahead Log (WAL)
func (txn *Txn) appendWal() error {
	data, err := txn.serializeTransaction()
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= txn.db.opts.WalAppendRetry; attempt++ {

		walPath := txn.db.memtable.Load().(*Memtable).wal.path
		wal, ok := txn.db.lru.Get(walPath)
		if !ok {
			// Open the WAL file
			walBm, err := blockmanager.Open(walPath, os.O_WRONLY|os.O_APPEND,
				txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
			if err != nil {
				lastErr = fmt.Errorf("failed to open WAL block manager: %w", err)
				if attempt == txn.db.opts.WalAppendRetry {
					return lastErr
				}
				time.Sleep(txn.db.opts.WalAppendBackoff)
				continue
			}

			// Add to LRU cache
			txn.db.lru.Put(walPath, walBm, func(key, value interface{}) {
				// Close the block manager when evicted from LRU
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})

			// Use the newly opened WAL
			wal = walBm
		}

		_, err = wal.(*blockmanager.BlockManager).Append(data)
		if err == nil {
			// Success EXIT!!
			return nil
		}

		lastErr = err

		// Special handling for bad file descriptor errors
		needsReopen := errors.Is(err, syscall.EBADF) ||
			strings.Contains(err.Error(), "bad file descriptor")

		if needsReopen {
			// Remove the bad file descriptor from cache first
			txn.db.lru.Delete(walPath)

			// Reopen the WAL file
			walBm, err := blockmanager.Open(walPath, os.O_WRONLY|os.O_APPEND,
				txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
			if err != nil {
				lastErr = fmt.Errorf("failed to reopen WAL block manager: %w", err)
				if attempt == txn.db.opts.WalAppendRetry {
					return lastErr
				}
				time.Sleep(txn.db.opts.WalAppendBackoff)
				continue
			}

			// Add to LRU cache
			txn.db.lru.Put(walPath, walBm, func(key, value interface{}) {
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})

			// Try append again immediately with the new descriptor
			_, err = walBm.Append(data)
			if err == nil {
				// Success after reopen EXIT!!
				return nil
			}

			lastErr = fmt.Errorf("failed to append transaction to WAL after reopen: %w", err)
		} else {
			lastErr = fmt.Errorf("failed to append transaction to WAL: %w", err)
		}

		// Check if we've used all our retries
		if attempt == txn.db.opts.WalAppendRetry {
			return lastErr
		}

		// Wait before next retry
		time.Sleep(txn.db.opts.WalAppendBackoff)
	}

	return lastErr
}

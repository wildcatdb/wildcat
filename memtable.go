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
	"fmt"
	"orindb/blockmanager"
	"orindb/skiplist"
	"os"
	"sync/atomic"
)

// A memtable contains a skiplist and a write-ahead log (WAL) for durability, they are paired.

// WAL is a write-ahead log structure
type WAL struct {
	path string // The WAL path i.e <db dir><timestamp>.wal
}

// Memtable is a memory table structure
type Memtable struct {
	skiplist *skiplist.SkipList // The skip list for the memtable, is atomic and concurrent safe
	wal      *WAL               // The write-ahead log for durability, is also atomic and concurrent safe
	size     int64              // Atomic size of the memtable
	db       *DB                // The database instance
}

// replay replays the WAL to recover the memtable
// In memtable.go: Improve replay method
func (memtable *Memtable) replay(activeTxns *[]*Txn) error {
	var walBm *blockmanager.BlockManager
	var err error

	// Check if wal file in lru cache and add debug logging
	walQueueEntry, ok := memtable.db.lru.Get(memtable.wal.path)
	if !ok {
		memtable.db.log(fmt.Sprintf("WAL file not in LRU cache, opening: %s", memtable.wal.path))
		// Open the WAL file
		walBm, err = blockmanager.Open(memtable.wal.path, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(memtable.db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add to LRU cache
		memtable.db.lru.Put(memtable.wal.path, walBm)
	} else {
		memtable.db.log(fmt.Sprintf("Found WAL file in LRU cache: %s", memtable.wal.path))
		// Use the cached WAL file handle
		walBm = walQueueEntry.(*blockmanager.BlockManager)
	}

	// Rest of the method remains largely unchanged...
	iter := walBm.Iterator()
	var memtSize int64

	// Track the latest state of each transaction by ID
	txnMap := make(map[int64]*Txn)
	var txnCount, committedCount int

	for {
		data, _, err := iter.Next()
		if err != nil {
			// End of WAL
			break
		}

		txnCount++

		// Deserialize the transaction
		var txn Txn
		err = txn.deserializeTransaction(data)
		if err != nil {
			memtable.db.log(fmt.Sprintf("Warning: failed to deserialize transaction: %v - skipping", err))
			continue // Skip this transaction instead of failing completely
		}

		// Set the database reference for the transaction
		txn.db = memtable.db

		// Update our transaction map with the latest state of this transaction
		txnMap[txn.Id] = &txn

		// Only apply committed transactions to the memtable
		if txn.Committed {
			committedCount++

			// Apply writes to the memtable
			for key, value := range txn.WriteSet {
				memtable.skiplist.Put([]byte(key), value, txn.Timestamp)
				memtSize += int64(len(key) + len(value))
			}

			// Apply deletes to the memtable
			for key := range txn.DeleteSet {
				memtable.skiplist.Delete([]byte(key), txn.Timestamp)
				memtSize -= int64(len(key))
			}
		}
	}

	memtable.db.log(fmt.Sprintf("Replay summary for %s: %d total transactions, %d committed",
		memtable.wal.path, txnCount, committedCount))

	// Update the memtable size
	atomic.StoreInt64(&memtable.size, memtSize)

	if activeTxns != nil {
		for _, txn := range txnMap {
			// Only include uncommitted transactions that haven't been rolled back
			if !txn.Committed && (len(txn.WriteSet) > 0 || len(txn.DeleteSet) > 0 || len(txn.ReadSet) > 0) {
				*activeTxns = append(*activeTxns, txn)
			}
		}

		if len(*activeTxns) > 0 {
			memtable.db.log(fmt.Sprintf("Found %d active transactions in %s", len(*activeTxns), memtable.wal.path))
		}
	}

	return nil
}

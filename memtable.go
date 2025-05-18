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
func (memtable *Memtable) replay(activeTxns *[]*Txn) error {
	var walBm *blockmanager.BlockManager
	var err error

	// Check if wal file is in lru cache
	walQueueEntry, ok := memtable.db.lru.Get(memtable.wal.path)
	if !ok {
		// Open the WAL file
		walBm, err = blockmanager.Open(memtable.wal.path, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(memtable.db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}
	} else {
		// Use the cached WAL file handle
		walBm = walQueueEntry.(*blockmanager.BlockManager)
	}

	iter := walBm.Iterator()
	var memtSize int64

	// Track the latest state of each transaction by ID
	txnMap := make(map[int64]*Txn)

	for {
		data, _, err := iter.Next()
		if err != nil {
			// End of WAL
			break
		}

		// Deserialize the transaction
		var txn Txn
		err = txn.deserializeTransaction(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize transaction: %w", err)
		}

		// Set the database reference for the transaction
		txn.db = memtable.db

		// Update our transaction map with the latest state of this transaction
		txnMap[txn.Id] = &txn

		// Only apply committed transactions to the memtable
		if txn.Committed {
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

	// Update the memtable size
	atomic.StoreInt64(&memtable.size, memtSize)

	if activeTxns != nil {
		for _, txn := range txnMap {
			// Only include uncommitted transactions that haven't been rolled back
			if !txn.Committed && len(txn.WriteSet) > 0 || len(txn.DeleteSet) > 0 || len(txn.ReadSet) > 0 {
				*activeTxns = append(*activeTxns, txn)
			}
		}
	}

	return nil
}

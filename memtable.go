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
	"fmt"
	"github.com/guycipher/wildcat/blockmanager"
	"github.com/guycipher/wildcat/bloomfilter"
	"github.com/guycipher/wildcat/skiplist"
	"os"
	"sync/atomic"
	"time"
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

	// Check if wal file in lru cache and add debug logging
	walQueueEntry, ok := memtable.db.lru.Get(memtable.wal.path)
	if !ok {
		memtable.db.log(fmt.Sprintf("WAL file not in LRU cache, opening: %s", memtable.wal.path))
		// Open the WAL file
		walBm, err = blockmanager.Open(memtable.wal.path, os.O_RDWR|os.O_CREATE, memtable.db.opts.Permission, blockmanager.SyncOption(memtable.db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add to LRU cache
		memtable.db.lru.Put(memtable.wal.path, walBm, func(key, value interface{}) {
			// Close the block manager when evicted from LRU
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})
	} else {
		memtable.db.log(fmt.Sprintf("Found WAL file in LRU cache: %s", memtable.wal.path))
		// Use the cached WAL file handle
		walBm = walQueueEntry.(*blockmanager.BlockManager)
	}

	iter := walBm.Iterator()

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

		var txn Txn
		err = txn.deserializeTransaction(data)
		if err != nil {
			memtable.db.log(fmt.Sprintf("Warning: failed to deserialize transaction: %v - skipping", err))
			continue
		}

		// Set the database reference
		txn.db = memtable.db

		// Check if we already have a transaction with this ID
		existingTxn, exists := txnMap[txn.Id]

		if !exists {
			// New transaction, just add it to the map
			txnCopy := txn // Make a copy
			txnMap[txn.Id] = &txnCopy
		} else {
			// Merge this transaction entry with the existing one
			for key, value := range txn.WriteSet {
				existingTxn.WriteSet[key] = value
			}
			for key := range txn.DeleteSet {
				existingTxn.DeleteSet[key] = true
			}
			for key, timestamp := range txn.ReadSet {
				existingTxn.ReadSet[key] = timestamp
			}

			// Update commit status - a transaction is committed if any entry says it is
			if txn.Committed {
				existingTxn.Committed = true
				existingTxn.Timestamp = txn.Timestamp // Use the timestamp from the commit entry
			}
		}
	}

	// After processing all entries, apply the committed transactions
	for _, txn := range txnMap {
		if txn.Committed {
			committedCount++

			// Apply writes to the memtable
			for key, value := range txn.WriteSet {
				memtable.skiplist.Put([]byte(key), value, txn.Timestamp)
				atomic.AddInt64(&memtable.size, int64(len(key)+len(value)))
			}

			// Apply deletes
			for key := range txn.DeleteSet {
				memtable.skiplist.Delete([]byte(key), txn.Timestamp)
				atomic.AddInt64(&memtable.size, -int64(len(key)))
			}
		}
	}

	// Collect active transactions if requested
	if activeTxns != nil {
		for _, txn := range txnMap {

			if !txn.Committed && (len(txn.WriteSet) > 0 || len(txn.DeleteSet) > 0 || len(txn.ReadSet) > 0) {
				txnCopy := *txn // Make a copy to prevent modification issues
				*activeTxns = append(*activeTxns, &txnCopy)
			}
		}
	}

	memtable.db.log(fmt.Sprintf("Replay summary for %s: %d total entries, %d unique transactions, %d committed",
		memtable.wal.path, txnCount, len(txnMap), committedCount))

	return nil
}

// Creates a bloom filter from skiplist
func (memtable *Memtable) createBloomFilter(entries int64) (*bloomfilter.BloomFilter, error) {
	maxPossibleTs := time.Now().UnixNano() + 10000000000 // Far in the future
	iter, err := memtable.skiplist.NewIterator(nil, maxPossibleTs)
	if err != nil {
		return nil, err
	}

	bf, err := bloomfilter.New(uint(entries), memtable.db.opts.BloomFilterFPR)
	if err != nil {
		return nil, err
	}

	for {
		key, val, _, ok := iter.Next()
		if !ok {
			break
		}

		if val == nil {
			continue // Skip deletion markers
		}

		err = bf.Add(key)
		if err != nil {
			// We log a warning
			memtable.db.log(fmt.Sprintf("Warning: failed to add key to Bloom filter: %v - skipping", err))
			continue
		}
	}

	return bf, nil

}

package wildcat

import (
	"fmt"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/bloomfilter"
	"github.com/wildcatdb/wildcat/v2/skiplist"
	"os"
	"sync/atomic"
	"time"
)

// A memtable contains a skiplist and a write-ahead log (WAL) for durability, they are paired.

// WAL is a write-ahead log structure
type WAL struct {
	path string // The WAL path i.e <db dir><id>.wal
}

// Memtable is a memory table structure
type Memtable struct {
	id       int64              // Takes from wal id
	skiplist *skiplist.SkipList // The skip list for the memtable, is atomic and concurrent safe
	wal      *WAL               // The write-ahead log for durability, is also atomic and concurrent safe
	size     int64              // Atomic size of the memtable
	db       *DB                // The database instance
}

// replay replays the WAL to recover the memtable
func (memtable *Memtable) replay(activeTxns *[]*Txn) error {
	var walBm *blockmanager.BlockManager
	var err error

	memtable.db.log(fmt.Sprintf("Replaying WAL for memtable: %s", memtable.wal.path))

	walQueueEntry, ok := memtable.db.lru.Get(memtable.wal.path)
	if !ok {
		memtable.db.log(fmt.Sprintf("WAL file not in LRU cache, opening: %s", memtable.wal.path))
		walBm, err = blockmanager.Open(memtable.wal.path, os.O_RDWR|os.O_CREATE, memtable.db.opts.Permission, blockmanager.SyncOption(memtable.db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		memtable.db.lru.Put(memtable.wal.path, walBm, func(key string, value interface{}) {
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})
	} else {
		memtable.db.log(fmt.Sprintf("Found WAL file in LRU cache: %s", memtable.wal.path))
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

// createBloomFilter Creates a bloom filter from skiplist
func (memtable *Memtable) createBloomFilter(entries int64) (*bloomfilter.BloomFilter, error) {
	maxPossibleTs := time.Now().UnixNano() + 10000000000 // Far in the future
	iter, err := memtable.skiplist.NewIterator(nil, maxPossibleTs)
	if err != nil {
		return nil, err
	}

	memtable.db.log(fmt.Sprintf("Creating Bloom filter for memtable with %d entries", entries))

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
			continue // Skip deletion markers (tombstones)
		}

		err = bf.Add(key)
		if err != nil {
			// We log a warning
			memtable.db.log(fmt.Sprintf("Warning: failed to add key to Bloom filter: %v - skipping", err))
			continue
		}
	}

	memtable.db.log(fmt.Sprintf("Bloom filter created for memtable with %d entries", entries))

	return bf, nil

}

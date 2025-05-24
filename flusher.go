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
	"github.com/guycipher/wildcat/queue"
	"github.com/guycipher/wildcat/skiplist"
	"os"
	"sync/atomic"
	"time"
)

// Flusher is responsible for queuing and flushing memtables to disk
type Flusher struct {
	db *DB // The db instance

	immutable *queue.Queue // Immutable queue for memtables
	swapping  int32        // Atomic flag indicating if the flusher is swapping
}

// newFlusher creates a new Flusher instance
func newFlusher(db *DB) *Flusher {
	return &Flusher{
		db:        db,
		immutable: queue.New(),
	}
}

// queueMemtable queues the current active memtable for flushing to disk.
func (flusher *Flusher) queueMemtable() error {

	// Check if the flusher is already swapping
	if atomic.LoadInt32(&flusher.swapping) == 1 {
		return nil // Already swapping, no need to queue again
	}

	// Set the swapping flag to indicate that we are in the process of swapping
	atomic.StoreInt32(&flusher.swapping, 1)
	defer atomic.StoreInt32(&flusher.swapping, 0)

	walId := flusher.db.walIdGenerator.nextID()
	// Create a new memtable
	newMemtable := &Memtable{
		db:       flusher.db,
		skiplist: skiplist.New(),
		wal: &WAL{
			path: fmt.Sprintf("%s%d%s", flusher.db.opts.Directory, walId, WALFileExtension),
		}}

	// Open the new WAL
	walBm, err := blockmanager.Open(newMemtable.wal.path, os.O_RDWR|os.O_CREATE, flusher.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption), flusher.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open WAL block manager: %w", err)
	}

	// Add the new WAL to the LRU cache
	flusher.db.lru.Put(newMemtable.wal.path, walBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	// Push the current memtable to the immutable stack
	flusher.immutable.Enqueue(flusher.db.memtable.Load().(*Memtable))

	// Update the current memtable to the new one
	flusher.db.memtable.Store(newMemtable)
	return nil
}

// backgroundProcess starts the background process for flushing memtables
func (flusher *Flusher) backgroundProcess() {
	defer flusher.db.wg.Done()
	ticker := time.NewTicker(FlusherTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-flusher.db.closeCh:
			flusher.db.log("Flusher: shutting down background process")
			return
		case <-ticker.C:
			immutableMemt := flusher.immutable.Dequeue()
			if immutableMemt == nil {
				continue // No immutable memtable to flush
			}

			flusher.db.log(fmt.Sprintf("Flusher: flushing immutable memtable %s", immutableMemt.(*Memtable).wal.path))

			// Flush the immutable memtable to disk
			err := flusher.flushMemtable(immutableMemt.(*Memtable))
			if err != nil {
				continue
			}
		}
	}
}

// flushMemtable flushes a memtable to disk as an SSTable at level 1
func (flusher *Flusher) flushMemtable(memt *Memtable) error {
	maxTimestamp := time.Now().UnixNano() + 10000000000 // Far in the future
	entryCount := memt.skiplist.Count(maxTimestamp)
	deletionCount := memt.skiplist.DeleteCount(maxTimestamp)

	if entryCount == 0 && deletionCount == 0 {
		flusher.db.log("Skipping flush for empty memtable")
		return nil // Nothing to flush
	}
	// Create a new SSTable
	sstable := &SSTable{
		Id:    flusher.db.sstIdGenerator.nextID(),
		db:    flusher.db,
		Level: 1, // We always flush to level 1, L0 is active memtable
	}

	// Use max timestamp to ensure we get all keys when finding min/max
	maxPossibleTs := time.Now().UnixNano() + 10000000000 // Far in the future

	minKey, _, exists := memt.skiplist.GetMin(maxPossibleTs)
	if exists {
		sstable.Min = minKey
	}

	maxKey, _, exists := memt.skiplist.GetMax(maxPossibleTs)
	if exists {
		sstable.Max = maxKey
	}

	// Calculate the approx size of the memtable
	sstable.Size = atomic.LoadInt64(&memt.size)

	// Use max timestamp to get a count of all entries regardless of version
	sstable.EntryCount = memt.skiplist.Count(maxPossibleTs)

	// We create new sstable files (.klog and .vlog) here
	klogPath := fmt.Sprintf("%s%s1%s%s%d%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s1%s%s%d%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, memt.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption), flusher.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, memt.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption), flusher.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	if flusher.db.opts.BloomFilter {
		// Create a bloom filter for the SSTable
		sstable.BloomFilter, err = memt.createBloomFilter(int64(entryCount))
		if err != nil {
			return fmt.Errorf("failed to create bloom filter: %w", err)

		}

	}

	// Encode metadata
	sstableData, err := sstable.serializeSSTable()
	if err != nil {
		return fmt.Errorf("failed to serialize SSTable: %w", err)
	}

	// Write first block to KLog
	_, err = klogBm.Append(sstableData)
	if err != nil {
		return fmt.Errorf("failed to write KLog: %w", err)
	}

	blockset := &BlockSet{
		Entries: make([]*KLogEntry, 0),
		Size:    0,
	}

	// Use the maximum possible timestamp to make sure we get ALL versions
	// of keys during iteration, preserving their original transaction timestamps
	iter := memt.skiplist.NewIterator(nil, maxPossibleTs)

	flusher.db.log(fmt.Sprintf("Starting to flush memtable to SSTable %d", sstable.Id))

	for {
		key, value, ts, ok := iter.Next()
		if !ok {
			break // No more entries
		}

		// Check if this is a deletion marker
		if value == nil {
			// Write a deletion marker to the SSTable
			klogEntry := &KLogEntry{
				Key:          key,
				Timestamp:    ts,
				ValueBlockID: -1, // Special marker for deletion
			}

			blockset.Entries = append(blockset.Entries, klogEntry)
			blockset.Size += int64(len(key))
		} else {
			// Normal entry - handle as before
			id, err := vlogBm.Append(value[:])
			if err != nil {
				return fmt.Errorf("failed to write VLog: %w", err)
			}

			klogEntry := &KLogEntry{
				Key:          key,
				Timestamp:    ts,
				ValueBlockID: id,
			}

			blockset.Entries = append(blockset.Entries, klogEntry)
			blockset.Size += int64(len(key) + len(value))
		}

		// Check if we need to flush this blockset
		if blockset.Size >= flusher.db.opts.BlockSetSize {
			// Write the blockset to KLog
			blocksetData, err := blockset.serializeBlockSet()
			if err != nil {
				return fmt.Errorf("failed to serialize BlockSet: %w", err)
			}

			_, err = klogBm.Append(blocksetData)
			if err != nil {
				return fmt.Errorf("failed to write BlockSet to KLog: %w", err)
			}

			// Reset the blockset for next iteration
			blockset = &BlockSet{
				Entries: make([]*KLogEntry, 0),
				Size:    0,
			}
		}
	}

	// Write any remaining blockset to KLog
	if len(blockset.Entries) > 0 {
		blocksetData, err := blockset.serializeBlockSet()
		if err != nil {
			return fmt.Errorf("failed to serialize BlockSet: %w", err)
		}

		_, err = klogBm.Append(blocksetData)
		if err != nil {
			return fmt.Errorf("failed to write BlockSet to KLog: %w", err)
		}

	}

	// Add both KLog and VLog to the LRU cache
	flusher.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})
	flusher.db.lru.Put(vlogPath, vlogBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	// Add the SSTable to level 1
	levels := flusher.db.levels.Load()
	if levels == nil {
		return fmt.Errorf("levels not initialized")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	var sstablesList []*SSTable

	if sstables != nil {
		sstablesList = *sstables
	} else {
		sstablesList = make([]*SSTable, 0)
	}

	sstablesList = append(sstablesList, sstable)

	level1.sstables.Store(&sstablesList)

	// Update the current size of the level
	atomic.AddInt64(&level1.currentSize, sstable.Size)

	// Delete original memtable wal
	_ = os.Remove(memt.wal.path) // Could be no wal

	flusher.db.log(fmt.Sprintf("SSTable %d added to level 1, min: %s, max: %s, entries: %d",
		sstable.Id, string(sstable.Min), string(sstable.Max), entryCount))

	return nil
}

// enqueueMemtable enqueues an immutable memtable for flushing
func (flusher *Flusher) enqueueMemtable(memt *Memtable) {

	// Add the immutable memtable to the queue
	flusher.immutable.Enqueue(memt)

}

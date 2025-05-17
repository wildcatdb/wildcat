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
	"orindb/queue"
	"orindb/skiplist"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Flusher is responsible for queuing and flushing memtables to disk
type Flusher struct {
	db        *DB          // The db instance
	lock      *sync.Mutex  // Mutex for thread safety
	immutable *queue.Queue // Immutable queue for memtables
}

// newFlusher creates a new Flusher instance
func newFlusher(db *DB) *Flusher {
	return &Flusher{
		db:        db,
		lock:      &sync.Mutex{},
		immutable: queue.New(),
	}
}

// queueMemtable queues the current active memtable for flushing to disk.
func (flusher *Flusher) queueMemtable() error {
	walId := flusher.db.walIdGenerator.nextID()
	// Create a new memtable
	newMemtable := &Memtable{
		db:       flusher.db,
		skiplist: skiplist.New(),
		wal: &WAL{
			path: fmt.Sprintf("%s%s%d%s", flusher.db.opts.Directory, string(os.PathSeparator), walId, WALFileExtension),
		}}

	// Open the new WAL
	walBm, err := blockmanager.Open(newMemtable.wal.path, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(flusher.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open WAL block manager: %w", err)
	}

	// Add the new WAL to the LRU cache
	flusher.db.lru.Put(newMemtable.wal.path, walBm)

	// Push the current memtable to the immutable stack
	flusher.immutable.Enqueue(flusher.db.memtable.Load().(*Memtable))

	// Reset the current memtable size
	atomic.StoreInt64(&flusher.db.memtable.Load().(*Memtable).size, 0)

	// Update the current memtable to the new one
	flusher.db.memtable.Store(newMemtable)
	return nil
}

// backgroundProcess starts the background process for flushing memtables
func (flusher *Flusher) backgroundProcess() {
	defer flusher.db.wg.Done()
	ticker := time.NewTicker(time.Millisecond * 24)
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
	// Create a new SSTable
	sstable := &SSTable{
		Id:    flusher.db.sstIdGenerator.nextID(),
		db:    flusher.db,
		Level: 1, // We always flush to level 1, L0 is active memtable
	}

	minKey, _, exists := memt.skiplist.GetMin(time.Now().UnixMicro())
	if exists {
		sstable.Min = minKey
	}

	maxKey, _, exists := memt.skiplist.GetMax(time.Now().UnixNano())
	if exists {
		sstable.Max = maxKey
	}

	// Calculate the approx size of the memtable
	sstable.Size = atomic.LoadInt64(&memt.size)
	sstable.EntryCount = memt.skiplist.Count(time.Now().UnixNano())

	// We create new sstable files (.klog and .vlog) here
	klogPath := fmt.Sprintf("%s%s1%s%s%d%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s1%s%s%d%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(flusher.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(flusher.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
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

	// Now we create a memtable iter
	iter := memt.skiplist.NewIterator(nil, time.Now().UnixNano())
	for {
		key, value, ts, ok := iter.Next()
		if !ok {
			break // No more entries
		}

		// Write the key-value pair to the VLog
		id, err := vlogBm.Append(value.([]byte)[:])
		if err != nil {
			return fmt.Errorf("failed to write VLog: %w", err)
		}

		klogEntry := &KLogEntry{
			Key:          key,
			Timestamp:    ts,
			ValueBlockID: id,
		}

		blockset.Entries = append(blockset.Entries, klogEntry)
		blockset.Size += int64(len(key) + len(value.([]byte)))

		sstable.EntryCount++

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

			blockset.Entries = make([]*KLogEntry, 0)
			blockset.Size = 0
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
	flusher.db.lru.Put(klogPath, klogBm)
	flusher.db.lru.Put(vlogPath, vlogBm)

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

	return nil
}

// enqueueMemtable enqueues an immutable memtable for flushing
func (flusher *Flusher) enqueueMemtable(memt *Memtable) {
	flusher.lock.Lock()
	defer flusher.lock.Unlock()

	// Add the immutable memtable to the queue
	flusher.immutable.Enqueue(memt)

}

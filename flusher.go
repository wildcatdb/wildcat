package wildcat

import (
	"fmt"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/queue"
	"github.com/wildcatdb/wildcat/v2/skiplist"
	"github.com/wildcatdb/wildcat/v2/tree"
	"os"
	"sync/atomic"
	"time"
)

// Flusher is responsible for queuing and flushing memtables to disk
type Flusher struct {
	db        *DB                      // The db instance
	immutable *queue.Queue             // Immutable queue for memtables
	flushing  atomic.Pointer[Memtable] // Atomic pointer to the current flushing memtable
	swapping  int32                    // Atomic flag indicating if the flusher is swapping
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

	flusher.db.log("Flusher: queuing current memtable for flushing")

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

	// Push the current memtable to the immutable queue
	flusher.immutable.Enqueue(flusher.db.memtable.Load().(*Memtable))

	flusher.db.log(fmt.Sprintf("Flusher: new active memtable created with WAL %s", newMemtable.wal.path))

	// Update the current memtable to the new one
	flusher.db.memtable.Store(newMemtable)

	return nil
}

// backgroundProcess starts the background process for flushing memtables
func (flusher *Flusher) backgroundProcess() {
	defer flusher.db.wg.Done()
	ticker := time.NewTicker(flusher.db.opts.FlusherTickerInterval)
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

			// Set the flushing memtable
			flusher.flushing.Store(immutableMemt.(*Memtable))

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

	// We defer clearing db.flusher.flushing
	defer func() {
		flusher.flushing.Store(nil)
	}()

	flusher.db.log(fmt.Sprintf("Flushing memtable with %d entries and %d deletions", entryCount, deletionCount))

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

	// Min and max keys are for sstable metadata
	minKey, _, exists := memt.skiplist.GetMin(maxPossibleTs)
	if exists {
		sstable.Min = minKey
	}

	maxKey, _, exists := memt.skiplist.GetMax(maxPossibleTs)
	if exists {
		sstable.Max = maxKey
	}

	latestTs := memt.skiplist.GetLatestTimestamp() // For compactor awareness

	// Calculate the approx size of the memtable
	sstable.Size = atomic.LoadInt64(&memt.size)

	// Use max timestamp to get a count of all entries regardless of version
	sstable.EntryCount = memt.skiplist.Count(maxPossibleTs)
	sstable.Timestamp = latestTs

	// We create new sstable files (.klog and .vlog) here

	// We have a temp and final path
	// We use a temp path in case of system crash
	// When we reopen the system we can check if the temp file exists, if so we delete it
	// This would be a flush that was not finalized thus an existing WAL exists and possibly corrupt levels
	vlogTmpPath := fmt.Sprintf("%s%s1%s%s%d%s%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, VLogExtension, TempFileExtension)
	vlogFinalPath := fmt.Sprintf("%s%s1%s%s%d%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, VLogExtension)

	klogTmpPath := fmt.Sprintf("%s%s1%s%s%d%s%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, KLogExtension, TempFileExtension)
	klogFinalPath := fmt.Sprintf("%s%s1%s%s%d%s", flusher.db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, KLogExtension)

	// Klog stores an immutable btree, vlog stores the values
	klogBm, err := blockmanager.Open(klogTmpPath, os.O_RDWR|os.O_CREATE, memt.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption), flusher.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogTmpPath, os.O_RDWR|os.O_CREATE, memt.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption), flusher.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// We create a new bloom filter if enabled and add it to sstable meta
	if flusher.db.opts.BloomFilter {

		// Create a bloom filter for the SSTable
		sstable.BloomFilter, err = memt.createBloomFilter(int64(entryCount))
		if err != nil {
			return fmt.Errorf("failed to create bloom filter: %w", err)

		}

	}

	// Create a BTree for the KLog
	t, err := tree.Open(klogBm, flusher.db.opts.SSTableBTreeOrder, sstable)
	if err != nil {
		return fmt.Errorf("failed to create BTree: %w", err)
	}

	// Use the maximum possible timestamp to make sure we get ALL versions
	// of keys during iteration, preserving their original transaction timestamps
	iter, err := memt.skiplist.NewIterator(nil, maxPossibleTs)
	if err != nil {
		return fmt.Errorf("failed to create iterator for flush: %w", err)
	}

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

			err = t.Put(key, klogEntry) // Insert deletion marker into B-tree
			if err != nil {
				return fmt.Errorf("failed to insert deletion marker into B-tree: %w", err)
			}
		} else {
			// Viewable value, write it to the VLog
			id, err := vlogBm.Append(value[:])
			if err != nil {
				return fmt.Errorf("failed to write VLog: %w", err)
			}

			klogEntry := &KLogEntry{
				Key:          key,
				Timestamp:    ts,
				ValueBlockID: id,
			}

			// Insert the KLog entry into the B-tree
			err = t.Put(key, klogEntry)
			if err != nil {
				return fmt.Errorf("failed to insert KLog entry into B-tree: %w", err)
			}
		}

	}

	flusher.db.log(fmt.Sprintf("Finished flushing memtable to SSTable %d", sstable.Id))

	// Now we close the klog and vlog temp files and rename them
	// This means the files are finalized
	_ = klogBm.Close()
	err = os.Rename(klogTmpPath, klogFinalPath)
	if err != nil {
		return fmt.Errorf("failed to rename KLog file: %w", err)
	}

	_ = vlogBm.Close()
	err = os.Rename(vlogTmpPath, vlogFinalPath)
	if err != nil {
		return fmt.Errorf("failed to rename VLog file: %w", err)
	}

	// Delete original memtable wal
	_ = os.Remove(memt.wal.path)

	flusher.db.log(fmt.Sprintf("SSTable %d flushed successfully, and finalized KLog: %s, VLog: %s", sstable.Id, klogFinalPath, vlogFinalPath))

	// Reopen the KLog and VLog block managers with final paths
	klogBm, err = blockmanager.Open(klogFinalPath, os.O_RDONLY, flusher.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err = blockmanager.Open(vlogFinalPath, os.O_RDONLY, flusher.db.opts.Permission, blockmanager.SyncOption(flusher.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// Add both KLog and VLog to the LRU cache
	flusher.db.lru.Put(klogFinalPath, klogBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})
	flusher.db.lru.Put(vlogFinalPath, vlogBm, func(key, value interface{}) {
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

	flusher.db.log(fmt.Sprintf("SSTable %d added to level 1, min: %s, max: %s, entries: %d",
		sstable.Id, string(sstable.Min), string(sstable.Max), entryCount))

	return nil
}

// enqueueMemtable enqueues an immutable memtable for flushing
func (flusher *Flusher) enqueueMemtable(memt *Memtable) {

	// Add the immutable memtable to the queue
	flusher.immutable.Enqueue(memt)

}

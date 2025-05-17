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
	"container/heap"
	"orindb/blockmanager"
	"orindb/skiplist"
	"os"
	"sort"
)

// MergeIterator is a bidirectional iterator that merges results from multiple sources
// while maintaining transaction isolation at the given read timestamp
type MergeIterator struct {
	txn            *Txn                 // Transaction for isolation
	memtableIter   *skiplist.Iterator   // Iterator for current memtable
	immutableIters []*skiplist.Iterator // Iterators for immutable memtables
	sstableIters   [][]*SSTableIterator // Iterators for SSTables by level

	// Current position data
	current       *iteratorHeapItem // Current item
	lastDirection int               // Last direction of iteration (1 for forward, -1 for backward)

	// Heap for merging
	minHeap minHeapItems // Min heap for Next operations
	maxHeap maxHeapItems // Max heap for Prev operations

	// Track seen keys to avoid duplicates (MVCC)
	seenKeys map[string]bool
}

// iteratorHeapItem represents an item in the iterator heaps
type iteratorHeapItem struct {
	key       []byte      // Key of the item
	value     interface{} // Value of the item
	source    int         // Source identifier (0=memtable, 1=immutable, 2+=sstables)
	sourceIdx int         // Index within the source type
	timestamp int64       // Timestamp of the item
}

// SSTableIterator is an iterator for a specific SSTable
type SSTableIterator struct {
	sstable *SSTable
	readTs  int64
	db      *DB
	klogBm  *blockmanager.BlockManager
	vlogBm  *blockmanager.BlockManager

	// Current state
	currentSet *BlockSet
	currentIdx int
	currentKey []byte

	// For bidirectional traversal
	history     []blockIterState
	initialized bool
	finished    bool
}

// blockIterState tracks state for bidirectional traversal
type blockIterState struct {
	blockID   int64
	setIdx    int
	key       []byte
	timestamp int64
	value     interface{}
}

// minHeapItems implements heap.Interface for min heap (Next operations)
type minHeapItems []*iteratorHeapItem

func (h minHeapItems) Len() int { return len(h) }
func (h minHeapItems) Less(i, j int) bool {
	// First compare keys
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// For same keys, newer versions come first (higher timestamp)
	return h[i].timestamp > h[j].timestamp
}
func (h minHeapItems) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeapItems) Push(x interface{}) { *h = append(*h, x.(*iteratorHeapItem)) }
func (h *minHeapItems) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// maxHeapItems implements heap.Interface for max heap (Prev operations)
type maxHeapItems []*iteratorHeapItem

func (h maxHeapItems) Len() int { return len(h) }
func (h maxHeapItems) Less(i, j int) bool {
	// First compare keys in reverse order
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp > 0 // Note the > instead of < for max heap
	}
	// For same keys, newer versions come first (higher timestamp)
	return h[i].timestamp > h[j].timestamp
}
func (h maxHeapItems) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeapItems) Push(x interface{}) { *h = append(*h, x.(*iteratorHeapItem)) }
func (h *maxHeapItems) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewMergeIterator creates a new iterator that merges all sources
// startKey can be nil to start from the beginning
func (txn *Txn) NewMergeIterator(startKey []byte) *MergeIterator {
	iter := &MergeIterator{
		txn:           txn,
		minHeap:       make(minHeapItems, 0),
		maxHeap:       make(maxHeapItems, 0),
		seenKeys:      make(map[string]bool),
		lastDirection: 0, // No direction yet
	}

	// Initialize the heap
	heap.Init(&iter.minHeap)
	heap.Init(&iter.maxHeap)

	// Initialize memtable iterator
	iter.memtableIter = txn.db.memtable.Load().(*Memtable).skiplist.NewIterator(startKey, txn.Timestamp)

	// Initialize immutable memtable iterators
	immutableMemtables := txn.db.immutable.List()
	iter.immutableIters = make([]*skiplist.Iterator, len(immutableMemtables))
	for i, memt := range immutableMemtables {
		if memt == nil {
			continue
		}
		iter.immutableIters[i] = memt.(*Memtable).skiplist.NewIterator(startKey, txn.Timestamp)
	}

	// Initialize SSTable iterators by level
	levels := txn.db.levels.Load()
	if levels != nil {
		iter.sstableIters = make([][]*SSTableIterator, len(*levels))

		for levelIdx, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}

			iter.sstableIters[levelIdx] = make([]*SSTableIterator, len(*sstables))
			for sstIdx, sst := range *sstables {
				// Skip SSTables that don't contain the key range we're looking for
				if startKey != nil && bytes.Compare(startKey, sst.Min) < 0 {
					continue
				}
				if startKey != nil && bytes.Compare(startKey, sst.Max) > 0 {
					continue
				}

				iter.sstableIters[levelIdx][sstIdx] = newSSTableIterator(sst, txn.Timestamp)
			}
		}
	}

	// Prime the iterators
	iter.initHeaps()

	return iter
}

// initHeaps initializes both min and max heaps with the first elements from each iterator
func (iter *MergeIterator) initHeaps() {
	// Clear existing heaps
	iter.minHeap = make(minHeapItems, 0)
	iter.maxHeap = make(maxHeapItems, 0)
	iter.seenKeys = make(map[string]bool)

	// Add first items from memtable iterator
	key, value, ts, ok := iter.memtableIter.Next()
	if ok {
		item := &iteratorHeapItem{
			key:       key,
			value:     value,
			timestamp: ts,
			source:    0, // Memtable source
			sourceIdx: 0,
		}
		heap.Push(&iter.minHeap, item)
		heap.Push(&iter.maxHeap, item)
	}

	// Add first items from immutable memtable iterators
	for i, immutableIter := range iter.immutableIters {
		if immutableIter == nil {
			continue
		}

		key, value, ts, ok := immutableIter.Next()
		if ok {
			item := &iteratorHeapItem{
				key:       key,
				value:     value,
				timestamp: ts,
				source:    1, // Immutable memtable source
				sourceIdx: i,
			}
			heap.Push(&iter.minHeap, item)
			heap.Push(&iter.maxHeap, item)
		}
	}

	// Add first items from SSTable iterators
	for levelIdx, levelIters := range iter.sstableIters {
		for sstIdx, sstIter := range levelIters {
			if sstIter == nil {
				continue
			}

			key, value, ts, ok := sstIter.Next()
			if ok {
				item := &iteratorHeapItem{
					key:       key,
					value:     value,
					timestamp: ts,
					source:    2 + levelIdx, // SSTable source (level + 2)
					sourceIdx: sstIdx,
				}
				heap.Push(&iter.minHeap, item)
				heap.Push(&iter.maxHeap, item)
			}
		}
	}
}

// Next returns the next key-value pair in ascending order
func (iter *MergeIterator) Next() ([]byte, interface{}, bool) {
	// If we were going backward, we need to reset and reinitialize
	if iter.lastDirection == -1 {
		iter.initHeaps()
	}
	iter.lastDirection = 1

	// Get items from the min heap until we find one that hasn't been seen
	// This ensures we only return one version of each key (MVCC)
	for iter.minHeap.Len() > 0 {
		item := heap.Pop(&iter.minHeap).(*iteratorHeapItem)
		keyStr := string(item.key)

		// Skip this key if we've seen it before
		if iter.seenKeys[keyStr] {
			// Get the next item from this source to replace what we just popped
			iter.advanceSourceForward(item.source, item.sourceIdx)
			continue
		}

		// Mark this key as seen
		iter.seenKeys[keyStr] = true
		iter.current = item

		// Get the next item from this source to replace what we just popped
		iter.advanceSourceForward(item.source, item.sourceIdx)

		// Check if this key is in our transaction's write set (would override anything from storage)
		if val, exists := iter.txn.WriteSet[keyStr]; exists {
			return item.key, val, true
		}

		// Check if this key is in the delete set
		if iter.txn.DeleteSet[keyStr] {
			// This key was deleted in the current transaction, skip it
			continue
		}

		return item.key, item.value, true
	}

	return nil, nil, false
}

// Prev returns the previous key-value pair in descending order
func (iter *MergeIterator) Prev() ([]byte, interface{}, bool) {
	// If we were going forward, we need to reset and reinitialize
	if iter.lastDirection == 1 {
		iter.initHeaps()
	}
	iter.lastDirection = -1

	// Get items from the max heap until we find one that hasn't been seen
	for iter.maxHeap.Len() > 0 {
		item := heap.Pop(&iter.maxHeap).(*iteratorHeapItem)
		keyStr := string(item.key)

		// Skip this key if we've seen it before
		if iter.seenKeys[keyStr] {
			// Get the previous item from this source to replace what we just popped
			iter.advanceSourceBackward(item.source, item.sourceIdx)
			continue
		}

		// Mark this key as seen
		iter.seenKeys[keyStr] = true
		iter.current = item

		// Get the previous item from this source to replace what we just popped
		iter.advanceSourceBackward(item.source, item.sourceIdx)

		// Check if this key is in our transaction's write set (would override anything from storage)
		if val, exists := iter.txn.WriteSet[keyStr]; exists {
			return item.key, val, true
		}

		// Check if this key is in the delete set
		if iter.txn.DeleteSet[keyStr] {
			// This key was deleted in the current transaction, skip it
			continue
		}

		return item.key, item.value, true
	}

	return nil, nil, false
}

// advanceSourceForward advances the specified source and adds the next item to the min heap
func (iter *MergeIterator) advanceSourceForward(source int, sourceIdx int) {
	var key []byte
	var value interface{}
	var ts int64
	var ok bool

	// Get the next item from the appropriate source
	if source == 0 {
		// Memtable
		key, value, ts, ok = iter.memtableIter.Next()
	} else if source == 1 {
		// Immutable memtable
		if sourceIdx < len(iter.immutableIters) && iter.immutableIters[sourceIdx] != nil {
			key, value, ts, ok = iter.immutableIters[sourceIdx].Next()
		}
	} else {
		// SSTable
		levelIdx := source - 2
		if levelIdx < len(iter.sstableIters) &&
			sourceIdx < len(iter.sstableIters[levelIdx]) &&
			iter.sstableIters[levelIdx][sourceIdx] != nil {
			key, value, ts, ok = iter.sstableIters[levelIdx][sourceIdx].Next()
		}
	}

	// If we got a valid item, add it to the min heap
	if ok {
		item := &iteratorHeapItem{
			key:       key,
			value:     value,
			timestamp: ts,
			source:    source,
			sourceIdx: sourceIdx,
		}
		heap.Push(&iter.minHeap, item)
	}
}

// advanceSourceBackward advances the specified source backward and adds the previous item to the max heap
func (iter *MergeIterator) advanceSourceBackward(source int, sourceIdx int) {
	var key []byte
	var value interface{}
	var ts int64
	var ok bool

	// Get the previous item from the appropriate source
	if source == 0 {
		// Memtable
		key, value, ts, ok = iter.memtableIter.Prev()
	} else if source == 1 {
		// Immutable memtable
		if sourceIdx < len(iter.immutableIters) && iter.immutableIters[sourceIdx] != nil {
			key, value, ts, ok = iter.immutableIters[sourceIdx].Prev()
		}
	} else {
		// SSTable
		levelIdx := source - 2
		if levelIdx < len(iter.sstableIters) &&
			sourceIdx < len(iter.sstableIters[levelIdx]) &&
			iter.sstableIters[levelIdx][sourceIdx] != nil {
			key, value, ts, ok = iter.sstableIters[levelIdx][sourceIdx].Prev()
		}
	}

	// If we got a valid item, add it to the max heap
	if ok {
		item := &iteratorHeapItem{
			key:       key,
			value:     value,
			timestamp: ts,
			source:    source,
			sourceIdx: sourceIdx,
		}
		heap.Push(&iter.maxHeap, item)
	}
}

// Seek positions the iterator at the specified key or the next key if exact match not found
func (iter *MergeIterator) Seek(key []byte) ([]byte, interface{}, bool) {
	// Reinitialize with the new start key
	iter.memtableIter = iter.txn.db.memtable.Load().(*Memtable).skiplist.NewIterator(key, iter.txn.Timestamp)

	// Reinitialize immutable iterators
	for i, memt := range iter.txn.db.immutable.List() {
		if memt == nil {
			continue
		}
		iter.immutableIters[i] = memt.(*Memtable).skiplist.NewIterator(key, iter.txn.Timestamp)
	}

	// Reinitialize SSTable iterators
	levels := iter.txn.db.levels.Load()
	if levels != nil {
		for levelIdx, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}

			for sstIdx, sst := range *sstables {
				// Skip SSTables that don't contain the key range we're looking for
				if bytes.Compare(key, sst.Min) < 0 || bytes.Compare(key, sst.Max) > 0 {
					continue
				}

				if len(iter.sstableIters) > levelIdx && len(iter.sstableIters[levelIdx]) > sstIdx {
					iter.sstableIters[levelIdx][sstIdx] = newSSTableIterator(sst, iter.txn.Timestamp)
				}
			}
		}
	}

	// Reset and prime the heaps
	iter.initHeaps()
	iter.lastDirection = 1 // Set direction to forward

	// Return the first item
	return iter.Next()
}

// newSSTableIterator creates a new iterator for an SSTable
func newSSTableIterator(sst *SSTable, readTs int64) *SSTableIterator {
	// Create KLog and VLog paths
	klogPath := sst.db.getSSTablKLogPath(sst)
	vlogPath := sst.db.getSSTablVLogPath(sst)

	// Get or open the KLog block manager
	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil
		}
		sst.db.lru.Put(klogPath, klogBm)
	}

	// Get or open the VLog block manager
	var vlogBm *blockmanager.BlockManager

	if v, ok := sst.db.lru.Get(vlogPath); ok {
		vlogBm = v.(*blockmanager.BlockManager)
	} else {
		vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil
		}
		sst.db.lru.Put(vlogPath, vlogBm)
	}

	return &SSTableIterator{
		sstable: sst,
		readTs:  readTs,
		db:      sst.db,
		klogBm:  klogBm,
		vlogBm:  vlogBm,
		history: make([]blockIterState, 0),
	}
}

// Next returns the next key-value pair from the SSTable
func (iter *SSTableIterator) Next() ([]byte, interface{}, int64, bool) {
	// If we've finished, no more items
	if iter.finished {
		return nil, nil, 0, false
	}

	// If we're not initialized, get the first block of data
	if !iter.initialized {
		if !iter.initializeForward() {
			return nil, nil, 0, false
		}
	}

	// If we were previously going backward, we need to handle that special case
	if len(iter.history) > 0 {
		// Pop the last history item and return it
		lastState := iter.history[len(iter.history)-1]
		iter.history = iter.history[:len(iter.history)-1]

		// Update the current state
		iter.currentKey = lastState.key

		return lastState.key, lastState.value, lastState.timestamp, true
	}

	// Keep looking until we find a valid entry or exhaust the SSTable
	for {
		// If we've reached the end of the current block set, load the next one
		if iter.currentSet == nil || iter.currentIdx >= len(iter.currentSet.Entries) {
			if !iter.loadNextBlockSet() {
				iter.finished = true
				return nil, nil, 0, false
			}
			iter.currentIdx = 0
		}

		// Get the current entry
		entry := iter.currentSet.Entries[iter.currentIdx]
		iter.currentIdx++

		// Skip entries with timestamps after our read timestamp (MVCC)
		if entry.Timestamp > iter.readTs {
			continue
		}

		// Read the value from VLog
		value, _, err := iter.vlogBm.Read(entry.ValueBlockID)
		if err != nil {
			continue
		}

		// Update current key and save state to history for bidirectional traversal
		iter.currentKey = entry.Key
		iter.history = append(iter.history, blockIterState{
			key:       entry.Key,
			timestamp: entry.Timestamp,
			value:     value,
		})

		return entry.Key, value, entry.Timestamp, true
	}
}

// Prev returns the previous key-value pair from the SSTable
func (iter *SSTableIterator) Prev() ([]byte, interface{}, int64, bool) {
	// If we're not initialized, initialize backward
	if !iter.initialized {
		if !iter.initializeBackward() {
			return nil, nil, 0, false
		}
	}

	// If we have history items, use the most recent one
	if len(iter.history) > 0 {
		lastState := iter.history[len(iter.history)-1]
		iter.history = iter.history[:len(iter.history)-1]

		// Update the current state
		iter.currentKey = lastState.key

		return lastState.key, lastState.value, lastState.timestamp, true
	}

	// If we don't have history and we're going backward, we need to build it
	// This is complex and requires scanning the SSTable from the beginning up to
	// the current position, which is inefficient but necessary for bidirectional traversal
	// in SSTable format which is inherently forward-oriented

	// Reset to beginning and build history
	iter.initialized = false
	if !iter.buildHistoryUntil(iter.currentKey) {
		return nil, nil, 0, false
	}

	// Now try again with rebuilt history
	return iter.Prev()
}

// initializeForward initializes the iterator for forward traversal
func (iter *SSTableIterator) initializeForward() bool {
	// Skip the first block which contains SSTable metadata
	blockData, _, err := iter.klogBm.Read(1)
	if err != nil {
		return false
	}

	// Get the second block which should be the first block set
	blockData, _, err = iter.klogBm.Read(2)
	if err != nil {
		return false
	}

	// Deserialize the block set
	var blockset BlockSet
	err = blockset.deserializeBlockSet(blockData)
	if err != nil {
		return false
	}

	iter.currentSet = &blockset
	iter.currentIdx = 0
	iter.initialized = true

	return true
}

// initializeBackward initializes the iterator for backward traversal
// This is more complex as we need to find the last valid entry
func (iter *SSTableIterator) initializeBackward() bool {
	// We need to read all blocks and build history from the beginning
	// then we'll traverse from the end

	// First, build complete history
	if !iter.buildCompleteHistory() {
		return false
	}

	// Mark as initialized
	iter.initialized = true

	// Sort history by key in descending order
	sort.Slice(iter.history, func(i, j int) bool {
		// First by key in descending order
		cmp := bytes.Compare(iter.history[i].key, iter.history[j].key)
		if cmp != 0 {
			return cmp > 0 // Note: > for descending
		}
		// For same key, newer timestamps first
		return iter.history[i].timestamp > iter.history[j].timestamp
	})

	// Deduplicate history to keep only the newest version of each key
	if len(iter.history) > 0 {
		dedupedHistory := make([]blockIterState, 0, len(iter.history))
		lastKey := iter.history[0].key
		dedupedHistory = append(dedupedHistory, iter.history[0])

		for i := 1; i < len(iter.history); i++ {
			if !bytes.Equal(iter.history[i].key, lastKey) {
				dedupedHistory = append(dedupedHistory, iter.history[i])
				lastKey = iter.history[i].key
			}
		}

		iter.history = dedupedHistory
	}

	return len(iter.history) > 0
}

// buildCompleteHistory reads all valid entries from the SSTable and builds complete history
func (iter *SSTableIterator) buildCompleteHistory() bool {
	// Skip the first block which contains SSTable metadata
	_, _, err := iter.klogBm.Read(1)
	if err != nil {
		return false
	}

	// Read all block sets
	blockIter := iter.klogBm.Iterator()

	// Skip first block (metadata)
	_, _, err = blockIter.Next()
	if err != nil {
		return false
	}

	// Process all blocks
	for {
		blockData, _, err := blockIter.Next()
		if err != nil {
			break // End of blocks
		}

		// Deserialize the block set
		var blockset BlockSet
		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			continue
		}

		// Process all entries in the block set
		for _, entry := range blockset.Entries {
			// Skip entries with timestamps after our read timestamp (MVCC)
			if entry.Timestamp > iter.readTs {
				continue
			}

			// Read the value from VLog
			value, _, err := iter.vlogBm.Read(entry.ValueBlockID)
			if err != nil {
				continue
			}

			// Add to history
			iter.history = append(iter.history, blockIterState{
				key:       entry.Key,
				timestamp: entry.Timestamp,
				value:     value,
			})
		}
	}

	return true
}

// buildHistoryUntil builds history up to the specified key
func (iter *SSTableIterator) buildHistoryUntil(targetKey []byte) bool {
	// Similar to buildCompleteHistory but stops when it reaches targetKey

	// Skip the first block which contains SSTable metadata
	_, _, err := iter.klogBm.Read(1)
	if err != nil {
		return false
	}

	// Read all block sets
	blockIter := iter.klogBm.Iterator()

	// Skip first block (metadata)
	_, _, err = blockIter.Next()
	if err != nil {
		return false
	}

	// Process all blocks
	for {
		blockData, _, err := blockIter.Next()
		if err != nil {
			break // End of blocks
		}

		// Deserialize the block set
		var blockset BlockSet
		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			continue
		}

		// Process all entries in the block set
		for _, entry := range blockset.Entries {
			// Stop if we've reached our target key
			if bytes.Compare(entry.Key, targetKey) >= 0 {
				// Sort history and deduplicate before returning
				sort.Slice(iter.history, func(i, j int) bool {
					// First by key in descending order
					cmp := bytes.Compare(iter.history[i].key, iter.history[j].key)
					if cmp != 0 {
						return cmp > 0 // Note: > for descending
					}
					// For same key, newer timestamps first
					return iter.history[i].timestamp > iter.history[j].timestamp
				})
				return true
			}

			// Skip entries with timestamps after our read timestamp (MVCC)
			if entry.Timestamp > iter.readTs {
				continue
			}

			// Read the value from VLog
			value, _, err := iter.vlogBm.Read(entry.ValueBlockID)
			if err != nil {
				continue
			}

			// Add to history
			iter.history = append(iter.history, blockIterState{
				key:       entry.Key,
				timestamp: entry.Timestamp,
				value:     value,
			})
		}
	}

	// If we get here, we've read all blocks
	// Sort history in descending order for Prev
	sort.Slice(iter.history, func(i, j int) bool {
		// First by key in descending order
		cmp := bytes.Compare(iter.history[i].key, iter.history[j].key)
		if cmp != 0 {
			return cmp > 0 // Note: > for descending
		}
		// For same key, newer timestamps first
		return iter.history[i].timestamp > iter.history[j].timestamp
	})

	return len(iter.history) > 0
}

// loadNextBlockSet loads the next block set from the SSTable
func (iter *SSTableIterator) loadNextBlockSet() bool {
	if iter.currentSet == nil {
		// First time loading, get the first data block (skip metadata)
		blockData, _, err := iter.klogBm.Read(2)
		if err != nil {
			return false
		}

		var blockset BlockSet
		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			return false
		}

		iter.currentSet = &blockset
		return true
	}

	// Get the next block in sequence
	blockIter := iter.klogBm.Iterator()

	// Skip blocks until we find where we were
	_, _, err := blockIter.Next() // Skip metadata
	if err != nil {
		return false
	}

	foundCurrent := false
	var blockset BlockSet

	for !foundCurrent {
		blockData, _, err := blockIter.Next()
		if err != nil {
			return false
		}

		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			continue
		}

		// Compare with current block set to see if we found it
		if len(blockset.Entries) > 0 && len(iter.currentSet.Entries) > 0 {
			if bytes.Equal(blockset.Entries[0].Key, iter.currentSet.Entries[0].Key) {
				foundCurrent = true
			}
		}
	}

	// Now get the next block
	blockData, _, err := blockIter.Next()
	if err != nil {
		return false
	}

	err = blockset.deserializeBlockSet(blockData)
	if err != nil {
		return false
	}

	iter.currentSet = &blockset
	return true
}

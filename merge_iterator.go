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
	"bytes"
	"fmt"
	"sync"
	"wildcat/skiplist"
)

// IteratorWithPeek is an interface for iterators that support peeking
type IteratorWithPeek interface {
	Next() ([]byte, []byte, int64, bool)
	Prev() ([]byte, []byte, int64, bool)
	Peek() ([]byte, []byte, int64, bool)
}

// IteratorItem represents an item in the merge iterator heap
type IteratorItem struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Iter      IteratorWithPeek
}

// MergeIterator combines results from multiple sources (memtables, immutable memtables, SSTables)
// in a sorted order based on key and timestamp
type MergeIterator struct {
	heap       []*IteratorItem
	txn        *Txn
	comparator skiplist.KeyComparator
	prefix     []byte
	mutex      sync.Mutex
}

// NewMergeIterator creates a new merge iterator that combines multiple iterators
func NewMergeIterator(txn *Txn, startKey []byte, prefix []byte) *MergeIterator {
	var iters []IteratorWithPeek

	// Add active memtable iterator
	mem := txn.db.memtable.Load().(*Memtable)
	memIter := mem.skiplist.NewIterator(startKey, txn.Timestamp)
	iters = append(iters, memIter)

	// Add immutable memtables iterators from the flusher queue
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		if memt, ok := item.(*Memtable); ok {
			immIter := memt.skiplist.NewIterator(startKey, txn.Timestamp)
			iters = append(iters, immIter)
		}
		return true
	})

	// Add all SSTables iterators from all levels
	levels := txn.db.levels.Load()
	for _, level := range *levels {
		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		for _, sst := range *sstables {
			// Skip empty SSTables
			if sst.EntryCount == 0 {
				continue
			}

			// Skip SSTable if we know the key is outside its range
			if len(startKey) > 0 && len(sst.Min) > 0 && len(sst.Max) > 0 {
				// Only skip if the key is definitely outside the range
				if bytes.Compare(startKey, sst.Max) > 0 {
					continue // Key is greater than max
				}
			}

			// Get iterator for this SSTable
			it := sst.iterator()
			if it == nil {
				txn.db.log(fmt.Sprintf("Failed to create iterator for SSTable %d", sst.Id))
				continue
			}

			// Wrap the iterator so it implements IteratorWithPeek
			wrapped := &SSTableIteratorWrapper{
				iter:      it,
				timestamp: txn.Timestamp,
				valid:     true,
			}

			// Try to seek to the start key if provided
			if len(startKey) > 0 {
				if err := it.Seek(startKey); err != nil {
					txn.db.log(fmt.Sprintf("Failed to seek in SSTable %d: %v", sst.Id, err))
				}
			}

			iters = append(iters, wrapped)
		}
	}

	// Create the merge iterator
	mi := &MergeIterator{
		heap:       make([]*IteratorItem, 0, len(iters)),
		txn:        txn,
		comparator: skiplist.DefaultComparator,
		prefix:     prefix,
		mutex:      sync.Mutex{},
	}

	for _, it := range iters {
		k, v, ts, ok := it.Next()
		if ok && ts <= txn.Timestamp && (prefix == nil || bytes.HasPrefix(k, prefix)) {
			mi.heap = append(mi.heap, &IteratorItem{
				Key:       k,
				Value:     v,
				Timestamp: ts,
				Iter:      it,
			})
		}

	}

	// Build min-heap based on keys
	heapify(mi.heap, mi.comparator)
	return mi
}

// Next returns the next key-value pair from the merged iterators
func (mi *MergeIterator) Next() ([]byte, []byte, int64, bool) {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()

	if len(mi.heap) == 0 {
		return nil, nil, 0, false
	}

	// Get the smallest key from the heap
	top := mi.heap[0]
	key := top.Key
	value := top.Value
	timestamp := top.Timestamp

	// Advance this iterator to its next value
	k, v, ts, ok := top.Iter.Next()

	// If the iterator has more values, update the heap
	if ok && ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {
		// Update the item with new values
		top.Key = k
		top.Value = v
		top.Timestamp = ts

		// Maintain heap property
		heapFixDown(mi.heap, 0, mi.comparator)
	} else {
		// Remove this iterator from the heap
		if len(mi.heap) > 1 {
			// Replace with the last element and fix heap
			mi.heap[0] = mi.heap[len(mi.heap)-1]
			mi.heap = mi.heap[:len(mi.heap)-1]
			heapFixDown(mi.heap, 0, mi.comparator)
		} else {
			// Just remove the last element
			mi.heap = mi.heap[:0]
		}
	}

	// Skip any duplicate keys (we only want the most recent version)
	// This is optional depending on your MVCC semantics
	mi.skipDuplicateKeys(key)

	return key, value, timestamp, true
}

// skipDuplicateKeys advances past any items with the same key as the one we just returned
func (mi *MergeIterator) skipDuplicateKeys(key []byte) {
	for len(mi.heap) > 0 && bytes.Equal(mi.heap[0].Key, key) {
		top := mi.heap[0]

		// Advance the iterator
		k, v, ts, ok := top.Iter.Next()

		if ok && ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {
			// Update heap item
			top.Key = k
			top.Value = v
			top.Timestamp = ts
			heapFixDown(mi.heap, 0, mi.comparator)
		} else {
			// Remove this iterator
			if len(mi.heap) > 1 {
				mi.heap[0] = mi.heap[len(mi.heap)-1]
				mi.heap = mi.heap[:len(mi.heap)-1]
				heapFixDown(mi.heap, 0, mi.comparator)
			} else {
				mi.heap = mi.heap[:0]
				break
			}
		}
	}
}

// Prev moves the iterator backward
func (mi *MergeIterator) Prev() ([]byte, []byte, int64, bool) {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()

	if len(mi.heap) == 0 {
		return nil, nil, 0, false
	}

	// Get the largest key from the heap
	top := mi.heap[0]
	key := top.Key
	value := top.Value
	timestamp := top.Timestamp

	// Move this iterator to its previous value
	k, v, ts, ok := top.Iter.Prev()

	// If the iterator has more values, update the heap
	if ok && ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {
		// Update the item with new values
		top.Key = k
		top.Value = v
		top.Timestamp = ts

		// Maintain heap property
		heapFixDown(mi.heap, 0, mi.comparator)
	} else {
		// Remove this iterator from the heap
		if len(mi.heap) > 1 {
			// Replace with the last element and fix heap
			mi.heap[0] = mi.heap[len(mi.heap)-1]
			mi.heap = mi.heap[:len(mi.heap)-1]
			heapFixDown(mi.heap, 0, mi.comparator)
		} else {
			// Just remove the last element
			mi.heap = mi.heap[:0]
		}
	}

	// Skip any duplicate keys (we only want the most recent version)
	mi.skipDuplicateKeys(key)

	return key, value, timestamp, true
}

// heapify builds a min-heap from an unordered array
func heapify(heap []*IteratorItem, cmp skiplist.KeyComparator) {
	n := len(heap)
	for i := n/2 - 1; i >= 0; i-- {
		heapFixDown(heap, i, cmp)
	}
}

// heapFixDown maintains heap property by sifting down
func heapFixDown(heap []*IteratorItem, i int, cmp skiplist.KeyComparator) {
	n := len(heap)
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < n && compare(heap[left], heap[smallest], cmp) < 0 {
			smallest = left
		}

		if right < n && compare(heap[right], heap[smallest], cmp) < 0 {
			smallest = right
		}

		if smallest == i {
			break
		}

		heap[i], heap[smallest] = heap[smallest], heap[i]
		i = smallest
	}
}

// compare compares two iterator items
func compare(a, b *IteratorItem, cmp skiplist.KeyComparator) int {
	// First compare by key
	keyComp := cmp(a.Key, b.Key)
	if keyComp != 0 {
		return keyComp
	}

	// For same key, higher timestamp (newer version) comes first
	if a.Timestamp > b.Timestamp {
		return -1
	} else if a.Timestamp < b.Timestamp {
		return 1
	}

	return 0
}

// SSTableIteratorWrapper adapts SSTableIterator to the IteratorWithPeek interface
type SSTableIteratorWrapper struct {
	iter      *SSTableIterator
	timestamp int64
	currKey   []byte
	currValue []byte
	currTs    int64
	hasPeeked bool
	valid     bool
}

// Peek returns the current key-value pair without advancing
func (s *SSTableIteratorWrapper) Peek() ([]byte, []byte, int64, bool) {
	if !s.valid {
		return nil, nil, 0, false
	}

	if s.hasPeeked {
		return s.currKey, s.currValue, s.currTs, true
	}

	// Get the next key-value pair
	k, v, ts, err := s.iter.next()
	if err != nil || ts > s.timestamp {
		s.valid = false
		return nil, nil, 0, false
	}

	// Save current state
	s.currKey = k
	s.currValue = v
	s.currTs = ts
	s.hasPeeked = true

	return k, v, ts, true
}

// Next advances the iterator and returns the next key-value pair
func (s *SSTableIteratorWrapper) Next() ([]byte, []byte, int64, bool) {
	if !s.valid {
		return nil, nil, 0, false
	}

	if s.hasPeeked {
		// Return saved values from previous peek
		s.hasPeeked = false
		return s.currKey, s.currValue, s.currTs, true
	}

	// Get next from underlying iterator
	k, v, ts, err := s.iter.next()
	if err != nil || ts > s.timestamp {
		s.valid = false
		return nil, nil, 0, false
	}

	return k, v, ts, true
}

// Prev moves to the previous key-value pair
func (s *SSTableIteratorWrapper) Prev() ([]byte, []byte, int64, bool) {
	if !s.valid {
		return nil, nil, 0, false
	}

	// Clear any peeked value
	if s.hasPeeked {
		s.hasPeeked = false
	}

	// Move to previous
	k, v, ts, err := s.iter.prev()
	if err != nil || ts > s.timestamp {
		s.valid = false
		return nil, nil, 0, false
	}

	return k, v, ts, true
}

func (it *SSTableIterator) Seek(key []byte) error {
	// Lock for thread safety
	it.lock.Lock()
	defer it.lock.Unlock()

	// Get the block manager
	klogBm := it.iterator.BlockManager()
	if klogBm == nil {
		return fmt.Errorf("invalid block manager")
	}

	// Create a new iterator starting from the beginning
	newIter := klogBm.Iterator()

	// Skip the metadata block
	_, _, err := newIter.Next()
	if err != nil {
		return fmt.Errorf("failed to skip metadata block: %w", err)
	}

	// Iterate through blocks looking for the target key
	for {
		// Read next block
		data, _, err := newIter.Next()
		if err != nil {
			break // End of blocks
		}

		// Parse the block set
		var blockSet BlockSet
		if err := blockSet.deserializeBlockSet(data); err != nil {
			continue // Skip corrupted block
		}

		// Check if any key in this block is >= the search key
		for i, entry := range blockSet.Entries {
			if bytes.Compare(entry.Key, key) >= 0 {
				// Found an entry >= our search key, update iterator state
				it.blockSet = blockSet
				it.blockSetIdx = int64(i)
				it.iterator = newIter
				return nil
			}
		}
	}

	// If no matching key is found, reset iterator to the beginning
	newIter = klogBm.Iterator()

	// Skip the metadata block
	_, _, err = newIter.Next()
	if err != nil {
		return fmt.Errorf("failed to reset iterator: %w", err)
	}

	// Read the first data block
	data, _, err := newIter.Next()
	if err != nil {
		return fmt.Errorf("no data blocks available: %w", err)
	}

	// Parse the block set
	var blockSet BlockSet
	if err := blockSet.deserializeBlockSet(data); err != nil {
		return fmt.Errorf("failed to parse block set: %w", err)
	}

	// Update iterator state to the start of data
	it.blockSet = blockSet
	it.blockSetIdx = 0
	it.iterator = newIter

	return nil
}

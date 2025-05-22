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
	"github.com/guycipher/wildcat/skiplist"
	"sync"
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
	lastKey    []byte
	atEnd      bool
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

// NewMergeIterator creates a new merge iterator that combines multiple iterators
func NewMergeIterator(txn *Txn, startKey []byte, prefix []byte) *MergeIterator {
	var iters []IteratorWithPeek

	// If we have a prefix but no startKey, use the prefix as the startKey
	// This helps the underlying iterators start from the right position
	effectiveStartKey := startKey
	if len(prefix) > 0 && (len(startKey) == 0 || bytes.Compare(prefix, startKey) > 0) {
		effectiveStartKey = prefix
	}

	// Add active memtable iterator
	mem := txn.db.memtable.Load().(*Memtable)
	memIter := mem.skiplist.NewIterator(effectiveStartKey, txn.Timestamp)
	iters = append(iters, memIter)

	// Add immutable memtables iterators from the flusher queue
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		if memt, ok := item.(*Memtable); ok {
			immIter := memt.skiplist.NewIterator(effectiveStartKey, txn.Timestamp)
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
			if len(effectiveStartKey) > 0 && len(sst.Min) > 0 && len(sst.Max) > 0 {

				// Only skip if the key is definitely outside the range
				if bytes.Compare(effectiveStartKey, sst.Max) > 0 {
					continue // Key is greater than max
				}
			}

			// If we have a prefix, also check if SSTable range could contain prefix matches
			if len(prefix) > 0 && len(sst.Min) > 0 && len(sst.Max) > 0 {

				// Skip if the prefix is completely outside the SSTable range
				if bytes.Compare(prefix, sst.Max) > 0 {
					continue // Prefix is greater than max
				}

				// Create a prefix upper bound by incrementing the last byte
				prefixUpperBound := make([]byte, len(prefix))
				copy(prefixUpperBound, prefix)
				if len(prefixUpperBound) > 0 {

					// Increment the last byte to create an upper bound
					for i := len(prefixUpperBound) - 1; i >= 0; i-- {
						if prefixUpperBound[i] < 255 {
							prefixUpperBound[i]++
							break
						}
						if i == 0 {
							// All bytes are 255, no upper bound possible..
							prefixUpperBound = nil
							break
						}
						prefixUpperBound[i] = 0
					}

					// Skip if the SSTable min is >= prefix upper bound
					if prefixUpperBound != nil && bytes.Compare(sst.Min, prefixUpperBound) >= 0 {
						continue
					}
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
			if len(effectiveStartKey) > 0 {
				if err := it.Seek(effectiveStartKey); err != nil {
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
		atEnd:      false,
	}

	// Initialize the heap with the first valid item from each iterator
	for _, it := range iters {
		// Keep advancing the iterator until we find a key that matches our prefix
		for {
			k, v, ts, ok := it.Next()
			if !ok {
				break // No more items in this iterator
			}

			// Check timestamp and prefix constraints
			if ts <= txn.Timestamp && (prefix == nil || bytes.HasPrefix(k, prefix)) {
				mi.heap = append(mi.heap, &IteratorItem{
					Key:       k,
					Value:     v,
					Timestamp: ts,
					Iter:      it,
				})
				break // Found a valid item, stop advancing this iterator
			}

			// If we have a prefix and this key is lexicographically greater than
			// any possible key with our prefix, we can stop this iterator
			if len(prefix) > 0 {

				// Create prefix upper bound
				prefixUpperBound := make([]byte, len(prefix))
				copy(prefixUpperBound, prefix)
				if len(prefixUpperBound) > 0 {
					// Increment the last byte to create an upper bound
					incremented := false
					for i := len(prefixUpperBound) - 1; i >= 0; i-- {
						if prefixUpperBound[i] < 255 {
							prefixUpperBound[i]++
							incremented = true
							break
						}
						if i == 0 {
							// All bytes are 255, no upper bound possible
							incremented = false
							break
						}
						prefixUpperBound[i] = 0
					}

					// If we found an upper bound and current key is >= upper bound, stop
					if incremented && bytes.Compare(k, prefixUpperBound) >= 0 {
						break
					}
				}
			}
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
		mi.atEnd = true
		return nil, nil, 0, false
	}

	// Reset atEnd flag since we're advancing
	mi.atEnd = false

	// Get the smallest key from the heap
	top := mi.heap[0]
	key := top.Key
	value := top.Value
	timestamp := top.Timestamp

	// Advance this iterator to its next valid value
	for {
		k, v, ts, ok := top.Iter.Next()

		if !ok {
			// No more items in this iterator, remove it from heap
			if len(mi.heap) > 1 {
				mi.heap[0] = mi.heap[len(mi.heap)-1]
				mi.heap = mi.heap[:len(mi.heap)-1]
				heapFixDown(mi.heap, 0, mi.comparator)
			} else {
				mi.heap = mi.heap[:0]
			}
			break
		}

		// Check timestamp and prefix constraints
		if ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {
			// Update the item with new values
			top.Key = k
			top.Value = v
			top.Timestamp = ts
			// Maintain heap property
			heapFixDown(mi.heap, 0, mi.comparator)
			break
		}

		// If we have a prefix and this key is lexicographically greater than
		// any possible key with our prefix, remove this iterator from heap
		if len(mi.prefix) > 0 {

			// Create prefix upper bound
			prefixUpperBound := make([]byte, len(mi.prefix))
			copy(prefixUpperBound, mi.prefix)
			if len(prefixUpperBound) > 0 {

				// Increment the last byte to create an upper bound
				incremented := false
				for i := len(prefixUpperBound) - 1; i >= 0; i-- {
					if prefixUpperBound[i] < 255 {
						prefixUpperBound[i]++
						incremented = true
						break
					}
					if i == 0 {

						// All bytes are 255, no upper bound possible
						incremented = false
						break
					}
					prefixUpperBound[i] = 0
				}

				// If we found an upper bound and current key is >= upper bound, remove iterator
				if incremented && bytes.Compare(k, prefixUpperBound) >= 0 {
					if len(mi.heap) > 1 {
						mi.heap[0] = mi.heap[len(mi.heap)-1]
						mi.heap = mi.heap[:len(mi.heap)-1]
						heapFixDown(mi.heap, 0, mi.comparator)
					} else {
						mi.heap = mi.heap[:0]
					}
					break
				}
			}
		}

		// Continue to next item in this iterator..
	}

	// Skip any duplicate keys (we only want the most recent version)
	mi.skipDuplicateKeys(key)

	// Update lastKey to the current key (make a copy to avoid slice reuse issues)
	mi.lastKey = make([]byte, len(key))

	copy(mi.lastKey, key)

	return key, value, timestamp, true
}

// skipDuplicateKeys advances past any items with the same key as the one we just returned
func (mi *MergeIterator) skipDuplicateKeys(key []byte) {
	for len(mi.heap) > 0 && bytes.Equal(mi.heap[0].Key, key) {

		top := mi.heap[0]

		// Advance the iterator to find next valid item
		for {
			k, v, ts, ok := top.Iter.Next()

			if !ok {

				// No more items, remove iterator from heap
				if len(mi.heap) > 1 {
					mi.heap[0] = mi.heap[len(mi.heap)-1]
					mi.heap = mi.heap[:len(mi.heap)-1]
					heapFixDown(mi.heap, 0, mi.comparator)
				} else {
					mi.heap = mi.heap[:0]
				}
				break
			}

			// Check timestamp and prefix constraints
			if ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {
				// Update heap item
				top.Key = k
				top.Value = v
				top.Timestamp = ts
				heapFixDown(mi.heap, 0, mi.comparator)
				break
			}

			// If we have a prefix and this key is beyond our prefix range, remove iterator
			if len(mi.prefix) > 0 {

				// Create prefix upper bound
				prefixUpperBound := make([]byte, len(mi.prefix))
				copy(prefixUpperBound, mi.prefix)
				if len(prefixUpperBound) > 0 {

					// Increment the last byte to create an upper bound
					incremented := false
					for i := len(prefixUpperBound) - 1; i >= 0; i-- {
						if prefixUpperBound[i] < 255 {
							prefixUpperBound[i]++
							incremented = true
							break
						}
						if i == 0 {
							// All bytes are 255, no upper bound possible
							incremented = false
							break
						}
						prefixUpperBound[i] = 0
					}

					// If we found an upper bound and current key is >= upper bound, remove iterator
					if incremented && bytes.Compare(k, prefixUpperBound) >= 0 {
						if len(mi.heap) > 1 {
							mi.heap[0] = mi.heap[len(mi.heap)-1]
							mi.heap = mi.heap[:len(mi.heap)-1]
							heapFixDown(mi.heap, 0, mi.comparator)
						} else {
							mi.heap = mi.heap[:0]
						}
						break
					}
				}
			}

			// Continue to next item..
		}
	}
}

// Prev moves the iterator backward to the previous key
func (mi *MergeIterator) Prev() ([]byte, []byte, int64, bool) {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()

	// If no lastKey is set, we haven't started iterating yet
	if len(mi.lastKey) == 0 {
		return nil, nil, 0, false
	}

	// If we just reached the end of forward iteration, return the last key
	if mi.atEnd {
		mi.atEnd = false

		// Find the value for lastKey
		allIterators := mi.createAllIterators(nil)
		for _, iter := range allIterators {
			for {
				k, v, ts, ok := iter.Next()
				if !ok {
					break
				}
				if ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {
					if bytes.Equal(k, mi.lastKey) {
						mi.rebuildHeapAt(mi.lastKey)
						return k, v, ts, true
					}
				}
			}
		}
	}

	// Find the largest key that is less than lastKey
	var bestKey []byte
	var bestValue []byte
	var bestTimestamp int64
	var found bool

	// Create new iterators positioned appropriately
	allIterators := mi.createAllIterators(nil) // Start from beginning

	for _, iter := range allIterators {
		var candidateKey []byte
		var candidateValue []byte
		var candidateTimestamp int64
		var candidateFound bool

		// Scan this iterator to find the largest key < lastKey
		for {
			k, v, ts, ok := iter.Next()
			if !ok {
				break
			}

			// Check constraints
			if ts > mi.txn.Timestamp {
				continue
			}
			if mi.prefix != nil && !bytes.HasPrefix(k, mi.prefix) {
				continue
			}

			// Check if this key is less than lastKey
			if bytes.Compare(k, mi.lastKey) >= 0 {
				break // We've gone too far
			}

			// This is a candidate.. keep the most recent one for this key
			candidateKey = make([]byte, len(k))
			copy(candidateKey, k)
			candidateValue = make([]byte, len(v))
			copy(candidateValue, v)
			candidateTimestamp = ts
			candidateFound = true
		}

		// Check if this iterator's candidate is better than our current best
		if candidateFound {
			if !found ||
				bytes.Compare(candidateKey, bestKey) > 0 ||
				(bytes.Equal(candidateKey, bestKey) && candidateTimestamp > bestTimestamp) {
				bestKey = candidateKey
				bestValue = candidateValue
				bestTimestamp = candidateTimestamp
				found = true
			}
		}
	}

	if !found {
		// No more keys before lastKey
		return nil, nil, 0, false
	}

	// Update lastKey to the found key
	mi.lastKey = make([]byte, len(bestKey))
	copy(mi.lastKey, bestKey)

	// Rebuild the heap positioned at this key for future Next() calls
	mi.rebuildHeapAt(bestKey)

	return bestKey, bestValue, bestTimestamp, true
}

// createAllIterators creates fresh iterators for all sources
func (mi *MergeIterator) createAllIterators(startKey []byte) []IteratorWithPeek {
	var iters []IteratorWithPeek

	effectiveStartKey := startKey
	if len(mi.prefix) > 0 && (len(startKey) == 0 || bytes.Compare(mi.prefix, startKey) > 0) {
		effectiveStartKey = mi.prefix
	}

	// Add active memtable iterator
	mem := mi.txn.db.memtable.Load().(*Memtable)
	memIter := mem.skiplist.NewIterator(effectiveStartKey, mi.txn.Timestamp)
	iters = append(iters, memIter)

	// Add immutable memtables iterators
	mi.txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		if memt, ok := item.(*Memtable); ok {
			immIter := memt.skiplist.NewIterator(effectiveStartKey, mi.txn.Timestamp)
			iters = append(iters, immIter)
		}
		return true
	})

	// Add SSTable iterators
	levels := mi.txn.db.levels.Load()
	for _, level := range *levels {
		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		for _, sst := range *sstables {
			if sst.EntryCount == 0 {
				continue
			}

			// Skip SSTable if key is outside range
			if len(effectiveStartKey) > 0 && len(sst.Min) > 0 && len(sst.Max) > 0 {
				if bytes.Compare(effectiveStartKey, sst.Max) > 0 {
					continue
				}
			}

			// Skip if prefix is outside SSTable range
			if len(mi.prefix) > 0 && len(sst.Min) > 0 && len(sst.Max) > 0 {
				if bytes.Compare(mi.prefix, sst.Max) > 0 {
					continue
				}

				// Create prefix upper bound
				prefixUpperBound := mi.createPrefixUpperBound(mi.prefix)
				if prefixUpperBound != nil && bytes.Compare(sst.Min, prefixUpperBound) >= 0 {
					continue
				}
			}

			it := sst.iterator()
			if it == nil {
				continue
			}

			wrapped := &SSTableIteratorWrapper{
				iter:      it,
				timestamp: mi.txn.Timestamp,
				valid:     true,
			}

			if len(effectiveStartKey) > 0 {
				if err := it.Seek(effectiveStartKey); err != nil {
					continue
				}
			}

			iters = append(iters, wrapped)
		}
	}

	return iters
}

// createPrefixUpperBound creates an upper bound for prefix matching
func (mi *MergeIterator) createPrefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	prefixUpperBound := make([]byte, len(prefix))
	copy(prefixUpperBound, prefix)

	// Increment the last byte to create an upper bound
	for i := len(prefixUpperBound) - 1; i >= 0; i-- {
		if prefixUpperBound[i] < 255 {
			prefixUpperBound[i]++
			return prefixUpperBound
		}
		if i == 0 {
			// All bytes are 255, no upper bound possible
			return nil
		}
		prefixUpperBound[i] = 0
	}
	return prefixUpperBound
}

// rebuildHeapAt rebuilds the heap to position all iterators at or after the given key
func (mi *MergeIterator) rebuildHeapAt(targetKey []byte) {
	// Clear current heap
	mi.heap = mi.heap[:0]

	// Create fresh iterators positioned at targetKey
	allIterators := mi.createAllIterators(targetKey)

	// Initialize heap with first valid item from each iterator that is >= targetKey
	for _, iter := range allIterators {

		// Find the first key >= targetKey
		for {
			k, v, ts, ok := iter.Next()
			if !ok {
				break
			}

			// Check timestamp and prefix constraints
			if ts <= mi.txn.Timestamp && (mi.prefix == nil || bytes.HasPrefix(k, mi.prefix)) {

				// Only add if key >= targetKey
				if bytes.Compare(k, targetKey) >= 0 {
					mi.heap = append(mi.heap, &IteratorItem{
						Key:       k,
						Value:     v,
						Timestamp: ts,
						Iter:      iter,
					})
					break
				}
			}

			// Check if we've gone beyond prefix range
			if len(mi.prefix) > 0 {
				prefixUpperBound := mi.createPrefixUpperBound(mi.prefix)
				if prefixUpperBound != nil && bytes.Compare(k, prefixUpperBound) >= 0 {
					break
				}
			}
		}
	}

	// Rebuild min-heap
	heapify(mi.heap, mi.comparator)
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

// Seek moves the iterator to the specified key
func (it *SSTableIterator) Seek(key []byte) error {
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

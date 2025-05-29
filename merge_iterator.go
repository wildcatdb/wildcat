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
	"container/heap"
	"github.com/wildcatdb/wildcat/skiplist"
	"github.com/wildcatdb/wildcat/tree"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MergeIterator combines multiple iterators into a single iterator
type MergeIterator struct {
	heap          iteratorHeap
	reverseHeap   reverseIteratorHeap
	ts            int64
	ascending     bool
	lastKey       []byte
	lastTimestamp int64
	allIterators  []*iterator
}

// iterator is the internal structure for each iterator
type iterator struct {
	underlyingIterator interface{}
	currentKey         []byte
	currentValue       []byte
	sst                *SSTable
	currentTimestamp   int64
	exhausted          bool
	index              int
	ascending          bool
}

// NewMergeIterator creates a new MergeIterator with the given iterators
func NewMergeIterator(iterators []*iterator, ts int64, ascending bool) (*MergeIterator, error) {
	mi := &MergeIterator{
		heap:         make(iteratorHeap, 0, len(iterators)),
		reverseHeap:  make(reverseIteratorHeap, 0, len(iterators)),
		ts:           ts,
		ascending:    ascending,
		allIterators: make([]*iterator, len(iterators)),
	}

	// Copy iterators for potential re-initialization
	copy(mi.allIterators, iterators)

	// Initialize each iterator and add to appropriate heap based on direction
	for _, it := range iterators {
		it.ascending = ascending
		if err := mi.initializeIterator(it); err != nil {
			return nil, err
		}

		if !it.exhausted {
			if ascending {
				heap.Push(&mi.heap, it)
			} else {
				heap.Push(&mi.reverseHeap, it)
			}
		}
	}

	return mi, nil
}

// initializeIterator sets up the iterator with its first key-value pair
func (mi *MergeIterator) initializeIterator(it *iterator) error {
	switch t := it.underlyingIterator.(type) {
	case *skiplist.Iterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {

			// For ascending, start from beginning
			key, value, ts, ok := t.Next()
			if ok && ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
			} else {
				it.exhausted = true
			}
		} else {
			// For descending, need to position at the end first
			if !t.Valid() {
				it.exhausted = true
				return nil
			}

			t.ToLast()

			// Get current position using Peek or by reading directly
			key, value, ts, ok := t.Peek()
			if ok && ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
			} else {
				// If peek failed or timestamp is not visible, try to find a valid position
				for {
					key, value, ts, ok := t.Prev()
					if !ok {
						it.exhausted = true
						break
					}
					if ts <= mi.ts {
						it.currentKey = key
						it.currentValue = value
						it.currentTimestamp = ts
						break
					}
				}
				if len(it.currentKey) == 0 {
					it.exhausted = true
				}
			}
		}
	case *skiplist.RangeIterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {
			// For ascending, start from beginning
			key, value, ts, ok := t.Next()
			if ok && ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
			} else {
				it.exhausted = true
			}
		} else {
			// For descending, need to position at the end first
			if !t.Valid() {
				it.exhausted = true
				return nil
			}

			t.ToLast()

			// After ToLast(), we should check if we're still in bounds
			key, value, ts, ok := t.Peek()
			if ok && ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
			} else {
				// If peek failed or timestamp is not visible, try to find a valid position
				for {
					key, value, ts, ok := t.Prev()
					if !ok {
						it.exhausted = true
						break
					}
					if ts <= mi.ts {
						it.currentKey = key
						it.currentValue = value
						it.currentTimestamp = ts
						break
					}
				}
				if len(it.currentKey) == 0 {
					it.exhausted = true
				}
			}
		}
	case *skiplist.PrefixIterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {

			// For ascending, start from beginning
			key, value, ts, ok := t.Next()
			if ok && ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
			} else {
				it.exhausted = true
			}
		} else {
			// For descending, need to position at the end first
			if !t.Valid() {
				it.exhausted = true
				return nil
			}

			t.ToLast()

			// Get current position using Peek or by reading directly
			key, value, ts, ok := t.Peek()
			if ok && ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
			} else {
				// If peek failed or timestamp is not visible, try to find a valid position
				for {
					key, value, ts, ok := t.Prev()
					if !ok {
						it.exhausted = true
						break
					}
					if ts <= mi.ts {
						it.currentKey = key
						it.currentValue = value
						it.currentTimestamp = ts
						break
					}
				}
				if len(it.currentKey) == 0 {
					it.exhausted = true
				}
			}
		}
	case *tree.Iterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {
			// For ascending, start from beginning
			if t.Next() {
				entry, err := mi.extractKLogEntry(t.Value())
				if err != nil {
					return err
				}

				if entry.Timestamp <= mi.ts {
					it.currentKey = entry.Key
					it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
					it.currentTimestamp = entry.Timestamp
				} else {
					it.exhausted = true
				}
			} else {
				it.exhausted = true
			}
		} else {
			// For descending, position at the end first
			if err := t.SeekToLast(); err != nil {
				it.exhausted = true
				return err
			}

			if t.Valid() {
				entry, err := mi.extractKLogEntry(t.Value())
				if err != nil {
					return err
				}

				if entry.Timestamp <= mi.ts {
					it.currentKey = entry.Key
					it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
					it.currentTimestamp = entry.Timestamp
				} else {

					// Find a valid position going backwards
					for t.Prev() {
						entry, err := mi.extractKLogEntry(t.Value())
						if err != nil {
							it.exhausted = true
							return err
						}
						if entry.Timestamp <= mi.ts {
							it.currentKey = entry.Key
							it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
							it.currentTimestamp = entry.Timestamp
							return nil
						}
					}
					it.exhausted = true
				}
			} else {
				it.exhausted = true
			}
		}

	default:
		it.exhausted = true
	}

	return nil
}

// extractKLogEntry converts various types to KLogEntry
func (mi *MergeIterator) extractKLogEntry(value interface{}) (*KLogEntry, error) {
	var entry *KLogEntry

	if klogEntry, ok := value.(*KLogEntry); ok {
		entry = klogEntry
	} else if doc, ok := value.(primitive.D); ok {
		entry = &KLogEntry{}

		// Extract fields from primitive.D (bson)
		for _, elem := range doc {
			switch elem.Key {
			case "key":
				if keyData, ok := elem.Value.(primitive.Binary); ok {
					entry.Key = keyData.Data
				}
			case "timestamp":
				if ts, ok := elem.Value.(int64); ok {
					entry.Timestamp = ts
				}
			case "valueblockid":
				if blockID, ok := elem.Value.(int64); ok {
					entry.ValueBlockID = blockID
				}
			}
		}
	} else {
		// Unknown type, try to convert via BSON
		bsonData, err := bson.Marshal(value)
		if err != nil {
			return nil, err
		}

		entry = &KLogEntry{}
		err = bson.Unmarshal(bsonData, entry)
		if err != nil {
			return nil, err
		}
	}

	return entry, nil
}

// SetDirection changes the iteration direction
func (mi *MergeIterator) SetDirection(ascending bool) error {
	if mi.ascending == ascending {
		return nil // No change needed
	}

	mi.ascending = ascending

	// Clear current heaps
	mi.heap = make(iteratorHeap, 0, len(mi.allIterators))
	mi.reverseHeap = make(reverseIteratorHeap, 0, len(mi.allIterators))

	for _, it := range mi.allIterators {
		it.ascending = ascending

		if !it.exhausted && len(it.currentKey) > 0 {
			if ascending {
				heap.Push(&mi.heap, it)
			} else {
				heap.Push(&mi.reverseHeap, it)
			}
		}
	}

	return nil
}

// seekIterator positions an iterator at or near the given key
func (mi *MergeIterator) seekIterator(it *iterator, seekKey []byte) error {
	switch t := it.underlyingIterator.(type) {
	case *skiplist.Iterator:
		// SkipList iterators don't have a direct seek method
		// We need to iterate until we find the right position
		it.exhausted = true
		for {
			var key []byte
			var value []byte
			var ts int64
			var ok bool

			if it.ascending {
				key, value, ts, ok = t.Next()
			} else {
				key, value, ts, ok = t.Prev()
			}

			if !ok {
				break
			}

			if ts <= mi.ts {
				cmp := bytes.Compare(key, seekKey)
				if (it.ascending && cmp >= 0) || (!it.ascending && cmp <= 0) {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					it.exhausted = false
					break
				}
			}
		}

	case *tree.Iterator:
		if err := t.Seek(seekKey); err != nil {
			it.exhausted = true
			return err
		}

		// Check if we have a valid position
		if t.Valid() {
			entry, err := mi.extractKLogEntry(t.Value())
			if err != nil {
				it.exhausted = true
				return err
			}

			if entry.Timestamp <= mi.ts {
				it.currentKey = entry.Key
				it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
				it.currentTimestamp = entry.Timestamp
				it.exhausted = false
			} else {
				it.exhausted = true
			}
		} else {
			it.exhausted = true
		}
	}

	return nil
}

// Next returns the next key-value pair in the configured direction
func (mi *MergeIterator) Next() ([]byte, []byte, int64, bool) {
	if mi.ascending {
		return mi.nextAscending()
	}
	return mi.nextDescending()
}

// nextAscending handles ascending iteration
func (mi *MergeIterator) nextAscending() ([]byte, []byte, int64, bool) {
	if mi.heap.Len() == 0 {
		return nil, nil, 0, false
	}

	// Get the iterator with the smallest key
	current := heap.Pop(&mi.heap).(*iterator)

	key := make([]byte, len(current.currentKey))
	copy(key, current.currentKey)
	value := make([]byte, len(current.currentValue))
	copy(value, current.currentValue)
	timestamp := current.currentTimestamp

	// Skip any duplicate keys with older timestamps
	for mi.heap.Len() > 0 && bytes.Equal(mi.heap[0].currentKey, key) {
		duplicate := heap.Pop(&mi.heap).(*iterator)
		mi.advanceIterator(duplicate)
		if !duplicate.exhausted {
			heap.Push(&mi.heap, duplicate)
		}
	}

	// Advance the current iterator
	mi.advanceIterator(current)
	if !current.exhausted {
		heap.Push(&mi.heap, current)
	}

	// Remember the last key for potential direction changes
	mi.lastKey = key
	mi.lastTimestamp = timestamp

	return key, value, timestamp, true
}

// nextDescending handles descending iteration
func (mi *MergeIterator) nextDescending() ([]byte, []byte, int64, bool) {
	if mi.reverseHeap.Len() == 0 {
		return nil, nil, 0, false
	}

	// Get the iterator with the largest key
	current := heap.Pop(&mi.reverseHeap).(*iterator)

	key := make([]byte, len(current.currentKey))
	copy(key, current.currentKey)
	value := make([]byte, len(current.currentValue))
	copy(value, current.currentValue)
	timestamp := current.currentTimestamp

	// Skip any duplicate keys with older timestamps
	for mi.reverseHeap.Len() > 0 && bytes.Equal(mi.reverseHeap[0].currentKey, key) {
		duplicate := heap.Pop(&mi.reverseHeap).(*iterator)
		mi.advanceIterator(duplicate)
		if !duplicate.exhausted {
			heap.Push(&mi.reverseHeap, duplicate)
		}
	}

	// Advance the current iterator
	mi.advanceIterator(current)
	if !current.exhausted {
		heap.Push(&mi.reverseHeap, current)
	}

	// Remember the last key for potential direction changes
	mi.lastKey = key
	mi.lastTimestamp = timestamp

	return key, value, timestamp, true
}

// Prev returns the previous key-value pair (opposite of configured direction)
func (mi *MergeIterator) Prev() ([]byte, []byte, int64, bool) {
	// Check if we have any iterators at all
	if len(mi.allIterators) == 0 {
		return nil, nil, 0, false
	}

	if mi.ascending {
		// If configured for ascending, Prev means descending
		if err := mi.SetDirection(false); err != nil {
			return nil, nil, 0, false
		}
		return mi.nextDescending()
	} else {
		// If configured for descending, Prev means ascending
		if err := mi.SetDirection(true); err != nil {
			return nil, nil, 0, false
		}
		return mi.nextAscending()
	}
}

// advanceIterator moves the iterator to the next valid entry
func (mi *MergeIterator) advanceIterator(it *iterator) {
	switch t := it.underlyingIterator.(type) {
	case *skiplist.Iterator:
		if t == nil {
			it.exhausted = true
			return
		}

		for {
			var key []byte
			var value []byte
			var ts int64
			var ok bool

			if it.ascending {
				key, value, ts, ok = t.Next()
			} else {
				key, value, ts, ok = t.Prev()
			}

			if !ok {
				it.exhausted = true
				return
			}
			if ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
				return
			}
		}

	case *skiplist.RangeIterator:
		if t == nil {
			it.exhausted = true
			return
		}

		for {
			var key []byte
			var value []byte
			var ts int64
			var ok bool

			if it.ascending {
				key, value, ts, ok = t.Next()
			} else {
				key, value, ts, ok = t.Prev()
			}

			if !ok {
				it.exhausted = true
				return
			}
			if ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
				return
			}
		}

	case *skiplist.PrefixIterator:
		if t == nil {
			it.exhausted = true
			return
		}

		for {
			var key []byte
			var value []byte
			var ts int64
			var ok bool

			if it.ascending {
				key, value, ts, ok = t.Next()
			} else {
				key, value, ts, ok = t.Prev()
			}

			if !ok {
				it.exhausted = true
				return
			}
			if ts <= mi.ts {
				it.currentKey = key
				it.currentValue = value
				it.currentTimestamp = ts
				return
			}
		}

	case *tree.Iterator:
		if t == nil {
			it.exhausted = true
			return
		}

		for {
			var hasNext bool
			if it.ascending {
				hasNext = t.Next()
			} else {
				hasNext = t.Prev()
			}

			if !hasNext {
				it.exhausted = true
				return
			}

			entry, err := mi.extractKLogEntry(t.Value())
			if err != nil {
				it.exhausted = true
				return
			}

			if entry.Timestamp <= mi.ts {
				it.currentKey = entry.Key
				it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
				it.currentTimestamp = entry.Timestamp
				return
			}
		}
	default:
		it.exhausted = true
	}
}

// HasNext returns true if there are more entries in the configured direction
func (mi *MergeIterator) HasNext() bool {
	if mi.ascending {
		return mi.heap.Len() > 0
	}
	return mi.reverseHeap.Len() > 0
}

// HasPrev returns true if there are entries in the opposite direction
func (mi *MergeIterator) HasPrev() bool {
	if mi.ascending {
		return mi.reverseHeap.Len() > 0
	}
	return mi.heap.Len() > 0
}

// IsAscending returns the current iteration direction
func (mi *MergeIterator) IsAscending() bool {
	return mi.ascending
}

// iteratorHeap implements heap.Interface for managing iterators by key
type iteratorHeap []*iterator

// Len returns the number of elements in the heap
func (h iteratorHeap) Len() int { return len(h) }

// Less compares two iterators based on their current keys and timestamps
func (h iteratorHeap) Less(i, j int) bool {

	// Compare keys lexicographically
	cmp := bytes.Compare(h[i].currentKey, h[j].currentKey)
	if cmp != 0 {
		return cmp < 0
	}
	// If keys are equal, prioritize by timestamp (newer first)
	return h[i].currentTimestamp > h[j].currentTimestamp
}

// Swap swaps two elements in the heap and updates their indices
func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push adds a new iterator to the heap
func (h *iteratorHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*iterator)
	item.index = n
	*h = append(*h, item)
}

// Pop removes and returns the iterator with the smallest key from the heap
func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// reverseIteratorHeap for descending iteration (largest keys first)
type reverseIteratorHeap []*iterator

// Len returns the number of elements in the reverse heap
func (h reverseIteratorHeap) Len() int { return len(h) }

// Less compares two iterators based on their current keys and timestamps in reverse order
func (h reverseIteratorHeap) Less(i, j int) bool {
	// Compare keys lexicographically (reverse order for descending)
	cmp := bytes.Compare(h[i].currentKey, h[j].currentKey)
	if cmp != 0 {
		return cmp > 0 // Reverse comparison for descending order
	}

	// If keys are equal, prioritize by timestamp (newer first)
	return h[i].currentTimestamp > h[j].currentTimestamp
}

// Swap swaps two elements in the reverse heap and updates their indices
func (h reverseIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push adds a new iterator to the reverse heap
func (h *reverseIteratorHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*iterator)
	item.index = n
	*h = append(*h, item)
}

// Pop removes and returns the iterator with the largest key from the reverse heap
func (h *reverseIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

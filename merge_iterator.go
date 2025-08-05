package wildcat

import (
	"bytes"
	"container/heap"
	"fmt"
	"github.com/wildcatdb/wildcat/v2/skiplist"
	"github.com/wildcatdb/wildcat/v2/tree"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"sync"
	"sync/atomic"
)

// iteratorPool reuses iterator objects
var iteratorPool = sync.Pool{
	New: func() interface{} {
		return &iterator{}
	},
}

// MergeIterator combines multiple iterators into a single iterator
type MergeIterator struct {
	ascendingHeap   iteratorHeap
	descendingHeap  reverseIteratorHeap
	ts              int64
	ascending       bool
	lastKey         []byte
	lastTimestamp   int64
	allIterators    []*iterator
	db              *DB
	initialized     bool
	duplicateBuffer []*iterator
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
	ts                 int64
	initialized        bool
}

// resetIterator clears iterator state for reuse
func (it *iterator) reset() {
	it.underlyingIterator = nil
	it.currentKey = nil
	it.currentValue = nil
	it.sst = nil
	it.currentTimestamp = 0
	it.exhausted = false
	it.index = -1
	it.ascending = false
	it.ts = 0
	it.initialized = false
}

// getIterator gets an iterator from the pool
func getIterator() *iterator {
	return iteratorPool.Get().(*iterator)
}

// putIterator returns an iterator to the pool
func putIterator(it *iterator) {
	it.reset()
	iteratorPool.Put(it)
}

// NewMergeIterator creates a new MergeIterator with the given iterators
func NewMergeIterator(db *DB, iterators []*iterator, ts int64, ascending bool) (*MergeIterator, error) {
	mi := &MergeIterator{
		ascendingHeap:   make(iteratorHeap, 0, len(iterators)),
		descendingHeap:  make(reverseIteratorHeap, 0, len(iterators)),
		ts:              ts,
		ascending:       ascending,
		allIterators:    make([]*iterator, len(iterators)),
		db:              db,
		duplicateBuffer: make([]*iterator, 0, len(iterators)),
	}

	copy(mi.allIterators, iterators)

	// Set timestamp for push-down filtering on each iterator
	for _, it := range mi.allIterators {
		it.ascending = ascending
		it.ts = ts
	}

	return mi, nil
}

// ensureInitialized performs lazy initialization of all iterators
func (mi *MergeIterator) ensureInitialized() error {
	if mi.initialized {
		return nil
	}

	// Initialize both heaps simultaneously to avoid rebuilding on direction changes
	for _, it := range mi.allIterators {
		if err := mi.initializeIterator(it); err != nil {
			return err
		}

		if !it.exhausted {
			ascendingCopy := getIterator()
			*ascendingCopy = *it
			descendingCopy := getIterator()
			*descendingCopy = *it

			heap.Push(&mi.ascendingHeap, ascendingCopy)
			heap.Push(&mi.descendingHeap, descendingCopy)
		}
	}

	mi.initialized = true
	return nil
}

// initializeIterator sets up the iterator with its first key-value pair
func (mi *MergeIterator) initializeIterator(it *iterator) error {
	if it.initialized {
		return nil
	}

	if it.sst != nil {
		atomic.CompareAndSwapInt32(&it.sst.isBeingRead, 0, 1)
	}

	switch t := it.underlyingIterator.(type) {
	case *skiplist.Iterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {
			// Find first valid entry with timestamp filtering
			for {
				key, value, ts, ok := t.Next()
				if !ok {
					it.exhausted = true
					break
				}
				if ts <= it.ts {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					break
				}
			}
		} else {
			if !t.Valid() {
				it.exhausted = true
				return nil
			}
			t.ToLast()

			// Find last valid entry with timestamp filtering
			for {
				key, value, ts, ok := t.Peek()
				if !ok {
					key, value, ts, ok = t.Prev()
				}
				if !ok {
					it.exhausted = true
					break
				}
				if ts <= it.ts {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					break
				}
				_, _, _, ok = t.Prev()
				if !ok {
					it.exhausted = true
					break
				}
			}
		}

	case *skiplist.RangeIterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {
			for {
				key, value, ts, ok := t.Next()
				if !ok {
					it.exhausted = true
					break
				}
				if ts <= it.ts {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					break
				}
			}
		} else {
			if !t.Valid() {
				it.exhausted = true
				return nil
			}
			t.ToLast()

			for {
				key, value, ts, ok := t.Peek()
				if !ok {
					key, value, ts, ok = t.Prev()
				}
				if !ok {
					it.exhausted = true
					break
				}
				if ts <= it.ts {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					break
				}
				_, _, _, ok = t.Prev()
				if !ok {
					it.exhausted = true
					break
				}
			}
		}

	case *skiplist.PrefixIterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {
			for {
				key, value, ts, ok := t.Next()
				if !ok {
					it.exhausted = true
					break
				}
				if ts <= it.ts {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					break
				}
			}
		} else {
			if !t.Valid() {
				it.exhausted = true
				return nil
			}
			t.ToLast()

			for {
				key, value, ts, ok := t.Peek()
				if !ok {
					key, value, ts, ok = t.Prev()
				}
				if !ok {
					it.exhausted = true
					break
				}
				if ts <= it.ts {
					it.currentKey = key
					it.currentValue = value
					it.currentTimestamp = ts
					break
				}
				_, _, _, ok = t.Prev()
				if !ok {
					it.exhausted = true
					break
				}
			}
		}

	case *tree.Iterator:
		if t == nil {
			it.exhausted = true
			return nil
		}

		if it.ascending {
			for t.Next() {
				entry, err := mi.extractKLogEntry(t.Value())
				if err != nil {
					if it.sst != nil {
						mi.db.log(fmt.Sprintf("Potential block corruption detected for SSTable %d at Level %d: %v", it.sst.Id, it.sst.Level, err))
					}
					it.exhausted = true
					return err
				}

				if entry.Timestamp <= it.ts {
					it.currentKey = entry.Key
					it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
					it.currentTimestamp = entry.Timestamp
					break
				}
			}
			if len(it.currentKey) == 0 {
				it.exhausted = true
			}
		} else {
			if err := t.SeekToLast(); err != nil {
				it.exhausted = true
				return err
			}

			for t.Valid() {
				entry, err := mi.extractKLogEntry(t.Value())
				if err != nil {
					if it.sst != nil {
						mi.db.log(fmt.Sprintf("Potential block corruption detected for SSTable %d at Level %d: %v", it.sst.Id, it.sst.Level, err))
					}
					it.exhausted = true
					return err
				}

				if entry.Timestamp <= it.ts {
					it.currentKey = entry.Key
					it.currentValue = it.sst.readValueFromVLog(entry.ValueBlockID)
					it.currentTimestamp = entry.Timestamp
					break
				}

				if !t.Prev() {
					break
				}
			}
			if len(it.currentKey) == 0 {
				it.exhausted = true
			}
		}

	default:
		it.exhausted = true
	}

	it.initialized = true
	return nil
}

// extractKLogEntry converts various types to KLogEntry
func (mi *MergeIterator) extractKLogEntry(value interface{}) (*KLogEntry, error) {
	if klogEntry, ok := value.(*KLogEntry); ok {
		return klogEntry, nil
	}

	if doc, ok := value.(primitive.D); ok {
		entry := &KLogEntry{}
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
		return entry, nil
	}

	bsonData, err := bson.Marshal(value)
	if err != nil {
		return nil, err
	}

	entry := &KLogEntry{}
	err = bson.Unmarshal(bsonData, entry)
	return entry, err
}

// SetDirection changes the iteration direction
func (mi *MergeIterator) SetDirection(ascending bool) error {
	if err := mi.ensureInitialized(); err != nil {
		return err
	}

	if mi.ascending == ascending {
		return nil
	}

	mi.ascending = ascending
	return nil
}

// Next returns the next key-value pair in the configured direction
// Returns slices that reference existing data to avoid allocations
func (mi *MergeIterator) Next() ([]byte, []byte, int64, bool) {
	if err := mi.ensureInitialized(); err != nil {
		return nil, nil, 0, false
	}

	if mi.ascending {
		return mi.nextAscending()
	}
	return mi.nextDescending()
}

// nextAscending handles ascending iteration with batch duplicate removal
func (mi *MergeIterator) nextAscending() ([]byte, []byte, int64, bool) {
	if mi.ascendingHeap.Len() == 0 {
		return nil, nil, 0, false
	}

	// Get the iterator with the smallest key
	current := heap.Pop(&mi.ascendingHeap).(*iterator)

	// Return direct references to avoid allocation
	key := current.currentKey
	value := current.currentValue
	timestamp := current.currentTimestamp

	// Batch process all duplicates with the same key
	mi.duplicateBuffer = mi.duplicateBuffer[:0] // Reuse buffer
	for mi.ascendingHeap.Len() > 0 && bytes.Equal(mi.ascendingHeap[0].currentKey, key) {
		duplicate := heap.Pop(&mi.ascendingHeap).(*iterator)
		mi.duplicateBuffer = append(mi.duplicateBuffer, duplicate)
	}

	mi.advanceIterator(current)
	if !current.exhausted {
		heap.Push(&mi.ascendingHeap, current)
	} else {
		putIterator(current)
	}

	// Advance all duplicate iterators
	for _, duplicate := range mi.duplicateBuffer {
		mi.advanceIterator(duplicate)
		if !duplicate.exhausted {
			heap.Push(&mi.ascendingHeap, duplicate)
		} else {
			putIterator(duplicate)
		}
	}

	mi.lastKey = key
	mi.lastTimestamp = timestamp

	return key, value, timestamp, true
}

// nextDescending handles descending iteration with batch duplicate removal
func (mi *MergeIterator) nextDescending() ([]byte, []byte, int64, bool) {
	if mi.descendingHeap.Len() == 0 {
		return nil, nil, 0, false
	}

	// Get the iterator with the largest key
	current := heap.Pop(&mi.descendingHeap).(*iterator)

	// Return direct references to avoid allocation
	key := current.currentKey
	value := current.currentValue
	timestamp := current.currentTimestamp

	// Batch process all duplicates with the same key
	mi.duplicateBuffer = mi.duplicateBuffer[:0] // Reuse buffer

	for mi.descendingHeap.Len() > 0 && bytes.Equal(mi.descendingHeap[0].currentKey, key) {
		duplicate := heap.Pop(&mi.descendingHeap).(*iterator)
		mi.duplicateBuffer = append(mi.duplicateBuffer, duplicate)
	}

	mi.advanceIterator(current)
	if !current.exhausted {
		heap.Push(&mi.descendingHeap, current)
	} else {
		putIterator(current)
	}

	for _, duplicate := range mi.duplicateBuffer {
		mi.advanceIterator(duplicate)
		if !duplicate.exhausted {
			heap.Push(&mi.descendingHeap, duplicate)
		} else {
			putIterator(duplicate)
		}
	}

	mi.lastKey = key
	mi.lastTimestamp = timestamp

	return key, value, timestamp, true
}

// advanceIterator moves the iterator to the next valid entry with timestamp filtering
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

			// Push-down timestamp filtering
			if ts <= it.ts {
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
			if ts <= it.ts {
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
			if ts <= it.ts {
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
				if it.sst != nil {
					mi.db.log(fmt.Sprintf("Potential block corruption detected for SSTable %d at Level %d: %v", it.sst.Id, it.sst.Level, err))
				}
				it.exhausted = true
				return
			}

			if entry.Timestamp <= it.ts {
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

// Prev returns the previous key-value pair (opposite of configured direction)
func (mi *MergeIterator) Prev() ([]byte, []byte, int64, bool) {
	if err := mi.ensureInitialized(); err != nil {
		return nil, nil, 0, false
	}

	if mi.ascending {
		mi.ascending = false
		return mi.nextDescending()
	} else {
		mi.ascending = true
		return mi.nextAscending()
	}
}

// HasNext returns true if there are more entries in the configured direction
func (mi *MergeIterator) HasNext() bool {
	if err := mi.ensureInitialized(); err != nil {
		return false
	}

	if mi.ascending {
		return mi.ascendingHeap.Len() > 0
	}
	return mi.descendingHeap.Len() > 0
}

// HasPrev returns true if there are entries in the opposite direction
func (mi *MergeIterator) HasPrev() bool {
	if err := mi.ensureInitialized(); err != nil {
		return false
	}

	if mi.ascending {
		return mi.descendingHeap.Len() > 0
	}
	return mi.ascendingHeap.Len() > 0
}

// IsAscending returns the current iteration direction
func (mi *MergeIterator) IsAscending() bool {
	return mi.ascending
}

// Close cleans up resources and returns iterators to the pool
func (mi *MergeIterator) Close() {
	for _, it := range mi.allIterators {
		if it.sst != nil {
			atomic.CompareAndSwapInt32(&it.sst.isBeingRead, 1, 0)
		}
		putIterator(it)
	}

	// Clear heap references
	for i := 0; i < mi.ascendingHeap.Len(); i++ {
		putIterator(mi.ascendingHeap[i])
	}
	for i := 0; i < mi.descendingHeap.Len(); i++ {
		putIterator(mi.descendingHeap[i])
	}

	mi.allIterators = mi.allIterators[:0]
	mi.ascendingHeap = mi.ascendingHeap[:0]
	mi.descendingHeap = mi.descendingHeap[:0]
	mi.duplicateBuffer = mi.duplicateBuffer[:0]
}

// iteratorHeap implements heap.Interface for managing iterators by key
type iteratorHeap []*iterator

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].currentKey, h[j].currentKey)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].currentTimestamp > h[j].currentTimestamp
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *iteratorHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*iterator)
	item.index = n
	*h = append(*h, item)
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// reverseIteratorHeap for descending iteration
type reverseIteratorHeap []*iterator

func (h reverseIteratorHeap) Len() int { return len(h) }

func (h reverseIteratorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].currentKey, h[j].currentKey)
	if cmp != 0 {
		return cmp > 0
	}
	return h[i].currentTimestamp > h[j].currentTimestamp
}

func (h reverseIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *reverseIteratorHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*iterator)
	item.index = n
	*h = append(*h, item)
}

func (h *reverseIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

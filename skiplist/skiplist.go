// Package skiplist
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
package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const MaxLevel = 16
const p = 0.25

// KeyComparator defines the interface for comparing keys
type KeyComparator func(a, b []byte) int

type ValueVersionType int

const (
	// Write represents a write operation (insert or update)
	Write ValueVersionType = iota
	// Delete represents a delete operation
	Delete
)

// ValueVersion represents a single version of a value in MVCC
type ValueVersion struct {
	Data      []byte           // Value data
	Timestamp int64            // Version timestamp
	Type      ValueVersionType // Type of version (write or delete)
	Next      *ValueVersion    // Pointer to the previous version (linked list)
}

// Node represents a node in the skip list
type Node struct {
	forward  [MaxLevel]unsafe.Pointer // array of atomic pointers to Node
	backward unsafe.Pointer           // atomic pointer to the previous node
	key      []byte                   // key used for searches
	versions unsafe.Pointer           // atomic pointer to the head of version chain
	mutex    sync.RWMutex             // mutex for version chain updates
}

// SkipList represents a concurrent skip list data structure with MVCC
type SkipList struct {
	header     *Node         // special header node
	level      atomic.Value  // current maximum level of the list (atomic)
	rng        *rand.Rand    // random number generator with its own lock
	rngMutex   sync.Mutex    // mutex for the random number generator
	comparator KeyComparator // user-provided comparator function
}

// Iterator provides a bidirectional iterator for the skip list with snapshot isolation
type Iterator struct {
	SkipList      *SkipList // Reference to the skip list
	current       *Node     // Current node in the iteration
	readTimestamp int64     // Timestamp for snapshot isolation
}

// PrefixIterator provides a bidirectional iterator for keys with a specific prefix
type PrefixIterator struct {
	SkipList      *SkipList // Reference to the skip list
	current       *Node     // Current node in the iteration
	readTimestamp int64     // Timestamp for snapshot isolation
	prefix        []byte    // Prefix to match
}

// RangeIterator provides a bidirectional iterator for keys within a specific range
type RangeIterator struct {
	SkipList      *SkipList // Reference to the skip list
	current       *Node     // Current node in the iteration
	readTimestamp int64     // Timestamp for snapshot isolation
	startKey      []byte    // Start of the range (inclusive)
	endKey        []byte    // End of the range (exclusive)
}

// DefaultComparator is the default key comparison function
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func DefaultComparator(a, b []byte) int {
	// Byte-by-byte comparison
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}

	// If all bytes compared so far are equal, the shorter key is "less than"
	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}

	// Keys are identical
	return 0
}

// New creates a new concurrent skip list with MVCC using the default comparator
func New() *SkipList {
	return NewWithComparator(DefaultComparator)
}

// NewWithComparator creates a new concurrent skip list with a custom key comparator
func NewWithComparator(cmp KeyComparator) *SkipList {
	// Create header node
	header := &Node{
		key: []byte{},
	}

	// Initialize all forward pointers to nil
	for i := 0; i < MaxLevel; i++ {
		header.forward[i] = nil
	}

	// Create the skip list
	sl := &SkipList{
		header:     header,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		comparator: cmp,
	}

	// Set initial level to 1
	sl.level.Store(1)

	return sl
}

// getLevel returns the current maximum level of the skip list
func (sl *SkipList) getLevel() int {
	return sl.level.Load().(int)
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	sl.rngMutex.Lock()
	defer sl.rngMutex.Unlock()

	lvl := 1
	for sl.rng.Float64() < p && lvl < MaxLevel {
		lvl++
	}
	return lvl
}

// getLatestVersion atomically returns the latest version of a node
func (n *Node) getLatestVersion() *ValueVersion {
	if n == nil {
		return nil
	}
	ptr := atomic.LoadPointer(&n.versions)
	return (*ValueVersion)(ptr)
}

// findVisibleVersion finds the latest version visible at the given timestamp
func (n *Node) findVisibleVersion(readTimestamp int64) *ValueVersion {
	if n == nil {
		return nil
	}

	// Use read lock to protect version chain traversal
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	// Get the latest version
	version := n.getLatestVersion()

	// Find the latest version that is visible at the read timestamp
	for version != nil {
		if version.Timestamp <= readTimestamp {
			// Check if it's a delete version
			if version.Type == Delete {
				// This key was deleted at or before our read timestamp
				return nil
			}
			// Found a valid version
			return version
		}
		// Try older version
		version = version.Next
	}

	// No visible version found
	return nil
}

// addVersion adds a new version to the node
func (n *Node) addVersion(data []byte, timestamp int64, versionType ValueVersionType) {
	// Create new version
	newVersion := &ValueVersion{
		Data:      data,
		Timestamp: timestamp,
		Type:      versionType,
	}

	// Use write lock to protect version chain updates
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// Get the current head of the version chain
	currentHead := n.getLatestVersion()

	// Set the next pointer of the new version to the current head
	newVersion.Next = currentHead

	// Atomically update the head of the version chain
	atomic.StorePointer(&n.versions, unsafe.Pointer(newVersion))
}

// Get retrieves a value from the skip list given a key and a read timestamp
func (sl *SkipList) Get(searchKey []byte, readTimestamp int64) ([]byte, int64, bool) {
	var prev *Node
	var curr *Node
	var currPtr unsafe.Pointer

	// Start at the header node
	prev = sl.header

	// Get current level
	currentLevel := sl.getLevel()

	// For each level, starting at the highest level in the list
	for i := currentLevel - 1; i >= 0; i-- {
		// Get the next node (atomic load)
		currPtr = atomic.LoadPointer(&prev.forward[i])
		curr = (*Node)(currPtr)

		// Traverse the current level
		for curr != nil {
			// If the current key is greater or equal, stop traversing this level
			if sl.comparator(curr.key, searchKey) >= 0 {
				break
			}

			// Move forward
			prev = curr
			currPtr = atomic.LoadPointer(&curr.forward[i])
			curr = (*Node)(currPtr)
		}
	}

	// Check bottom level for exact match
	currPtr = atomic.LoadPointer(&prev.forward[0])
	curr = (*Node)(currPtr)

	// Search for the node at level 0
	for curr != nil {
		// Check if we found the key
		cmp := sl.comparator(curr.key, searchKey)
		if cmp == 0 {
			// Find the visible version at the given read timestamp
			version := curr.findVisibleVersion(readTimestamp)
			if version != nil && version.Type != Delete {
				return version.Data, version.Timestamp, true
			}
			return nil, 0, false
		}

		// If we've gone too far, the key doesn't exist
		if cmp > 0 {
			return nil, 0, false
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	return nil, 0, false
}

// Put inserts or updates a value in the skip list with the given key
func (sl *SkipList) Put(searchKey []byte, newValue []byte, writeTimestamp int64) {
	var retry bool
	var topLevel int
	var existingNode *Node

	for {
		retry = false
		existingNode = nil
		var update [MaxLevel]*Node
		var prev *Node
		var curr *Node
		var currPtr unsafe.Pointer

		prev = sl.header
		currentLevel := sl.getLevel()

		for i := currentLevel - 1; i >= 0; i-- {
			currPtr = atomic.LoadPointer(&prev.forward[i])
			curr = (*Node)(currPtr)

			for curr != nil {
				if sl.comparator(curr.key, searchKey) >= 0 {
					break
				}
				prev = curr
				currPtr = atomic.LoadPointer(&curr.forward[i])
				curr = (*Node)(currPtr)
			}
			update[i] = prev
		}

		currPtr = atomic.LoadPointer(&prev.forward[0])
		curr = (*Node)(currPtr)

		for curr != nil {
			cmp := sl.comparator(curr.key, searchKey)
			if cmp == 0 {
				existingNode = curr
				break
			}
			if cmp > 0 {
				break
			}
			prev = curr
			currPtr = atomic.LoadPointer(&curr.forward[0])
			curr = (*Node)(currPtr)
			update[0] = prev
		}

		if existingNode != nil {
			existingNode.addVersion(newValue, writeTimestamp, Write)
			return
		}

		if topLevel == 0 {
			topLevel = sl.randomLevel()
		}

		if topLevel > currentLevel {
			for i := currentLevel; i < topLevel; i++ {
				update[i] = sl.header
			}
			newLevel := topLevel
			sl.level.CompareAndSwap(currentLevel, newLevel)
		}

		keyClone := make([]byte, len(searchKey))
		copy(keyClone, searchKey)

		newNode := &Node{
			key: keyClone,
		}
		newNode.addVersion(newValue, writeTimestamp, Write)

		for i := 0; i < topLevel; i++ {
			for {
				next := update[i]
				if next == nil {
					retry = true
					break
				}

				nextPtr := atomic.LoadPointer(&next.forward[i])
				nextNode := (*Node)(nextPtr)

				atomic.StorePointer(&newNode.forward[i], unsafe.Pointer(nextNode))

				if atomic.CompareAndSwapPointer(&next.forward[i], nextPtr, unsafe.Pointer(newNode)) {
					if i == 0 {
						// Update backward pointer for level 0
						if nextNode != nil {
							atomic.StorePointer(&nextNode.backward, unsafe.Pointer(newNode))
						}
						atomic.StorePointer(&newNode.backward, unsafe.Pointer(next))
					}
					break
				}
				retry = true
				break
			}
			if retry {
				break
			}
		}

		if retry {
			continue
		}
		return
	}
}

// Delete adds a delete marker in the version chain at the given timestamp
// This follows MVCC principles where deletes are just another version
func (sl *SkipList) Delete(searchKey []byte, deleteTimestamp int64) bool {
	var prev *Node
	var curr *Node
	var currPtr unsafe.Pointer

	// Start at the header node
	prev = sl.header

	// Get current level
	currentLevel := sl.getLevel()

	// For each level, starting at the highest level in the list
	for i := currentLevel - 1; i >= 0; i-- {
		// Get the next node (atomic load)
		currPtr = atomic.LoadPointer(&prev.forward[i])
		curr = (*Node)(currPtr)

		// Traverse the current level
		for curr != nil {
			// If the current key is greater or equal, stop traversing this level
			if sl.comparator(curr.key, searchKey) >= 0 {
				break
			}

			// Move forward
			prev = curr
			currPtr = atomic.LoadPointer(&curr.forward[i])
			curr = (*Node)(currPtr)
		}
	}

	// Check bottom level for exact match
	currPtr = atomic.LoadPointer(&prev.forward[0])
	curr = (*Node)(currPtr)

	// Search for the node at level 0
	for curr != nil {
		// Check if we found the key
		cmp := sl.comparator(curr.key, searchKey)
		if cmp == 0 {
			// Add a delete version
			curr.addVersion(nil, deleteTimestamp, Delete)
			return true
		}

		// If we've gone too far, the key doesn't exist
		if cmp > 0 {
			return false
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	return false
}

// NewIterator creates a new iterator starting at the given key
// If startKey is nil, the iterator starts at the beginning of the list
func (sl *SkipList) NewIterator(startKey []byte, readTimestamp int64) (*Iterator, error) {
	// Start at the header node
	curr := sl.header

	if startKey != nil && len(startKey) > 0 {
		// Traverse to find the node right before the start key
		for i := sl.getLevel() - 1; i >= 0; i-- {
			for {
				currPtr := atomic.LoadPointer(&curr.forward[i])
				next := (*Node)(currPtr)
				if next == nil || sl.comparator(next.key, startKey) >= 0 {
					break
				}
				curr = next
			}
		}
	}

	// Return the iterator with curr as current
	// First call to Next() will move to the appropriate node
	return &Iterator{
		SkipList:      sl,
		current:       curr,
		readTimestamp: readTimestamp,
	}, nil
}

// ToLast moves the iterator to the last node in the skip list
func (it *Iterator) ToLast() {
	// Move to the last node
	for {
		currPtr := atomic.LoadPointer(&it.current.forward[0])
		next := (*Node)(currPtr)
		if next == nil {
			break
		}
		it.current = next
	}
}

// Valid checks if the iterator is currently pointing to a valid node
func (it *Iterator) Valid() bool {
	return it.current != nil
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

// Value returns the current value and timestamp
func (it *Iterator) Value() ([]byte, int64, bool) {
	if it.current == nil {
		return nil, 0, false
	}

	// Find the visible version at the read timestamp
	version := it.current.findVisibleVersion(it.readTimestamp)
	if version != nil && version.Type != Delete {
		return version.Data, version.Timestamp, true
	}

	return nil, 0, false
}

// Next moves the iterator to the next node and returns the key and visible version
// Returns nil, nil, false if there are no more nodes or no visible versions
func (it *Iterator) Next() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// Move to the next node
	currPtr := atomic.LoadPointer(&it.current.forward[0])
	it.current = (*Node)(currPtr)

	// Return the key and visible version at the read timestamp
	if it.current != nil {
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, recursively try the next node
		return it.Next()
	}

	return nil, nil, 0, false
}

// Prev moves the iterator to the previous node and returns the key and visible version
// Returns nil, nil, false if there are no more nodes or no visible versions
func (it *Iterator) Prev() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// Move to the previous node
	currPtr := atomic.LoadPointer(&it.current.backward)
	it.current = (*Node)(currPtr)

	// Return the key and visible version at the read timestamp
	if it.current != nil && it.current != it.SkipList.header {
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, recursively try the previous node
		return it.Prev()
	}

	return nil, nil, 0, false
}

// Peek peek ahead without advancing the iterator
func (it *Iterator) Peek() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// If we're at the header, advance to first valid node
	if it.current == it.SkipList.header {
		currPtr := atomic.LoadPointer(&it.current.forward[0])
		next := (*Node)(currPtr)

		for next != nil {
			version := next.findVisibleVersion(it.readTimestamp)
			if version != nil && version.Type != Delete {
				return next.key, version.Data, version.Timestamp, true
			}
			currPtr = atomic.LoadPointer(&next.forward[0])
			next = (*Node)(currPtr)
		}
		return nil, nil, 0, false
	}

	// Return current node's data if visible
	version := it.current.findVisibleVersion(it.readTimestamp)
	if version != nil && version.Type != Delete {
		return it.current.key, version.Data, version.Timestamp, true
	}

	return nil, nil, 0, false
}

// GetMin retrieves the minimum key-value pair from the skip list
// Returns the key, value, and a boolean indicating success
func (sl *SkipList) GetMin(readTimestamp int64) ([]byte, []byte, bool) {
	// Start at the header node
	curr := sl.header

	// Get the first node at level 0
	currPtr := atomic.LoadPointer(&curr.forward[0])
	curr = (*Node)(currPtr)

	// If the list is empty, return false
	if curr == nil {
		return nil, nil, false
	}

	// Find the first node with a visible version
	for curr != nil {
		version := curr.findVisibleVersion(readTimestamp)
		if version != nil && version.Type != Delete {
			return curr.key, version.Data, true
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	// No visible nodes found
	return nil, nil, false
}

// GetMax retrieves the maximum key-value pair from the skip list
// Returns the key, value, and a boolean indicating success
func (sl *SkipList) GetMax(readTimestamp int64) ([]byte, []byte, bool) {
	// Start at the header node
	curr := sl.header
	var lastVisible *Node
	var lastVisibleVersion *ValueVersion

	// Traverse the list to find the last node with a visible version
	for {
		currPtr := atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)

		if curr == nil {
			break
		}

		version := curr.findVisibleVersion(readTimestamp)
		if version != nil && version.Type != Delete {
			lastVisible = curr
			lastVisibleVersion = version
		}
	}

	// If we found a visible node, return it
	if lastVisible != nil {
		if lastVisibleVersion == nil {
			return nil, nil, false
		}
		return lastVisible.key, lastVisibleVersion.Data, true
	}

	// No visible nodes found
	return nil, nil, false
}

// Count returns the number of entries visible at the given timestamp
func (sl *SkipList) Count(readTimestamp int64) int {
	// Start at the header node
	curr := sl.header
	count := 0

	// Traverse all nodes at level 0
	currPtr := atomic.LoadPointer(&curr.forward[0])
	curr = (*Node)(currPtr)

	for curr != nil {
		// Check if this node has a visible version at the given timestamp
		version := curr.findVisibleVersion(readTimestamp)
		if version != nil && version.Type != Delete {
			count++
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	return count
}

// DeleteCount returns the number of delete operations visible at the given timestamp
func (sl *SkipList) DeleteCount(readTimestamp int64) int {
	// Start at the header node
	curr := sl.header
	count := 0

	// Traverse all nodes at level 0
	currPtr := atomic.LoadPointer(&curr.forward[0])
	curr = (*Node)(currPtr)

	for curr != nil {
		// Check if this node has a visible version at the given timestamp
		version := curr.findVisibleVersion(readTimestamp)
		if version != nil && version.Type == Delete {
			count++
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	return count
}

// hasPrefix checks if the key has the specified prefix
func hasPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	return bytes.Equal(key[:len(prefix)], prefix)
}

// isInRange checks if the key is within the specified range [startKey, endKey)
func (sl *SkipList) isInRange(key, startKey, endKey []byte) bool {
	// Check if key >= startKey
	if startKey != nil && sl.comparator(key, startKey) < 0 {
		return false
	}
	// Check if key < endKey
	if endKey != nil && sl.comparator(key, endKey) >= 0 {
		return false
	}
	return true
}

// NewPrefixIterator creates a new prefix iterator starting at the first key with the given prefix
func (sl *SkipList) NewPrefixIterator(prefix []byte, readTimestamp int64) (*PrefixIterator, error) {
	if prefix == nil || len(prefix) == 0 {
		return nil, errors.New("prefix cannot be nil or empty")
	}

	// Start at the header node
	curr := sl.header

	// Traverse to find the first node with the prefix or the position where it would be
	for i := sl.getLevel() - 1; i >= 0; i-- {
		for {
			currPtr := atomic.LoadPointer(&curr.forward[i])
			next := (*Node)(currPtr)
			if next == nil || sl.comparator(next.key, prefix) >= 0 {
				break
			}
			curr = next
		}
	}

	// Return the iterator positioned before the first matching node
	return &PrefixIterator{
		SkipList:      sl,
		current:       curr,
		readTimestamp: readTimestamp,
		prefix:        append([]byte(nil), prefix...), // Copy the prefix
	}, nil
}

// NewRangeIterator creates a new range iterator for keys in [startKey, endKey)
// If startKey is nil, iteration starts from the beginning
// If endKey is nil, iteration continues to the end
func (sl *SkipList) NewRangeIterator(startKey, endKey []byte, readTimestamp int64) (*RangeIterator, error) {
	if startKey != nil && endKey != nil &&
		len(startKey) > 0 && len(endKey) > 0 {
		cmp := sl.comparator(startKey, endKey)
		if cmp > 0 {
			return nil, errors.New("startKey must be less than endKey")
		}
	}

	// Start at the header node
	curr := sl.header

	if startKey != nil && len(startKey) > 0 {
		// Traverse to find the node right before the start key
		for i := sl.getLevel() - 1; i >= 0; i-- {
			for {
				currPtr := atomic.LoadPointer(&curr.forward[i])
				next := (*Node)(currPtr)
				if next == nil || sl.comparator(next.key, startKey) >= 0 {
					break
				}
				curr = next
			}
		}
	}

	return &RangeIterator{
		SkipList:      sl,
		current:       curr,
		readTimestamp: readTimestamp,
		startKey:      append([]byte(nil), startKey...), // Copy startKey
		endKey:        append([]byte(nil), endKey...),   // Copy endKey
	}, nil
}

// Key returns the current key for the prefix iterator
func (it *PrefixIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

// ToLast moves the prefix iterator to the last node with the matching prefix
func (it *PrefixIterator) ToLast() {
	// Move to the last node with the prefix
	for {
		currPtr := atomic.LoadPointer(&it.current.forward[0])
		next := (*Node)(currPtr)
		if next == nil || !hasPrefix(next.key, it.prefix) {
			break
		}
		it.current = next
	}
}

// Value returns the current value and timestamp for the prefix iterator
func (it *PrefixIterator) Value() ([]byte, int64, bool) {
	if it.current == nil {
		return nil, 0, false
	}

	// Find the visible version at the read timestamp
	version := it.current.findVisibleVersion(it.readTimestamp)
	if version != nil && version.Type != Delete {
		return version.Data, version.Timestamp, true
	}

	return nil, 0, false

}

// Valid checks if the prefix iterator is currently pointing to a valid node
func (it *PrefixIterator) Valid() bool {
	return it.current != nil
}

// Next moves the prefix iterator to the next node with the matching prefix
// Returns key, value, timestamp, and success flag
func (it *PrefixIterator) Next() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// Move to the next node
	currPtr := atomic.LoadPointer(&it.current.forward[0])
	it.current = (*Node)(currPtr)

	// Check if we have a valid node and it matches the prefix
	if it.current != nil {
		// Check if the key has the required prefix
		if !hasPrefix(it.current.key, it.prefix) {
			// No more keys with this prefix
			it.current = nil
			return nil, nil, 0, false
		}

		// Find visible version at the read timestamp
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, try the next node
		return it.Next()
	}

	return nil, nil, 0, false
}

// Prev moves the prefix iterator to the previous node with the matching prefix
// Returns key, value, timestamp, and success flag
func (it *PrefixIterator) Prev() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		// If current is nil, we need to find the last node with the prefix
		curr := it.SkipList.header
		var lastValidNode *Node

		// Traverse to find the last node with the prefix
		currPtr := atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)

		for curr != nil {
			if hasPrefix(curr.key, it.prefix) {
				version := curr.findVisibleVersion(it.readTimestamp)
				if version != nil && version.Type != Delete {
					lastValidNode = curr
				}
			} else if it.SkipList.comparator(curr.key, it.prefix) > 0 {
				// We've gone past all keys with this prefix
				break
			}

			currPtr = atomic.LoadPointer(&curr.forward[0])
			curr = (*Node)(currPtr)
		}

		if lastValidNode != nil {
			it.current = lastValidNode
			version := it.current.findVisibleVersion(it.readTimestamp)
			return it.current.key, version.Data, version.Timestamp, true
		}
		return nil, nil, 0, false
	}

	// Move to the previous node
	currPtr := atomic.LoadPointer(&it.current.backward)
	it.current = (*Node)(currPtr)

	// Check if we have a valid node and it's not the header
	if it.current != nil && it.current != it.SkipList.header {
		// Check if the key has the required prefix
		if !hasPrefix(it.current.key, it.prefix) {
			// No more keys with this prefix in the backward direction
			it.current = nil
			return nil, nil, 0, false
		}

		// Find visible version at the read timestamp
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, try the previous node
		return it.Prev()
	}

	return nil, nil, 0, false
}

// Peek returns the current key-value pair without moving the iterator
func (it *PrefixIterator) Peek() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// If we're at the header, find first valid node with prefix
	if it.current == it.SkipList.header {
		currPtr := atomic.LoadPointer(&it.current.forward[0])
		next := (*Node)(currPtr)

		for next != nil {
			if hasPrefix(next.key, it.prefix) {
				version := next.findVisibleVersion(it.readTimestamp)
				if version != nil && version.Type != Delete {
					return next.key, version.Data, version.Timestamp, true
				}
			} else if it.SkipList.comparator(next.key, it.prefix) > 0 {
				// Gone past prefix range
				break
			}
			currPtr = atomic.LoadPointer(&next.forward[0])
			next = (*Node)(currPtr)
		}
		return nil, nil, 0, false
	}

	// Check if current node has the prefix and is visible
	if hasPrefix(it.current.key, it.prefix) {
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}
	}

	return nil, nil, 0, false
}

// Key returns the current key of the range iterator
func (it *RangeIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

// ToLast moves the range iterator to the last node within the specified range
func (it *RangeIterator) ToLast() {
	// Find the last node within the range
	var lastValidNode *Node = nil

	// Start from current position and scan forward to find last valid node
	curr := it.current
	for {
		if curr == nil {
			break
		}

		// Check if current node is in range and has visible data
		if it.SkipList.isInRange(curr.key, it.startKey, it.endKey) {
			version := curr.findVisibleVersion(it.readTimestamp)
			if version != nil && version.Type != Delete {
				lastValidNode = curr
			}
		}

		// Move to next node
		currPtr := atomic.LoadPointer(&curr.forward[0])
		next := (*Node)(currPtr)

		// Stop if next node is out of range
		if next == nil || !it.SkipList.isInRange(next.key, it.startKey, it.endKey) {
			break
		}

		curr = next
	}

	// Set current to the last valid node found
	if lastValidNode != nil {
		it.current = lastValidNode
	} else {
		// No valid nodes in range
		it.current = nil
	}
}

// Valid checks if the range iterator is currently positioned at a valid node
func (it *RangeIterator) Valid() bool {
	return it.current != nil
}

// Value gets the current value and timestamp
func (it *RangeIterator) Value() ([]byte, int64, bool) {
	if it.current == nil {
		return nil, 0, false
	}

	// Find the visible version at the read timestamp
	version := it.current.findVisibleVersion(it.readTimestamp)
	if version != nil && version.Type != Delete {
		return version.Data, version.Timestamp, true
	}

	return nil, 0, false

}

// Next moves the range iterator to the next node within the range
// Returns key, value, timestamp, and success flag
func (it *RangeIterator) Next() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// Move to the next node
	currPtr := atomic.LoadPointer(&it.current.forward[0])
	it.current = (*Node)(currPtr)

	// Check if we have a valid node and it's within the range
	if it.current != nil {
		// Check if the key is within the range
		if !it.SkipList.isInRange(it.current.key, it.startKey, it.endKey) {
			// We've moved outside the range
			it.current = nil
			return nil, nil, 0, false
		}

		// Find visible version at the read timestamp
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, try the next node
		return it.Next()
	}

	return nil, nil, 0, false
}

// Prev moves the range iterator to the previous node within the range
// Returns key, value, timestamp, and success flag
func (it *RangeIterator) Prev() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		// If current is nil, we need to find the last node in the range
		curr := it.SkipList.header
		var lastValidNode *Node

		// Traverse to find the last node in the range
		currPtr := atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)

		for curr != nil {
			if it.SkipList.isInRange(curr.key, it.startKey, it.endKey) {
				version := curr.findVisibleVersion(it.readTimestamp)
				if version != nil && version.Type != Delete {
					lastValidNode = curr
				}
			} else if it.endKey != nil && it.SkipList.comparator(curr.key, it.endKey) >= 0 {
				// We've gone past the end of the range
				break
			}

			currPtr = atomic.LoadPointer(&curr.forward[0])
			curr = (*Node)(currPtr)
		}

		if lastValidNode != nil {
			it.current = lastValidNode
			version := it.current.findVisibleVersion(it.readTimestamp)
			return it.current.key, version.Data, version.Timestamp, true
		}
		return nil, nil, 0, false
	}

	// Move to the previous node
	currPtr := atomic.LoadPointer(&it.current.backward)
	it.current = (*Node)(currPtr)

	// Check if we have a valid node and it's not the header
	if it.current != nil && it.current != it.SkipList.header {
		// Check if the key is within the range
		if !it.SkipList.isInRange(it.current.key, it.startKey, it.endKey) {
			// We've moved outside the range
			it.current = nil
			return nil, nil, 0, false
		}

		// Find visible version at the read timestamp
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, try the previous node
		return it.Prev()
	}

	return nil, nil, 0, false
}

// Peek returns the current key-value pair without moving the iterator
func (it *RangeIterator) Peek() ([]byte, []byte, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// If we're at the header, find first valid node in range
	if it.current == it.SkipList.header {
		currPtr := atomic.LoadPointer(&it.current.forward[0])
		next := (*Node)(currPtr)

		for next != nil {
			if it.SkipList.isInRange(next.key, it.startKey, it.endKey) {
				version := next.findVisibleVersion(it.readTimestamp)
				if version != nil && version.Type != Delete {
					return next.key, version.Data, version.Timestamp, true
				}
			} else if it.endKey != nil && it.SkipList.comparator(next.key, it.endKey) >= 0 {
				// Gone past end of range
				break
			}
			currPtr = atomic.LoadPointer(&next.forward[0])
			next = (*Node)(currPtr)
		}
		return nil, nil, 0, false
	}

	// Check if current node is in range and visible
	if it.SkipList.isInRange(it.current.key, it.startKey, it.endKey) {
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}
	}

	return nil, nil, 0, false
}

// GetLatestTimestamp retrieves the latest timestamp across all nodes in the skip list
func (sl *SkipList) GetLatestTimestamp() int64 {
	var latestTimestamp int64 = 0

	// Start at the header node
	curr := sl.header

	// Traverse all nodes at level 0 (bottom level contains all nodes)
	currPtr := atomic.LoadPointer(&curr.forward[0])
	curr = (*Node)(currPtr)

	for curr != nil {
		// Get the latest version for this node (head of version chain)
		latestVersion := curr.getLatestVersion()

		// Check if this version has a newer timestamp
		if latestVersion != nil && latestVersion.Timestamp > latestTimestamp {
			latestTimestamp = latestVersion.Timestamp
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	return latestTimestamp
}

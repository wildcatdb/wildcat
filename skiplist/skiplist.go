// Package skiplist
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
package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const MaxLevel = 16
const p = 0.25

type ValueVersionType int

const (
	// Write represents a write operation (insert or update)
	Write ValueVersionType = iota
	// Delete represents a delete operation
	Delete
)

// ValueVersion represents a single version of a value in MVCC
type ValueVersion struct {
	Data      interface{}      // Value data
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
	header   *Node        // special header node
	level    atomic.Value // current maximum level of the list (atomic)
	rng      *rand.Rand   // random number generator with its own lock
	rngMutex sync.Mutex   // mutex for the random number generator
}

// Iterator provides a bidirectional iterator for the skip list with snapshot isolation
type Iterator struct {
	skipList      *SkipList // Reference to the skip list
	current       *Node     // Current node in the iteration
	readTimestamp int64     // Timestamp for snapshot isolation
}

// New creates a new concurrent skip list with MVCC
func New() *SkipList {
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
		header: header,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
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
func (n *Node) addVersion(data interface{}, timestamp int64, versionType ValueVersionType) {
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
func (sl *SkipList) Get(searchKey []byte, readTimestamp int64) interface{} {
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
			if bytes.Compare(curr.key, searchKey) >= 0 {
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
		if bytes.Equal(curr.key, searchKey) {
			// Find the visible version at the given read timestamp
			version := curr.findVisibleVersion(readTimestamp)
			if version != nil && version.Type != Delete {
				return version.Data
			}
			return nil
		}

		// If we've gone too far, the key doesn't exist
		if bytes.Compare(curr.key, searchKey) > 0 {
			return nil
		}

		// Move to the next node
		currPtr = atomic.LoadPointer(&curr.forward[0])
		curr = (*Node)(currPtr)
	}

	return nil
}

// Put inserts or updates a value in the skip list with the given key
func (sl *SkipList) Put(searchKey []byte, newValue interface{}, writeTimestamp int64) {
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
				if bytes.Compare(curr.key, searchKey) >= 0 {
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
			if bytes.Equal(curr.key, searchKey) {
				existingNode = curr
				break
			}
			if bytes.Compare(curr.key, searchKey) > 0 {
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
			if bytes.Compare(curr.key, searchKey) >= 0 {
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
		if bytes.Equal(curr.key, searchKey) {
			// Add a delete version
			curr.addVersion(nil, deleteTimestamp, Delete)
			return true
		}

		// If we've gone too far, the key doesn't exist
		if bytes.Compare(curr.key, searchKey) > 0 {
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
func (sl *SkipList) NewIterator(startKey []byte, readTimestamp int64) *Iterator {
	// Start at the header node
	curr := sl.header

	if startKey != nil && len(startKey) > 0 {
		// Traverse to find the node right before the start key
		for i := sl.getLevel() - 1; i >= 0; i-- {
			for {
				currPtr := atomic.LoadPointer(&curr.forward[i])
				next := (*Node)(currPtr)
				if next == nil || bytes.Compare(next.key, startKey) >= 0 {
					break
				}
				curr = next
			}
		}
	}

	// Return the iterator with curr as current
	// First call to Next() will move to the appropriate node
	return &Iterator{
		skipList:      sl,
		current:       curr,
		readTimestamp: readTimestamp,
	}
}

// Next moves the iterator to the next node and returns the key and visible version
// Returns nil, nil, false if there are no more nodes or no visible versions
func (it *Iterator) Next() ([]byte, interface{}, int64, bool) {
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
func (it *Iterator) Prev() ([]byte, interface{}, int64, bool) {
	if it.current == nil {
		return nil, nil, 0, false
	}

	// Move to the previous node
	currPtr := atomic.LoadPointer(&it.current.backward)
	it.current = (*Node)(currPtr)

	// Return the key and visible version at the read timestamp
	if it.current != nil && it.current != it.skipList.header {
		version := it.current.findVisibleVersion(it.readTimestamp)
		if version != nil && version.Type != Delete {
			return it.current.key, version.Data, version.Timestamp, true
		}

		// If the current node doesn't have a visible version, recursively try the previous node
		return it.Prev()
	}

	return nil, nil, 0, false
}

// GetMin retrieves the minimum key-value pair from the skip list
// Returns the key, value, and a boolean indicating success
func (sl *SkipList) GetMin(readTimestamp int64) ([]byte, interface{}, bool) {
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
func (sl *SkipList) GetMax(readTimestamp int64) ([]byte, interface{}, bool) {
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

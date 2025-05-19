// Package lru
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
package lru

import (
	"math"
	"runtime"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"
)

// Node represents a node in the linked list
type Node struct {
	key       interface{}
	value     interface{}
	accessCnt uint64         // Count of accesses, atomically updated
	timestamp int64          // Time of insertion
	next      unsafe.Pointer // *Node - using unsafe.Pointer for atomic operations
	prev      unsafe.Pointer // *Node - using unsafe.Pointer for atomic operations
}

// LRU is a lockless linked list with capacity constraints
type LRU struct {
	head         unsafe.Pointer // *Node
	tail         unsafe.Pointer // *Node
	length       int64          // Current length of the list
	capacity     int64          // Maximum capacity
	evictRatio   float64        // Ratio of nodes to evict when capacity is reached
	accessWeight float64        // Weight of access count in the eviction score
	timeWeight   float64        // Weight of time in the eviction score
}

// New creates a new lru atomic linked list
func New(capacity int64, evictRatio float64, accessWeight float64) *LRU {
	if capacity <= 0 {
		capacity = math.MaxInt64 // Default to "unlimited"
	}
	if evictRatio <= 0 || evictRatio >= 1 {
		evictRatio = 0.25 // Default to 25%
	}
	if accessWeight < 0 || accessWeight > 1 {
		accessWeight = 0.7 // Default to 70% weight for access count
	}

	// Create a sentinel node for the head and tail
	sentinel := &Node{
		key:       nil,
		value:     nil,
		accessCnt: 0,
		timestamp: time.Now().UnixNano(),
	}

	return &LRU{
		head:         unsafe.Pointer(sentinel),
		tail:         unsafe.Pointer(sentinel),
		length:       0,
		capacity:     capacity,
		evictRatio:   evictRatio,
		accessWeight: accessWeight,
		timeWeight:   1 - accessWeight,
	}
}

// Get retrieves a value by key and updates access count
func (list *LRU) Get(key interface{}) (interface{}, bool) {
	// Start from the head
	current := (*Node)(atomic.LoadPointer(&list.head))

	// Skip the sentinel node
	current = (*Node)(atomic.LoadPointer(&current.next))

	// Traverse the list
	for current != nil {
		if current.key == key {
			// Increment access count atomically
			atomic.AddUint64(&current.accessCnt, 1)
			return current.value, true
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
	return nil, false
}

// Put adds or updates a key-value pair
func (list *LRU) Put(key, value interface{}) bool {
	// First check if the key already exists
	current := (*Node)(atomic.LoadPointer(&list.head))

	// Skip the sentinel node
	current = (*Node)(atomic.LoadPointer(&current.next))

	// Traverse the list to find existing key
	for current != nil {
		if current.key == key {
			// Update value and access count
			current.value = value
			atomic.AddUint64(&current.accessCnt, 1)
			return true
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}

	// Check if we need to evict before adding
	if atomic.LoadInt64(&list.length) >= list.capacity {
		list.evict()
	}

	// Create new node
	newNode := &Node{
		key:       key,
		value:     value,
		accessCnt: 1,
		timestamp: time.Now().UnixNano(),
		next:      nil,
	}

	// Add node to the list using CAS
	for {
		tail := (*Node)(atomic.LoadPointer(&list.tail))

		// Try to set the next pointer of the tail
		if atomic.CompareAndSwapPointer(&tail.next, nil, unsafe.Pointer(newNode)) {
			// Set prev pointer of new node
			atomic.StorePointer(&newNode.prev, unsafe.Pointer(tail))

			// Try to update the tail
			atomic.CompareAndSwapPointer(&list.tail, unsafe.Pointer(tail), unsafe.Pointer(newNode))

			// Increment length
			atomic.AddInt64(&list.length, 1)
			return true
		}

		// If CAS failed, help update tail pointer
		nextTail := (*Node)(atomic.LoadPointer(&tail.next))
		if nextTail != nil {
			atomic.CompareAndSwapPointer(&list.tail, unsafe.Pointer(tail), unsafe.Pointer(nextTail))
		}

		// Add a small backoff to reduce contention
		runtime.Gosched()
	}
}

// Delete removes a node by key
func (list *LRU) Delete(key interface{}) bool {
	// Start from the head
	current := (*Node)(atomic.LoadPointer(&list.head))

	// Skip the sentinel node
	current = (*Node)(atomic.LoadPointer(&current.next))

	// Traverse the list
	for current != nil {
		if current.key == key {
			// Found the node to delete
			// Get prev and next nodes
			prev := (*Node)(atomic.LoadPointer(&current.prev))
			next := (*Node)(atomic.LoadPointer(&current.next))

			// Try to update prev.next to skip current
			if prev != nil {
				atomic.CompareAndSwapPointer(&prev.next, unsafe.Pointer(current), unsafe.Pointer(next))
			}

			// Try to update next.prev to skip current
			if next != nil {
				atomic.CompareAndSwapPointer(&next.prev, unsafe.Pointer(current), unsafe.Pointer(prev))
			}

			// Special case: if deleting the tail
			if next == nil {
				atomic.CompareAndSwapPointer(&list.tail, unsafe.Pointer(current), unsafe.Pointer(prev))
			}

			// Decrement length
			atomic.AddInt64(&list.length, -1)
			return true
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
	return false
}

// Length returns the current length of the list
func (list *LRU) Length() int64 {
	return atomic.LoadInt64(&list.length)
}

// evict removes a proportion of least accessed nodes
func (list *LRU) evict() {
	// Calculate number of nodes to evict
	length := atomic.LoadInt64(&list.length)
	toEvict := int(float64(length) * list.evictRatio)
	if toEvict < 1 {
		toEvict = 1
	}

	// Build a slice of nodes to calculate eviction scores
	nodes := make([]*Node, 0, length)
	current := (*Node)(atomic.LoadPointer(&list.head))
	current = (*Node)(atomic.LoadPointer(&current.next)) // Skip sentinel

	for current != nil {
		nodes = append(nodes, current)
		current = (*Node)(atomic.LoadPointer(&current.next))
	}

	// Skip if there are no nodes (shouldn't happen)
	if len(nodes) == 0 {
		return
	}

	// Calculate scores for eviction
	// Find max access count and newest timestamp for normalization
	maxAccess := uint64(1)
	var newestTime int64 = nodes[0].timestamp
	var oldestTime int64 = nodes[0].timestamp

	for _, node := range nodes {
		if atomic.LoadUint64(&node.accessCnt) > maxAccess {
			maxAccess = atomic.LoadUint64(&node.accessCnt)
		}
		if node.timestamp > newestTime {
			newestTime = node.timestamp
		}
		if node.timestamp < oldestTime {
			oldestTime = node.timestamp
		}
	}

	// Avoid division by zero
	if newestTime == oldestTime {
		newestTime = oldestTime + 1
	}

	// Calculate scores (lower is more evictable)
	type scoredNode struct {
		node  *Node
		score float64
	}

	scoredNodes := make([]scoredNode, len(nodes))
	for i, node := range nodes {
		// Normalize access count (0-1, higher is better)
		accessNorm := float64(atomic.LoadUint64(&node.accessCnt)) / float64(maxAccess)

		// Normalize time (0-1, newer is better)
		timeNorm := float64(node.timestamp-oldestTime) / float64(newestTime-oldestTime)

		// Calculate score - higher means less likely to evict
		score := (list.accessWeight * accessNorm) + (list.timeWeight * timeNorm)

		scoredNodes[i] = scoredNode{node: node, score: score}
	}

	// Sort by score (ascending, lowest first)
	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score < scoredNodes[j].score
	})

	// Evict the nodes with lowest scores
	for i := 0; i < toEvict && i < len(scoredNodes); i++ {
		list.Delete(scoredNodes[i].node.key)
	}
}

// ForEach iterates through the list safely and applies a function to each node
func (list *LRU) ForEach(fn func(key, value interface{}, accessCount uint64) bool) {
	current := (*Node)(atomic.LoadPointer(&list.head))

	// Skip the sentinel node
	current = (*Node)(atomic.LoadPointer(&current.next))

	for current != nil {
		// Increment access count
		accesses := atomic.LoadUint64(&current.accessCnt)

		// Apply function
		if !fn(current.key, current.value, accesses) {
			break
		}

		// Move to next node
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
}

// Clear empties the list
func (list *LRU) Clear() {
	// Create a new sentinel node
	sentinel := &Node{
		key:       nil,
		value:     nil,
		accessCnt: 0,
		timestamp: time.Now().UnixNano(),
	}

	// Reset head and tail
	atomic.StorePointer(&list.head, unsafe.Pointer(sentinel))
	atomic.StorePointer(&list.tail, unsafe.Pointer(sentinel))

	// Reset length
	atomic.StoreInt64(&list.length, 0)
}

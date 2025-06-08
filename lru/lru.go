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
	"github.com/wildcatdb/wildcat/v2/queue"
	"math"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

type EvictionCallback func(key, value interface{})

// ValueWrapper is a wrapper for values stored in the LRU list
type ValueWrapper struct {
	data interface{}
}

// Node represents a node in the linked list
type Node struct {
	key               interface{}
	value             unsafe.Pointer // *ValueWrapper
	accessCnt         uint64
	timestamp         int64
	next              unsafe.Pointer // *Node
	prev              unsafe.Pointer // *Node
	onEvict           EvictionCallback
	markedForEviction int32 // atomic flag(0=normal, 1=marked for eviction)
}

// LRU is a lockless linked list with lazy eviction and anti-thrashing mechanisms
type LRU struct {
	head             unsafe.Pointer // *Node
	tail             unsafe.Pointer // *Node
	length           int64
	capacity         int64
	evictRatio       float64
	accessWeight     float64
	timeWeight       float64
	evictionQueue    *queue.Queue
	evicting         int32 // Prevent recursive eviction
	lastProgressTime int64 // Track when we last made progress
	stuckCounter     int32 // Count consecutive stuck operations
}

// New creates a new lru atomic linked list with lazy eviction
func New(capacity int64, evictRatio float64, accessWeight float64) *LRU {
	if capacity <= 0 {
		capacity = math.MaxInt64
	}
	if evictRatio <= 0 || evictRatio >= 1 {
		evictRatio = 0.25
	}
	if accessWeight < 0 || accessWeight > 1 {
		accessWeight = 0.7
	}

	valueWrapper := &ValueWrapper{data: nil}
	sentinel := &Node{
		key:       nil,
		value:     unsafe.Pointer(valueWrapper),
		accessCnt: 0,
		timestamp: time.Now().UnixNano(),
	}

	lru := &LRU{
		head:             unsafe.Pointer(sentinel),
		tail:             unsafe.Pointer(sentinel),
		length:           0,
		capacity:         capacity,
		evictRatio:       evictRatio,
		accessWeight:     accessWeight,
		timeWeight:       1 - accessWeight,
		evictionQueue:    queue.New(),
		lastProgressTime: time.Now().UnixNano(),
	}

	return lru
}

// shouldEvictNode determines if a node should be evicted with balanced approach
func (list *LRU) shouldEvictNode(node *Node, currentTime int64) bool {
	// Don't evict if already marked
	if atomic.LoadInt32(&node.markedForEviction) == 1 {
		return false
	}

	currentLength := atomic.LoadInt64(&list.length)
	loadFactor := float64(currentLength) / float64(list.capacity)

	accessCount := atomic.LoadUint64(&node.accessCnt)
	age := currentTime - node.timestamp

	// Only evict if we're actually over capacity or very close
	if loadFactor < 0.95 {
		return false // Don't evict unless we're at 95%+ capacity
	}

	// At capacity or over, an immediate eviction needed
	if loadFactor >= 1.0 {
		// Evict nodes with low access count regardless of age
		return accessCount <= 2
	}

	// Very close to capacity (95%+), must be more selective
	if loadFactor >= 0.95 {
		// Evict old nodes with very low access
		return age > 50*time.Millisecond.Nanoseconds() && accessCount <= 1 // Evict nodes that are old and have low access count
	}

	return false
}

// lazyEvictDuringTraversal checks if we should evict the current node during traversal
func (list *LRU) lazyEvictDuringTraversal(node *Node) {
	// Only do lazy eviction if we're actually over capacity
	currentLength := atomic.LoadInt64(&list.length)
	if currentLength <= list.capacity {
		return // Don't evict if we're not over capacity
	}

	currentTime := time.Now().UnixNano()
	if list.shouldEvictNode(node, currentTime) {
		// Mark for eviction atomically
		if atomic.CompareAndSwapInt32(&node.markedForEviction, 0, 1) {
			// Successfully marked, add to eviction queue
			list.evictionQueue.Enqueue(node)
		}
	}
}

// detectAndRecoverFromStuck detects stuck states and recovers
func (list *LRU) detectAndRecoverFromStuck() bool {
	now := time.Now().UnixNano()
	lastProgress := atomic.LoadInt64(&list.lastProgressTime)

	// If no progress for 10ms, we might be stuck
	if now-lastProgress > 10*time.Millisecond.Nanoseconds() {
		stuckCount := atomic.AddInt32(&list.stuckCounter, 1)

		if stuckCount > 5 {
			// Emergency!
			list.emergencyRecovery()
			atomic.StoreInt32(&list.stuckCounter, 0)
			return true
		}
	} else {
		atomic.StoreInt32(&list.stuckCounter, 0)
	}

	atomic.StoreInt64(&list.lastProgressTime, now)
	return false
}

// emergencyRecovery performs emergency recovery from stuck state
func (list *LRU) emergencyRecovery() {
	// Clear eviction queue
	for list.evictionQueue.Dequeue() != nil {
		// Draining up
	}

	list.repairTailPointer()

	// Reset all eviction flags
	current := (*Node)(atomic.LoadPointer(&list.head))
	for current != nil {
		atomic.StoreInt32(&current.markedForEviction, 0)
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
}

// repairTailPointer fixes corrupted tail pointer by walking the list
func (list *LRU) repairTailPointer() {
	// Walk from head to find actual tail
	current := (*Node)(atomic.LoadPointer(&list.head))
	var actualTail = current

	for current != nil {
		next := (*Node)(atomic.LoadPointer(&current.next))
		if next == nil {
			actualTail = current
			break
		}
		current = next
	}

	// Update tail pointer
	atomic.StorePointer(&list.tail, unsafe.Pointer(actualTail))
}

// reuseOrCreateNode attempts to reuse an evicted node or creates a new one
func (list *LRU) reuseOrCreateNode(key, value interface{}, onEvict EvictionCallback) *Node {

	// Try to reuse an evicted node first
	if reusedNode := list.evictionQueue.Dequeue(); reusedNode != nil {
		// Call eviction callback for the old data
		if reusedNode.(*Node).onEvict != nil {
			valuePtr := atomic.LoadPointer(&reusedNode.(*Node).value)
			if valuePtr != nil {
				oldValue := (*ValueWrapper)(valuePtr)
				reusedNode.(*Node).onEvict(reusedNode.(*Node).key, oldValue.data)
			}
		}

		// Reset and reuse the node
		valueWrapper := &ValueWrapper{data: value}
		reusedNode.(*Node).key = key
		atomic.StorePointer(&reusedNode.(*Node).value, unsafe.Pointer(valueWrapper))
		atomic.StoreUint64(&reusedNode.(*Node).accessCnt, 1)
		reusedNode.(*Node).timestamp = time.Now().UnixNano()
		atomic.StorePointer(&reusedNode.(*Node).next, nil)
		atomic.StorePointer(&reusedNode.(*Node).prev, nil)
		reusedNode.(*Node).onEvict = onEvict
		atomic.StoreInt32(&reusedNode.(*Node).markedForEviction, 0)

		return reusedNode.(*Node)
	}

	// Create new node if no reused node available
	valueWrapper := &ValueWrapper{data: value}
	return &Node{
		key:               key,
		value:             unsafe.Pointer(valueWrapper),
		accessCnt:         1,
		timestamp:         time.Now().UnixNano(),
		next:              nil,
		prev:              nil,
		onEvict:           onEvict,
		markedForEviction: 0,
	}
}

// Get retrieves a value by key with lazy eviction
func (list *LRU) Get(key interface{}) (interface{}, bool) {
	// Process eviction queue to clean up marked nodes
	list.processEvictionQueue()

	current := (*Node)(atomic.LoadPointer(&list.head))
	current = (*Node)(atomic.LoadPointer(&current.next))

	for current != nil {
		// Check if this node should be lazily evicted
		list.lazyEvictDuringTraversal(current)

		// Skip nodes marked for eviction
		if atomic.LoadInt32(&current.markedForEviction) == 1 {
			current = (*Node)(atomic.LoadPointer(&current.next))
			continue
		}

		if current.key == key {
			atomic.AddUint64(&current.accessCnt, 1)
			valuePtr := atomic.LoadPointer(&current.value)
			value := (*ValueWrapper)(valuePtr)

			// Mark progress
			atomic.StoreInt64(&list.lastProgressTime, time.Now().UnixNano())
			return value.data, true
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
	return nil, false
}

// Put adds or updates a key-value pair with anti-thrashing mechanisms
func (list *LRU) Put(key, value interface{}, onEvict ...EvictionCallback) bool {
	var evictCallback EvictionCallback
	if len(onEvict) > 0 {
		evictCallback = onEvict[0]
	}

	// Check for stuck state
	if list.detectAndRecoverFromStuck() {
		runtime.Gosched() // Give other goroutines a chance
	}

	// Process eviction queue to clean up marked nodes
	list.processEvictionQueue()

	// Check if key already exists
	current := (*Node)(atomic.LoadPointer(&list.head))
	current = (*Node)(atomic.LoadPointer(&current.next))

	for current != nil {
		// Lazy eviction check
		list.lazyEvictDuringTraversal(current)

		// Skip nodes marked for eviction
		if atomic.LoadInt32(&current.markedForEviction) == 1 {
			current = (*Node)(atomic.LoadPointer(&current.next))
			continue
		}

		if current.key == key {
			// Update existing node
			newValue := &ValueWrapper{data: value}
			if evictCallback != nil {
				current.onEvict = evictCallback
			}
			atomic.StorePointer(&current.value, unsafe.Pointer(newValue))
			atomic.AddUint64(&current.accessCnt, 1)

			// Mark progress
			atomic.StoreInt64(&list.lastProgressTime, time.Now().UnixNano())
			return true
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}

	// Trigger eviction if at or near capacity
	currentLength := atomic.LoadInt64(&list.length)
	if currentLength >= list.capacity {
		list.forceEviction()
		// Process eviction immediately to make room
		list.processEvictionQueue()
	}

	// Create or reuse node
	newNode := list.reuseOrCreateNode(key, value, evictCallback)

	// Add node to the list with retry logic and backoff
	const maxRetries = 100
	retryCount := 0
	backoffNs := int64(1000) // Start with 1μs

	for {
		if retryCount > maxRetries {
			// Fallback: try to recover by rebuilding tail pointer
			list.repairTailPointer()
			return false
		}

		tail := (*Node)(atomic.LoadPointer(&list.tail))

		if atomic.CompareAndSwapPointer(&tail.next, nil, unsafe.Pointer(newNode)) {
			atomic.StorePointer(&newNode.prev, unsafe.Pointer(tail))

			// Try to update tail with timeout
			tailUpdated := false
			for attempts := 0; attempts < 10; attempts++ {
				if atomic.CompareAndSwapPointer(&list.tail, unsafe.Pointer(tail), unsafe.Pointer(newNode)) {
					tailUpdated = true
					break
				}
				currentTail := (*Node)(atomic.LoadPointer(&list.tail))
				if currentTail == newNode {
					tailUpdated = true
					break
				}
				time.Sleep(time.Duration(backoffNs))
				backoffNs = min(backoffNs*2, 1000000) // Cap at 1ms
			}

			if tailUpdated {
				atomic.AddInt64(&list.length, 1)
				// Mark progress
				atomic.StoreInt64(&list.lastProgressTime, time.Now().UnixNano())
				return true
			}
		}

		// Exponential backoff
		time.Sleep(time.Duration(backoffNs))
		backoffNs = min(backoffNs*2, 1000000)
		retryCount++

		// Try to advance tail if it's stale
		nextTail := (*Node)(atomic.LoadPointer(&tail.next))
		if nextTail != nil {
			atomic.CompareAndSwapPointer(&list.tail, unsafe.Pointer(tail), unsafe.Pointer(nextTail))
		}
		runtime.Gosched()
	}
}

// forceEviction aggressively evicts nodes when at capacity with anti-cascading
func (list *LRU) forceEviction() {
	// Prevent recursive eviction
	if !atomic.CompareAndSwapInt32(&list.evicting, 0, 1) {
		return // Already evicting
	}
	defer atomic.StoreInt32(&list.evicting, 0)

	currentLength := atomic.LoadInt64(&list.length)
	if currentLength < list.capacity {
		return // No need to evict
	}

	toEvict := int(float64(list.capacity) * list.evictRatio)
	toEvict = int(min(int64(toEvict), currentLength/2)) // Never evict more than half

	if toEvict < 1 {
		toEvict = 1
	}

	// When at/over capacity, be more aggressive about eviction
	currentTime := time.Now().UnixNano()
	current := (*Node)(atomic.LoadPointer(&list.head))
	current = (*Node)(atomic.LoadPointer(&current.next))
	evicted := 0

	// Try to evict based on normal criteria
	for current != nil && evicted < toEvict {
		next := (*Node)(atomic.LoadPointer(&current.next))

		if list.shouldEvictNode(current, currentTime) {
			if atomic.CompareAndSwapInt32(&current.markedForEviction, 0, 1) {
				list.evictionQueue.Enqueue(current)
				evicted++
			}
		}

		current = next
	}

	// If we didn't evict enough, be more aggressive
	if evicted < toEvict && currentLength >= list.capacity {
		current = (*Node)(atomic.LoadPointer(&list.head))
		current = (*Node)(atomic.LoadPointer(&current.next))

		for current != nil && evicted < toEvict {
			next := (*Node)(atomic.LoadPointer(&current.next))

			// Skip already marked nodes
			if atomic.LoadInt32(&current.markedForEviction) == 1 {
				current = next
				continue
			}

			// Evict nodes with low access count
			accessCount := atomic.LoadUint64(&current.accessCnt)
			if accessCount <= 3 { // Evict nodes accessed 3 times or less
				if atomic.CompareAndSwapInt32(&current.markedForEviction, 0, 1) {
					list.evictionQueue.Enqueue(current)
					evicted++
				}
			}

			current = next
		}
	}

	// Process immediately but with limits
	list.processEvictionQueue()
}

// processEvictionQueue removes nodes that have been marked for eviction with limits
func (list *LRU) processEvictionQueue() {
	processed := 0
	maxProcess := 100

	for processed < maxProcess {
		node := list.evictionQueue.Dequeue()
		if node == nil {
			break
		}

		nodePtr := node.(*Node)

		// Double-check eviction flag
		if atomic.LoadInt32(&nodePtr.markedForEviction) != 1 {
			continue
		}

		// Call eviction callback
		if nodePtr.onEvict != nil {
			valuePtr := atomic.LoadPointer(&nodePtr.value)
			if valuePtr != nil {
				value := (*ValueWrapper)(valuePtr)
				nodePtr.onEvict(nodePtr.key, value.data)
			}
		}

		list.removeNodeFromList(nodePtr)
		processed++
	}
}

// removeNodeFromList physically removes a node from the linked list
func (list *LRU) removeNodeFromList(node *Node) {
	prev := (*Node)(atomic.LoadPointer(&node.prev))
	next := (*Node)(atomic.LoadPointer(&node.next))

	if prev != nil {
		atomic.CompareAndSwapPointer(&prev.next, unsafe.Pointer(node), unsafe.Pointer(next))
	}
	if next != nil {
		atomic.CompareAndSwapPointer(&next.prev, unsafe.Pointer(node), unsafe.Pointer(prev))
	}
	if next == nil {
		atomic.CompareAndSwapPointer(&list.tail, unsafe.Pointer(node), unsafe.Pointer(prev))
	}

	atomic.AddInt64(&list.length, -1)
}

// Delete removes a node by key
func (list *LRU) Delete(key interface{}) bool {
	current := (*Node)(atomic.LoadPointer(&list.head))
	current = (*Node)(atomic.LoadPointer(&current.next))

	for current != nil {
		if current.key == key {
			// Mark for eviction and immediately process
			if atomic.CompareAndSwapInt32(&current.markedForEviction, 0, 1) {
				list.evictionQueue.Enqueue(current)
				list.processEvictionQueue()

				// Mark progress
				atomic.StoreInt64(&list.lastProgressTime, time.Now().UnixNano())
				return true
			}
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
	return false
}

// Length returns the current length of the list
func (list *LRU) Length() int64 {
	return atomic.LoadInt64(&list.length)
}

// ForEach iterates through the list safely
func (list *LRU) ForEach(fn func(key, value interface{}, accessCount uint64) bool) {
	current := (*Node)(atomic.LoadPointer(&list.head))
	current = (*Node)(atomic.LoadPointer(&current.next))

	for current != nil {
		// Skip nodes marked for eviction
		if atomic.LoadInt32(&current.markedForEviction) == 1 {
			current = (*Node)(atomic.LoadPointer(&current.next))
			continue
		}

		accesses := atomic.LoadUint64(&current.accessCnt)
		valuePtr := atomic.LoadPointer(&current.value)
		valueWrapper := (*ValueWrapper)(valuePtr)

		if !fn(current.key, valueWrapper.data, accesses) {
			break
		}
		current = (*Node)(atomic.LoadPointer(&current.next))
	}
}

// Clear empties the list
func (list *LRU) Clear() {
	valueWrapper := &ValueWrapper{data: nil}
	sentinel := &Node{
		key:       nil,
		value:     unsafe.Pointer(valueWrapper),
		accessCnt: 0,
		timestamp: time.Now().UnixNano(),
	}

	atomic.StorePointer(&list.head, unsafe.Pointer(sentinel))
	atomic.StorePointer(&list.tail, unsafe.Pointer(sentinel))
	atomic.StoreInt64(&list.length, 0)

	// Clear eviction queue
	list.evictionQueue = queue.New()

	// Reset thrashing prevention fields
	atomic.StoreInt32(&list.evicting, 0)
	atomic.StoreInt32(&list.stuckCounter, 0)
	atomic.StoreInt64(&list.lastProgressTime, time.Now().UnixNano())
}

// ForceEvictionProcessing forces the processing of the eviction queue
// ***************This is mainly for testing purposes to ensure eviction happens immediately
func (list *LRU) ForceEvictionProcessing() {
	// Force eviction if we're over capacity
	currentLength := atomic.LoadInt64(&list.length)
	if currentLength > list.capacity {
		list.forceEviction()
	}

	// Process the eviction queue multiple times to ensure completion
	for i := 0; i < 3; i++ {
		list.processEvictionQueue()
		// Small delay to allow other goroutines to process
		runtime.Gosched()
	}
}

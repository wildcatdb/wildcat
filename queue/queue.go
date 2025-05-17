// Package queue
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
package queue

import (
	"sync/atomic"
	"unsafe"
)

// Node represents a node in the queue
type Node struct {
	value interface{}
	next  unsafe.Pointer // *Node
}

// Queue implements a concurrent non-blocking queue
type Queue struct {
	head unsafe.Pointer // *Node
	tail unsafe.Pointer // *Node
}

// New creates a new concurrent queue
func New() *Queue {
	node := &Node{}
	nodePtr := unsafe.Pointer(node)
	return &Queue{
		head: nodePtr,
		tail: nodePtr,
	}
}

// List returns a slice of all values in the queue
func (q *Queue) List() []interface{} {
	var result []interface{}
	headPtr := atomic.LoadPointer(&q.head)
	head := (*Node)(headPtr)
	nextPtr := atomic.LoadPointer(&head.next)
	for nextPtr != nil {
		next := (*Node)(nextPtr)
		result = append(result, next.value)
		nextPtr = atomic.LoadPointer(&next.next)
	}
	return result
}

// Enqueue adds a value to the queue
func (q *Queue) Enqueue(value interface{}) {
	node := &Node{value: value}
	nodePtr := unsafe.Pointer(node)

	for {
		tailPtr := atomic.LoadPointer(&q.tail)
		tail := (*Node)(tailPtr)
		nextPtr := atomic.LoadPointer(&tail.next)

		// Check if tail is consistent
		if tailPtr == atomic.LoadPointer(&q.tail) {
			if nextPtr == nil {
				// Try to link node at the end of the list
				if atomic.CompareAndSwapPointer(&tail.next, nil, nodePtr) {
					// Enqueue is done, try to swing tail to the inserted node
					atomic.CompareAndSwapPointer(&q.tail, tailPtr, nodePtr)
					return
				}
			} else {
				// Tail was not pointing to the last node, try to advance tail
				atomic.CompareAndSwapPointer(&q.tail, tailPtr, nextPtr)
			}
		}
	}
}

// Dequeue removes and returns a value from the queue
// Returns nil if the queue is empty
func (q *Queue) Dequeue() interface{} {
	for {
		headPtr := atomic.LoadPointer(&q.head)
		tailPtr := atomic.LoadPointer(&q.tail)
		head := (*Node)(headPtr)
		nextPtr := atomic.LoadPointer(&head.next)

		// Check if head, tail, and next are consistent
		if headPtr == atomic.LoadPointer(&q.head) {
			// Is queue empty or tail falling behind?
			if headPtr == tailPtr {
				// Is queue empty?
				if nextPtr == nil {
					return nil // Queue is empty
				}
				// Tail is falling behind. Try to advance it
				atomic.CompareAndSwapPointer(&q.tail, tailPtr, nextPtr)
			} else {
				// Queue is not empty, read value before CAS
				next := (*Node)(nextPtr)
				value := next.value

				// Try to swing Head to the next node
				if atomic.CompareAndSwapPointer(&q.head, headPtr, nextPtr) {
					return value // Dequeue is done
				}
			}
		}
	}
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	headPtr := atomic.LoadPointer(&q.head)
	head := (*Node)(headPtr)
	return atomic.LoadPointer(&head.next) == nil
}

// ForEach iterates over the queue and applies the function f to each item
func (q *Queue) ForEach(f func(item interface{}) bool) {
	headPtr := atomic.LoadPointer(&q.head)
	head := (*Node)(headPtr)
	nextPtr := atomic.LoadPointer(&head.next)
	for nextPtr != nil {
		next := (*Node)(nextPtr)
		if !f(next.value) {
			return
		}
		nextPtr = atomic.LoadPointer(&next.next)
	}
}

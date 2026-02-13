// Package queue
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
package queue

import (
	"sync/atomic"
)

// Node represents a node in the queue
type Node struct {
	value interface{}
	next  atomic.Pointer[Node]
}

// Queue implements a concurrent non-blocking queue
type Queue struct {
	head atomic.Pointer[Node]
	tail atomic.Pointer[Node]
	size int64 // Atomic counter
}

// New creates a new concurrent queue
func New() *Queue {
	node := &Node{}
	q := &Queue{}
	q.head.Store(node)
	q.tail.Store(node)
	return q
}

// List returns a slice of all values in the queue
func (q *Queue) List() []interface{} {
	var result []interface{}
	head := q.head.Load()
	if head == nil {
		return result
	}
	next := head.next.Load()
	for next != nil {
		result = append(result, next.value)
		next = next.next.Load()
	}
	return result
}

// Enqueue adds a value to the queue
func (q *Queue) Enqueue(value interface{}) {
	node := &Node{value: value}

	for {
		tail := q.tail.Load()
		if tail == nil {
			continue
		}
		next := tail.next.Load()

		// Check if tail is consistent
		if tail == q.tail.Load() {
			if next == nil {
				// Try to link node at the end of the list
				if tail.next.CompareAndSwap(nil, node) {
					// Enqueue is done, try to swing tail to the inserted node
					q.tail.CompareAndSwap(tail, node)
					atomic.AddInt64(&q.size, 1)
					return
				}
			} else {
				// Tail was not pointing to the last node, try to advance tail
				q.tail.CompareAndSwap(tail, next)
			}
		}
	}
}

// Dequeue removes and returns a value from the queue
// Returns nil if the queue is empty
func (q *Queue) Dequeue() interface{} {
	for {
		head := q.head.Load()
		tail := q.tail.Load()
		if head == nil {
			continue
		}
		next := head.next.Load()

		// Check if head, tail, and next are consistent
		if head == q.head.Load() {
			// Is queue empty or tail falling behind?
			if head == tail {
				// Is queue empty?
				if next == nil {
					return nil // Queue is empty
				}
				// Tail is falling behind. Try to advance it
				q.tail.CompareAndSwap(tail, next)
			} else {
				// Queue is not empty, read value before CAS
				if next == nil {
					continue
				}
				value := next.value

				// Try to swing Head to the next node
				if q.head.CompareAndSwap(head, next) {
					atomic.AddInt64(&q.size, -1) // Decrement counter
					return value                 // Dequeue is done
				}
			}
		}
	}
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	head := q.head.Load()
	if head == nil {
		return true
	}
	return head.next.Load() == nil
}

// Peek returns the value at the front of the queue without removing it
// Returns nil if the queue is empty
func (q *Queue) Peek() interface{} {
	head := q.head.Load()
	if head == nil {
		return nil
	}
	next := head.next.Load()
	if next == nil {
		return nil // Queue is empty
	}
	return next.value
}

// ForEach iterates over the queue and applies the function f to each item
func (q *Queue) ForEach(f func(item interface{}) bool) {
	head := q.head.Load()
	if head == nil {
		return
	}
	next := head.next.Load()
	for next != nil {
		if !f(next.value) {
			return
		}
		next = next.next.Load()
	}
}

// Size returns the number of items in the queue
func (q *Queue) Size() int64 {
	return atomic.LoadInt64(&q.size)
}

// Package stack
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
package stack

import (
	"sync/atomic"
	"unsafe"
)

// Node represents an element in our stack
type Node struct {
	Value interface{}
	Next  *Node
}

// Stack implements a lock-free stack
type Stack struct {
	head unsafe.Pointer // *Node
}

// New creates a new stack
func New() *Stack {
	return &Stack{
		head: nil,
	}
}

// Push adds a value to the stack
func (s *Stack) Push(value interface{}) {
	newNode := &Node{Value: value}

	for {
		// Get the current head
		oldHead := atomic.LoadPointer(&s.head)

		// Set new node's next pointer to current head
		newNode.Next = (*Node)(oldHead)

		// Try to swap the head pointer
		if atomic.CompareAndSwapPointer(
			&s.head,
			oldHead,
			unsafe.Pointer(newNode),
		) {
			return // Success, we're done
		}

		// If we're here, the CAS failed (another thread changed the head)
		// Loop and try again
	}
}

// IsEmpty checks if the stack is empty
func (s *Stack) IsEmpty() bool {
	// Check if the head pointer is nil
	return atomic.LoadPointer(&s.head) == nil
}

// Size returns the number of elements in the stack
func (s *Stack) Size() int {
	count := 0
	current := (*Node)(atomic.LoadPointer(&s.head))

	// Traverse the stack and count the nodes
	for current != nil {
		count++
		current = current.Next
	}

	return count
}

// Pop removes and returns the top value from the stack
func (s *Stack) Pop() interface{} {
	for {
		// Get the current head
		oldHead := atomic.LoadPointer(&s.head)
		if oldHead == nil {
			return nil // Stack is empty
		}

		// Get the node the head points to
		oldHeadNode := (*Node)(oldHead)

		// Try to update the head to the next node
		if atomic.CompareAndSwapPointer(
			&s.head,
			oldHead,
			unsafe.Pointer(oldHeadNode.Next),
		) {
			return oldHeadNode.Value // Success, return the value
		}

		// If we're here, the CAS failed, loop and try again
	}
}

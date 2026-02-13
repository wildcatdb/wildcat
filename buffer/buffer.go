// Package buffer
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
package buffer

import (
	"errors"
	"github.com/wildcatdb/wildcat/v2/queue"
	"sync/atomic"
)

// entry is a buffer entry
type entry struct {
	value interface{}
}

// Buffer is a concurrent lock-free buffer with ID/Slot-based access
type Buffer struct {
	buffer         []atomic.Pointer[entry] // Slice of pointers to entries
	capacity       int64                    // Maximum capacity of the buffer
	availableSlots *queue.Queue             // Queue of available slots for new entries
}

// New creates a new atomic buffer with the specified capacity
func New(capacity int) (*Buffer, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be greater than 0")
	}

	buff := &Buffer{
		buffer:         make([]atomic.Pointer[entry], capacity),
		capacity:       int64(capacity),
		availableSlots: queue.New(),
	}

	// Enqueue all available slots
	for i := 0; i < capacity; i++ {
		buff.availableSlots.Enqueue(int64(i))
	}

	return buff, nil
}

// getAvailableSlot retrieves an available slot from the buffer
func (buff *Buffer) getAvailableSlot() (int64, error) {
	slot := buff.availableSlots.Dequeue()
	if slot == nil {
		return 0, errors.New("no available slots")
	}

	return slot.(int64), nil
}

// releaseSlot returns a slot to the available slots queue
func (buff *Buffer) releaseSlot(slot int64) {
	buff.availableSlots.Enqueue(slot)
}

// Add attempts to add an item to the buffer
func (buff *Buffer) Add(item interface{}) (int64, error) {
	slot, err := buff.getAvailableSlot()
	if err != nil {
		return 0, err
	}

	e := &entry{
		value: item,
	}

	buff.buffer[slot].Store(e)

	return slot, nil
}

// Get retrieves an item by its slot ID
func (buff *Buffer) Get(slot int64) (interface{}, error) {
	if slot < 0 || slot >= buff.capacity {
		return nil, errors.New("invalid slot ID")
	}

	e := buff.buffer[slot].Load()
	if e == nil {
		return nil, errors.New("item not found")
	}

	return e.value, nil
}

// Remove removes an item by its slot ID
func (buff *Buffer) Remove(slot int64) error {
	if slot < 0 || slot >= buff.capacity {
		return errors.New("invalid slot ID")
	}

	e := buff.buffer[slot].Load()
	if e == nil {
		return errors.New("item not found")
	}

	// Atomically clear the slot using CompareAndSwap to prevent race conditions
	if !buff.buffer[slot].CompareAndSwap(e, nil) {
		// Another thread removed the item between our load and CAS
		return errors.New("item was already removed")
	}

	// Release the slot back to available slots
	buff.releaseSlot(slot)

	return nil
}

// Update atomically updates an item at the given slot
func (buff *Buffer) Update(slot int64, newValue interface{}) error {
	if slot < 0 || slot >= buff.capacity {
		return errors.New("invalid slot ID")
	}

	newEntry := &entry{value: newValue}

	// Keep trying until we successfully update or determine slot is empty
	for {
		oldEntry := buff.buffer[slot].Load()
		if oldEntry == nil {
			return errors.New("item not found")
		}

		if buff.buffer[slot].CompareAndSwap(oldEntry, newEntry) {
			return nil
		}
		// If CAS failed, retry - another thread might have updated the slot
	}
}

// Count returns the number of items currently in the buffer
func (buff *Buffer) Count() int64 {
	return buff.capacity - buff.availableSlots.Size()
}

// IsFull returns true if the buffer is at capacity
func (buff *Buffer) IsFull() bool {
	return buff.availableSlots.IsEmpty()
}

// IsEmpty returns true if the buffer contains no items
func (buff *Buffer) IsEmpty() bool {
	return buff.availableSlots.Size() == buff.capacity
}

// Capacity returns the maximum capacity of the buffer
func (buff *Buffer) Capacity() int64 {
	return buff.capacity
}

// List returns a snapshot of all non-nil values in the buffer
// *This is not atomic across all slots, so it provides a point-in-time view
func (buff *Buffer) List() []interface{} {
	var result []interface{}

	for i := int64(0); i < buff.capacity; i++ {
		e := buff.buffer[i].Load()
		if e != nil {
			result = append(result, e.value)
		}
	}

	return result
}

// ForEach applies function f to each item in the buffer
// Returns early if f returns false
// *This is not atomic across all slots
func (buff *Buffer) ForEach(f func(slot int64, item interface{}) bool) {
	for i := int64(0); i < buff.capacity; i++ {
		e := buff.buffer[i].Load()
		if e != nil {
			if !f(i, e.value) {
				return
			}
		}
	}
}

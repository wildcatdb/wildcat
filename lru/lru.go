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
	"container/list"
	"sync"
)

// EvictionCallback is called when an item is evicted from the cache
type EvictionCallback func(key string, value interface{})

// LRU is the main LRU structure
type LRU struct {
	capacity int
	items    map[string]*list.Element
	lruList  *list.List
	mutex    sync.RWMutex
}

// entry is an item stored in the cache
type entry struct {
	key     string
	value   interface{}
	onEvict EvictionCallback
}

// New creates a new LRU cache with the specified capacity
func New(capacity int) *LRU {
	if capacity <= 0 {
		return nil // Invalid capacity, return nil
	}

	return &LRU{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		lruList:  list.New(),
	}
}

// Put adds or updates a key-value pair in the cache with an optional eviction callback
// If the key already exists, it updates the value and eviction callback, then moves it to the front
// If adding a new key would exceed capacity, it evicts the least recently used item
func (c *LRU) Put(key string, value interface{}, onEvict EvictionCallback) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, exists := c.items[key]; exists {
		c.lruList.MoveToFront(elem)
		e := elem.Value.(*entry)
		e.value = value
		e.onEvict = onEvict
		return
	}

	newEntry := &entry{
		key:     key,
		value:   value,
		onEvict: onEvict,
	}
	elem := c.lruList.PushFront(newEntry)
	c.items[key] = elem

	if c.lruList.Len() > c.capacity {
		c.evictLRU()
	}
}

// Get retrieves a value from the cache
// Returns the value and true if found, nil and false otherwise
// Accessing an item moves it to the front (most recently used)
func (c *LRU) Get(key string) (interface{}, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, exists := c.items[key]; exists {
		c.lruList.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}

	return nil, false
}

// Remove removes a key from the cache
// Returns true if the key was found and removed, false otherwise
func (c *LRU) Remove(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, exists := c.items[key]; exists {
		c.removeElement(elem)
		return true
	}

	return false
}

// ForEach iterates over all items in the cache from most recently used to least recently used
// The callback function receives the key and value of each item
// If the callback returns false, iteration stops
func (c *LRU) ForEach(fn func(key string, value interface{}) bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		e := elem.Value.(*entry)
		if !fn(e.key, e.value) {
			break
		}
	}
}

// Len returns the current number of items in the cache
func (c *LRU) Len() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.lruList.Len()
}

// Cap returns the capacity of the cache
func (c *LRU) Cap() int {
	return c.capacity
}

// Clear removes all items from the cache
func (c *LRU) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		e := elem.Value.(*entry)
		if e.onEvict != nil {
			e.onEvict(e.key, e.value)
		}
	}

	c.items = make(map[string]*list.Element)
	c.lruList.Init()
}

// Keys returns a slice of all keys in the cache from most recently used to least recently used
func (c *LRU) Keys() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	keys := make([]string, 0, c.lruList.Len())
	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		e := elem.Value.(*entry)
		keys = append(keys, e.key)
	}

	return keys
}

// Contains checks if a key exists in the cache without affecting its position
func (c *LRU) Contains(key string) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	_, exists := c.items[key]
	return exists
}

// Peek returns the value associated with key without updating the "recently used"-ness
// Returns the value and true if found, nil and false otherwise
func (c *LRU) Peek(key string) (interface{}, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if elem, exists := c.items[key]; exists {
		return elem.Value.(*entry).value, true
	}

	return nil, false
}

// GetOldest returns the oldest (least recently used) key-value pair without removing it
// Returns empty string, nil, false if cache is empty
func (c *LRU) GetOldest() (string, interface{}, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if elem := c.lruList.Back(); elem != nil {
		e := elem.Value.(*entry)
		return e.key, e.value, true
	}

	return "", nil, false
}

// RemoveOldest removes and returns the oldest (least recently used) key-value pair
// Returns empty string, nil, false if cache is empty
func (c *LRU) RemoveOldest() (string, interface{}, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem := c.lruList.Back(); elem != nil {
		e := elem.Value.(*entry)
		c.removeElement(elem)
		return e.key, e.value, true
	}

	return "", nil, false
}

// evictLRU removes the least recently used item
func (c *LRU) evictLRU() {
	if elem := c.lruList.Back(); elem != nil {
		c.removeElement(elem)
	}
}

// removeElement removes an element from both the list and map
func (c *LRU) removeElement(elem *list.Element) {
	c.lruList.Remove(elem)
	e := elem.Value.(*entry)
	delete(c.items, e.key)

	if e.onEvict != nil {
		e.onEvict(e.key, e.value)
	}
}

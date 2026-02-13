// Package tree
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
package tree

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

const ReservedMetadataBlockID = 2

// BTree is the main struct for the b-tree
type BTree struct {
	blockManager BlockManager // Block manager for storing nodes
	metadata     *Metadata    // Metadata of the B-tree
}

// Metadata represents the metadata of the B-tree
type Metadata struct {
	RootBlockID int64       // Block ID of the root node
	Order       int         // Minimum degree t
	Extra       interface{} // Extra metadata, can be any type
}

// BlockManager interface defines methods for managing blocks of data
type BlockManager interface {
	Append(data []byte) (int64, error)
	Read(blockID int64) ([]byte, int64, error)
	Update(blockID int64, newData []byte) (int64, error)
	Close() error
}

// Node represents a B-tree node
type Node struct {
	BlockID  int64         // BlockID in which node lives
	IsLeaf   bool          // Is this a leaf node?
	Keys     [][]byte      // Keys in the node, sorted
	Values   []interface{} // Used in both leaf and internal nodes for consistency
	Children []int64       // Block IDs of child nodes
	Parent   int64         // Block ID of parent node (for efficient traversal)
	NextLeaf int64         // Block ID of next leaf (for range iteration)
	PrevLeaf int64         // Block ID of previous leaf (for bidirectional iteration)
}

// Iterator provides bidirectional iteration capabilities
type Iterator struct {
	btree       *BTree          // Reference to the B-tree instance
	currentNode *Node           // Current node being iterated
	currentIdx  int             // Current index within the current node
	stack       []iteratorFrame // Stack for tree traversal
	ascending   bool            // Direction of iteration (true for ascending, false for descending)
	startKey    []byte          // Start key for range iteration
	endKey      []byte          // End key for range iteration
	prefix      []byte          // Prefix for prefix iteration
	finished    bool            // Indicates if the iterator has finished iterating
}

// iteratorFrame is used to keep track of the current node and index during iteration
type iteratorFrame struct {
	node *Node
	idx  int
}

// Open retrieves/opens a btree instance.  Block manager should be opened with read/write!!
func Open(blockManager BlockManager, order int, extraMeta interface{}) (*BTree, error) {
	if order < 2 {
		return nil, errors.New("order must be at least 2")
	}

	bt := &BTree{
		blockManager: blockManager,
	}

	// Try to load existing metadata first
	err := bt.loadMetadata()
	if err != nil {

		// File doesn't exist or is empty - create new tree
		return bt.createNewTree(extraMeta, order)
	} else {
		// Validate existing tree
		if bt.metadata.Order != order {
			return nil, fmt.Errorf("existing tree has order %d, requested %d", bt.metadata.Order, order)
		}
	}

	// Verify root exists and is valid
	_, err = bt.loadNode(bt.metadata.RootBlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing root: %v", err)
	}

	return bt, nil
}

// createNewTree initializes a new B-tree with an empty root node
func (bt *BTree) createNewTree(extra interface{}, order int) (*BTree, error) {
	// Create initial root node
	root := &Node{
		BlockID:  -1,
		IsLeaf:   true,
		Keys:     make([][]byte, 0),
		Values:   make([]interface{}, 0),
		Children: make([]int64, 0),
		Parent:   -1,
		NextLeaf: -1,
		PrevLeaf: -1,
	}

	blockID, err := bt.storeNode(root)
	if err != nil {
		return nil, err
	}

	bt.metadata = &Metadata{
		RootBlockID: blockID,
		Order:       order,
	}

	if extra != nil {
		bt.metadata.Extra = extra // Attach extra metadata if provided
	}

	// Save metadata
	err = bt.saveMetadata()
	if err != nil {
		return nil, err
	}

	return bt, nil
}

// loadNode loads a node from the block manager
func (bt *BTree) loadMetadata() error {
	data, _, err := bt.blockManager.Read(ReservedMetadataBlockID)
	if err != nil {
		return err // File doesn't exist or block ReservedMetadataBlockID is empty
	}

	var metadata Metadata
	err = bson.Unmarshal(data, &metadata)
	if err != nil {
		return err // Failed to unmarshal metadata
	}

	bt.metadata = &metadata

	return nil
}

// saveMetadata saves the metadata of the B-tree
func (bt *BTree) saveMetadata() error {

	meta, err := bson.Marshal(&bt.metadata)
	if err != nil {
		return err
	}

	// Save to block 0, or create it if it doesn't exist
	_, err = bt.blockManager.Update(ReservedMetadataBlockID, meta)
	if err != nil {
		// If update fails, try append (for first time)
		_, err = bt.blockManager.Append(meta)
	}

	return err
}

// Put adds a key-value pair to the B-tree, updates if the key already exists
func (bt *BTree) Put(key []byte, value interface{}) error {

	if bt.metadata.RootBlockID == -1 {
		return errors.New("tree not initialized")
	}

	root, err := bt.loadNode(bt.metadata.RootBlockID)
	if err != nil {
		return err
	}

	oldRootID := bt.metadata.RootBlockID

	// If root is full, create new root
	if bt.isNodeFull(root) {
		newRoot := &Node{
			BlockID:  -1, // Initialize as new node
			IsLeaf:   false,
			Keys:     make([][]byte, 0),
			Values:   make([]interface{}, 0),
			Children: []int64{bt.metadata.RootBlockID},
			Parent:   -1,
		}

		newRootID, err := bt.storeNode(newRoot)
		if err != nil {
			return err
		}

		// Update the root reference
		bt.metadata.RootBlockID = newRootID

		// Update old root's parent
		root.Parent = newRootID
		if _, err := bt.storeNode(root); err != nil {
			return err
		}

		// Split the old root (now child 0 of new root)
		if err := bt.splitChild(newRoot, 0); err != nil {
			return err
		}
	}

	// If root changed, save metadata
	if bt.metadata.RootBlockID != oldRootID {
		err := bt.saveMetadata()
		if err != nil {
			return fmt.Errorf("failed to save metadata after root change: %v", err)
		}
	}

	return bt.insertNonFull(bt.metadata.RootBlockID, key, value)
}

// Get finds a value by key
func (bt *BTree) Get(key []byte) (interface{}, bool, error) {

	return bt.get(bt.metadata.RootBlockID, key)
}

// RangeIterator returns an iterator for keys in the range [startKey, endKey]
func (bt *BTree) RangeIterator(startKey, endKey []byte, ascending bool) (*Iterator, error) {

	iter := &Iterator{
		btree:     bt,
		ascending: ascending,
		stack:     make([]iteratorFrame, 0),
		startKey:  make([]byte, len(startKey)),
		endKey:    make([]byte, len(endKey)),
		finished:  false,
	}
	copy(iter.startKey, startKey)
	copy(iter.endKey, endKey)

	var searchKey []byte
	if ascending {
		searchKey = startKey
	} else {
		searchKey = endKey
	}

	// Find starting position
	node, idx, err := bt.findLeafPosition(searchKey)
	if err != nil {
		return nil, err
	}

	iter.currentNode = node
	iter.currentIdx = idx

	// Adjust position if needed
	if ascending {

		// For ascending, ensure we start at or after startKey
		for iter.currentIdx < len(iter.currentNode.Keys) {
			if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], startKey) >= 0 {
				break
			}
			iter.currentIdx++
		}

		// If we've gone past the end of this node, try the next one
		if iter.currentIdx >= len(iter.currentNode.Keys) {
			if iter.currentNode.NextLeaf != -1 {
				nextNode, err := iter.btree.loadNode(iter.currentNode.NextLeaf)
				if err == nil {
					iter.currentNode = nextNode
					iter.currentIdx = 0

					// Re-check bounds in the new node
					for iter.currentIdx < len(iter.currentNode.Keys) {
						if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], startKey) >= 0 {
							break
						}
						iter.currentIdx++
					}
				} else {
					iter.finished = true
				}
			} else {
				iter.finished = true
			}
		}

		// Check if we're already past the end key
		if iter.currentIdx < len(iter.currentNode.Keys) && bytes.Compare(iter.currentNode.Keys[iter.currentIdx], endKey) > 0 {
			iter.finished = true
		}
	} else {

		// For descending, ensure we start at or before endKey
		if iter.currentIdx >= len(iter.currentNode.Keys) {
			iter.currentIdx = len(iter.currentNode.Keys) - 1
		}
		for iter.currentIdx >= 0 {
			if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], endKey) <= 0 {
				break
			}
			iter.currentIdx--
		}
		// If we've gone past the beginning of this node, try the previous one
		if iter.currentIdx < 0 {
			if iter.currentNode.PrevLeaf != -1 {
				prevNode, err := iter.btree.loadNode(iter.currentNode.PrevLeaf)
				if err == nil {
					iter.currentNode = prevNode
					iter.currentIdx = len(prevNode.Keys) - 1
					// Re-check bounds in the new node
					for iter.currentIdx >= 0 {
						if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], endKey) <= 0 {
							break
						}
						iter.currentIdx--
					}
				} else {
					iter.finished = true
				}
			} else {
				iter.finished = true
			}
		}

		// Check if we're already past the start key
		if iter.currentIdx >= 0 && bytes.Compare(iter.currentNode.Keys[iter.currentIdx], startKey) < 0 {
			iter.finished = true
		}
	}

	return iter, nil
}

// Iterator returns a general-purpose iterator that can seek to any position
func (bt *BTree) Iterator(ascending bool) (*Iterator, error) {

	iter := &Iterator{
		btree:     bt,
		ascending: ascending,
		stack:     make([]iteratorFrame, 0),
		finished:  false,
	}

	// Start at the appropriate end of the tree
	if ascending {
		// Find the leftmost (smallest) key
		node, err := bt.findFirstLeaf()
		if err != nil {
			return nil, err
		}
		iter.currentNode = node
		iter.currentIdx = 0
	} else {
		// Find the rightmost (largest) key
		node, err := bt.findLastLeaf()
		if err != nil {
			return nil, err
		}
		iter.currentNode = node
		if len(node.Keys) > 0 {
			iter.currentIdx = len(node.Keys) - 1
		} else {
			iter.finished = true
		}
	}

	return iter, nil
}

// Seek positions the iterator at the first key >= seekKey (for ascending) or <= seekKey (for descending)
func (iter *Iterator) Seek(seekKey []byte) error {
	if iter.btree == nil {
		return errors.New("iterator not initialized")
	}

	// Find the leaf position for the seek key
	node, idx, err := iter.btree.findLeafPosition(seekKey)
	if err != nil {
		return err
	}

	iter.currentNode = node
	iter.currentIdx = idx
	iter.finished = false

	if iter.ascending {
		// For ascending, ensure we're at or after seekKey
		for iter.currentIdx < len(iter.currentNode.Keys) {
			if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], seekKey) >= 0 {
				break
			}
			iter.currentIdx++
		}
		// If we've gone past the end of this node, try the next one
		if iter.currentIdx >= len(iter.currentNode.Keys) {
			if iter.currentNode.NextLeaf != -1 {
				nextNode, err := iter.btree.loadNode(iter.currentNode.NextLeaf)
				if err == nil {
					iter.currentNode = nextNode
					iter.currentIdx = 0
				} else {
					iter.finished = true
				}
			} else {
				iter.finished = true
			}
		}
	} else {
		// For descending, ensure we're at or before seekKey
		if iter.currentIdx >= len(iter.currentNode.Keys) {
			iter.currentIdx = len(iter.currentNode.Keys) - 1
		}
		for iter.currentIdx >= 0 {
			if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], seekKey) <= 0 {
				break
			}
			iter.currentIdx--
		}

		// If we've gone past the beginning of this node, try the previous one
		if iter.currentIdx < 0 {
			if iter.currentNode.PrevLeaf != -1 {
				prevNode, err := iter.btree.loadNode(iter.currentNode.PrevLeaf)
				if err == nil {
					iter.currentNode = prevNode
					iter.currentIdx = len(prevNode.Keys) - 1
					// Re-check bounds in the new node
					for iter.currentIdx >= 0 {
						if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], seekKey) <= 0 {
							break
						}
						iter.currentIdx--
					}
				} else {
					iter.finished = true
				}
			} else {
				iter.finished = true
			}
		}
	}

	return nil
}

// SeekToFirst positions the iterator at the first key in the tree
func (iter *Iterator) SeekToFirst() error {
	if iter.btree == nil {
		return errors.New("iterator not initialized")
	}

	node, err := iter.btree.findFirstLeaf()
	if err != nil {
		return err
	}

	iter.currentNode = node
	iter.currentIdx = 0
	iter.finished = len(node.Keys) == 0
	iter.ascending = true

	return nil
}

// SeekToLast positions the iterator at the last key in the tree
func (iter *Iterator) SeekToLast() error {
	if iter.btree == nil {
		return errors.New("iterator not initialized")
	}

	node, err := iter.btree.findLastLeaf()
	if err != nil {
		return err
	}

	iter.currentNode = node
	if len(node.Keys) > 0 {
		iter.currentIdx = len(node.Keys) - 1
		iter.finished = false
	} else {
		iter.currentIdx = 0
		iter.finished = true
	}
	iter.ascending = false

	return nil
}

// SetDirection changes the iteration direction
func (iter *Iterator) SetDirection(ascending bool) {
	iter.ascending = ascending
}

// Peek returns the current key and value without advancing the iterator
func (iter *Iterator) Peek() ([]byte, interface{}, bool) {
	if !iter.Valid() {
		return nil, nil, false
	}

	key := iter.Key()
	value := iter.Value()

	return key, value, true
}

// Valid returns true if the iterator is positioned at a valid key
func (iter *Iterator) Valid() bool {
	if iter.finished || iter.currentNode == nil {
		return false
	}

	// Check if we're within the bounds of the current node
	if iter.ascending {
		if iter.currentIdx >= len(iter.currentNode.Keys) {
			return false
		}
	} else {
		if iter.currentIdx < 0 || iter.currentIdx >= len(iter.currentNode.Keys) {
			return false
		}
	}

	// For range and prefix iterators, check bounds
	if iter.currentIdx >= 0 && iter.currentIdx < len(iter.currentNode.Keys) {
		key := iter.currentNode.Keys[iter.currentIdx]

		// Check range bounds
		if iter.endKey != nil && bytes.Compare(key, iter.endKey) > 0 {
			return false
		}
		if iter.startKey != nil && bytes.Compare(key, iter.startKey) < 0 {
			return false
		}

		// Check prefix
		if iter.prefix != nil && !bytes.HasPrefix(key, iter.prefix) {
			return false
		}
	}

	return true
}

// Key returns the current key (without advancing the iterator)
func (iter *Iterator) Key() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.currentNode.Keys[iter.currentIdx]
}

// Value returns the current value (without advancing the iterator)
func (iter *Iterator) Value() interface{} {
	if !iter.Valid() {
		return nil
	}
	return iter.currentNode.Values[iter.currentIdx]
}

// NextItem returns the current key-value pair and advances the iterator (for backward compatibility)
func (iter *Iterator) NextItem() ([]byte, interface{}, bool) {
	if !iter.Valid() {
		return nil, nil, false
	}

	key := iter.Key()
	value := iter.Value()

	// Advance iterator
	iter.Next()

	// Return the key-value we just read
	return key, value, true
}

// Prev moves to the previous item (regardless of ascending flag)
func (iter *Iterator) Prev() bool {
	if iter.currentNode == nil {
		return false
	}

	// Move to previous item
	iter.currentIdx--

	// If we've gone past the beginning of current node, try previous leaf
	if iter.currentIdx < 0 {
		if iter.currentNode.PrevLeaf != -1 {
			prevNode, err := iter.btree.loadNode(iter.currentNode.PrevLeaf)
			if err != nil {
				iter.finished = true
				return false
			}
			iter.currentNode = prevNode
			iter.currentIdx = len(prevNode.Keys) - 1
		} else {
			iter.finished = true
			return false
		}
	}

	return iter.Valid()
}

// PrefixIterator returns an iterator for all keys with the given prefix
func (bt *BTree) PrefixIterator(prefix []byte, ascending bool) (*Iterator, error) {

	iter := &Iterator{
		btree:     bt,
		ascending: ascending,
		stack:     make([]iteratorFrame, 0),
		prefix:    make([]byte, len(prefix)),
		finished:  false,
	}
	copy(iter.prefix, prefix)

	// Find first key with prefix
	node, idx, err := bt.findLeafPosition(prefix)
	if err != nil {
		return nil, err
	}

	iter.currentNode = node
	iter.currentIdx = idx

	if ascending {

		// Find first key that has the prefix
		for {
			if iter.currentIdx >= len(iter.currentNode.Keys) {

				// Move to next leaf
				if iter.currentNode.NextLeaf != -1 {
					nextNode, err := iter.btree.loadNode(iter.currentNode.NextLeaf)
					if err != nil {
						iter.finished = true
						break
					}
					iter.currentNode = nextNode
					iter.currentIdx = 0
				} else {
					iter.finished = true
					break
				}
			} else if bytes.HasPrefix(iter.currentNode.Keys[iter.currentIdx], prefix) {
				break
			} else if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], prefix) > 0 {

				// We've gone past any possible prefix matches
				iter.finished = true
				break
			} else {
				iter.currentIdx++
			}
		}
	} else {
		// For descending, find the last key with prefix
		// First, find the upper bound for the prefix
		upperBound := make([]byte, len(prefix))
		copy(upperBound, prefix)
		if len(upperBound) > 0 {

			// Increment the last byte to get upper bound
			for i := len(upperBound) - 1; i >= 0; i-- {
				if upperBound[i] < 255 {
					upperBound[i]++
					break
				}
				upperBound[i] = 0
				if i == 0 {

					// Overflow case
					// We set to a very large value
					upperBound = append(upperBound, 0)
				}
			}
		}

		// Find position for upper bound
		node, idx, err = bt.findLeafPosition(upperBound)
		if err != nil {
			return nil, err
		}
		iter.currentNode = node
		iter.currentIdx = idx - 1

		// Find last key with prefix by going backward
		for {
			if iter.currentIdx < 0 {
				// Move to previous leaf
				if iter.currentNode.PrevLeaf != -1 {
					prevNode, err := iter.btree.loadNode(iter.currentNode.PrevLeaf)
					if err != nil {
						iter.finished = true
						break
					}
					iter.currentNode = prevNode
					iter.currentIdx = len(prevNode.Keys) - 1
				} else {
					iter.finished = true
					break
				}
			} else if bytes.HasPrefix(iter.currentNode.Keys[iter.currentIdx], prefix) {
				break
			} else if bytes.Compare(iter.currentNode.Keys[iter.currentIdx], prefix) < 0 {

				// We've gone past any possible prefix matches
				iter.finished = true
				break
			} else {
				iter.currentIdx--
			}
		}
	}

	return iter, nil
}

// Next moves to the next item (direction depends on ascending flag)
// Returns true if successful, false if no more items
func (iter *Iterator) Next() bool {
	if iter.finished || iter.currentNode == nil {
		return false
	}

	if iter.ascending {
		return iter.nextAscending()
	}
	return iter.nextDescending()
}

// HasNext checks if there are more items
func (iter *Iterator) HasNext() bool {
	return iter.Valid()
}

// BTree returns the B-tree instance associated with this iterator
func (iter *Iterator) BTree() *BTree {
	return iter.btree
}

// nextAcsending moves to the next item (for ascending iteration)
func (iter *Iterator) nextAscending() bool {
	if iter.currentNode == nil {
		iter.finished = true
		return false
	}

	// Move to next position
	iter.currentIdx++

	// Check if we have items in current node
	if iter.currentIdx < len(iter.currentNode.Keys) {
		// Check bounds and prefix in Valid() method
		return iter.Valid()
	}

	// Move to next leaf
	if iter.currentNode.NextLeaf != -1 {
		nextNode, err := iter.btree.loadNode(iter.currentNode.NextLeaf)
		if err != nil {
			iter.finished = true
			return false
		}
		iter.currentNode = nextNode
		iter.currentIdx = 0
		return iter.Valid()
	}

	iter.finished = true
	return false
}

// nextDescending moves to the previous item (for descending iteration)
func (iter *Iterator) nextDescending() bool {
	if iter.currentNode == nil {
		iter.finished = true
		return false
	}

	// Move to previous position
	iter.currentIdx--

	// Check if we have items in current node
	if iter.currentIdx >= 0 && iter.currentIdx < len(iter.currentNode.Keys) {
		// Check bounds and prefix in Valid() method
		return iter.Valid()
	}

	// Move to previous leaf
	if iter.currentNode.PrevLeaf != -1 {
		prevNode, err := iter.btree.loadNode(iter.currentNode.PrevLeaf)
		if err != nil {
			iter.finished = true
			return false
		}
		iter.currentNode = prevNode
		iter.currentIdx = len(prevNode.Keys) - 1
		return iter.Valid()
	}

	iter.finished = true
	return false
}

// get searches for a key in the B-tree
func (bt *BTree) get(blockID int64, key []byte) (interface{}, bool, error) {
	if blockID == -1 {
		return nil, false, nil
	}

	node, err := bt.loadNode(blockID)
	if err != nil {
		return nil, false, err
	}

	idx := bt.findKeyIndex(node.Keys, key)

	// Check if key exists at this position
	if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], key) {
		// Key found - return the value directly
		return node.Values[idx], true, nil
	}

	// Key not found at this level
	if node.IsLeaf {
		return nil, false, nil
	}

	// Search in appropriate child
	return bt.get(node.Children[idx], key)
}

// findLeafPosition finds the leaf node and index for a given key
func (bt *BTree) findLeafPosition(key []byte) (*Node, int, error) {
	node, err := bt.loadNode(bt.metadata.RootBlockID)
	if err != nil {
		return nil, 0, err
	}

	for !node.IsLeaf {
		idx := bt.findKeyIndex(node.Keys, key)
		node, err = bt.loadNode(node.Children[idx])
		if err != nil {
			return nil, 0, err
		}
	}

	idx := bt.findKeyIndex(node.Keys, key)
	return node, idx, nil
}

// findFirstLeaf finds the leftmost (first) leaf node in the tree
func (bt *BTree) findFirstLeaf() (*Node, error) {
	node, err := bt.loadNode(bt.metadata.RootBlockID)
	if err != nil {
		return nil, err
	}

	// Keep going left until we reach a leaf
	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return nil, errors.New("internal node has no children")
		}
		node, err = bt.loadNode(node.Children[0])
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// findLastLeaf finds the rightmost (last) leaf node in the tree
func (bt *BTree) findLastLeaf() (*Node, error) {
	node, err := bt.loadNode(bt.metadata.RootBlockID)
	if err != nil {
		return nil, err
	}

	// Keep going right until we reach a leaf
	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return nil, errors.New("internal node has no children")
		}
		node, err = bt.loadNode(node.Children[len(node.Children)-1])
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// insertNonFull inserts a key-value pair into a non-full node
func (bt *BTree) insertNonFull(blockID int64, key []byte, value interface{}) error {
	node, err := bt.loadNode(blockID)
	if err != nil {
		return err
	}

	if node.IsLeaf {
		// Insert into leaf
		idx := bt.findKeyIndex(node.Keys, key)

		// Check if key already exists
		if idx < len(node.Keys) && bytes.Equal(node.Keys[idx], key) {

			// Update existing key
			node.Values[idx] = value
		} else {

			// Insert new key
			node.Keys = append(node.Keys, nil)
			node.Values = append(node.Values, nil)

			// Shift elements
			copy(node.Keys[idx+1:], node.Keys[idx:])
			copy(node.Values[idx+1:], node.Values[idx:])

			node.Keys[idx] = key
			node.Values[idx] = value
		}

		_, err = bt.storeNode(node)
		return err
	}

	// Internal node
	idx := bt.findKeyIndex(node.Keys, key)
	childID := node.Children[idx]

	child, err := bt.loadNode(childID)
	if err != nil {
		return err
	}

	if bt.isNodeFull(child) {
		err = bt.splitChild(node, idx)
		if err != nil {
			return err
		}

		node, err = bt.loadNode(blockID)
		if err != nil {
			return err
		}

		// After split, determine which child to insert into
		if bytes.Compare(key, node.Keys[idx]) > 0 {
			idx++
		}
	}

	return bt.insertNonFull(node.Children[idx], key, value)
}

// splitChild splits a full child node into two nodes
func (bt *BTree) splitChild(parent *Node, childIdx int) error {
	child, err := bt.loadNode(parent.Children[childIdx])
	if err != nil {
		return err
	}

	mid := bt.metadata.Order - 1
	midKey := child.Keys[mid]
	midValue := child.Values[mid]

	// Create new right node
	rightNode := &Node{
		BlockID:  -1, // Initialize as new node
		IsLeaf:   child.IsLeaf,
		Keys:     make([][]byte, 0),
		Values:   make([]interface{}, 0),
		Children: make([]int64, 0),
		Parent:   parent.BlockID,
		NextLeaf: -1,
		PrevLeaf: -1,
	}

	// Copy keys and values from mid+1 to end to right node
	if mid+1 < len(child.Keys) {
		rightNode.Keys = make([][]byte, len(child.Keys[mid+1:]))
		copy(rightNode.Keys, child.Keys[mid+1:])
		rightNode.Values = make([]interface{}, len(child.Values[mid+1:]))
		copy(rightNode.Values, child.Values[mid+1:])
	}

	if child.IsLeaf {

		// Update leaf links
		rightNode.NextLeaf = child.NextLeaf
		rightNode.PrevLeaf = child.BlockID
	} else {
		// For internal nodes, copy children from mid+1 to end
		if mid+1 < len(child.Children) {
			rightNode.Children = make([]int64, len(child.Children[mid+1:]))
			copy(rightNode.Children, child.Children[mid+1:])
		}
	}

	// Store right node to get its BlockID
	rightBlockID, err := bt.storeNode(rightNode)
	if err != nil {
		return err
	}

	// Update leaf links with actual BlockID
	if child.IsLeaf {
		child.NextLeaf = rightBlockID
		rightNode.PrevLeaf = child.BlockID

		// Update next leafs prev pointer if it exists
		if rightNode.NextLeaf != -1 {
			nextLeaf, err := bt.loadNode(rightNode.NextLeaf)
			if err == nil {
				nextLeaf.PrevLeaf = rightBlockID
				_, err = bt.storeNode(nextLeaf)
				if err != nil {
					return err
				}
			}
		}
	} else {
		// Update parent pointers for children of right node
		for _, childID := range rightNode.Children {
			if grandchild, err := bt.loadNode(childID); err == nil {
				grandchild.Parent = rightBlockID
				_, err = bt.storeNode(grandchild)
				if err != nil {
					return err
				}
			}
		}
	}

	// Truncate left node
	if child.IsLeaf {

		// For leaf nodes, keep keys 0 to mid (inclusive) - middle key stays in left
		child.Keys = child.Keys[:mid+1]
		child.Values = child.Values[:mid+1]
	} else {

		// For internal nodes, keep keys 0 to mid-1, children 0 to mid
		child.Keys = child.Keys[:mid]
		child.Values = child.Values[:mid]
		child.Children = child.Children[:mid+1]
	}

	// Insert midKey into parent at the correct position
	parent.Keys = append(parent.Keys, nil)
	parent.Values = append(parent.Values, nil)
	parent.Children = append(parent.Children, 0)

	// Shift elements to make room for the new key
	insertIdx := childIdx
	copy(parent.Keys[insertIdx+1:], parent.Keys[insertIdx:])
	copy(parent.Values[insertIdx+1:], parent.Values[insertIdx:])
	copy(parent.Children[insertIdx+2:], parent.Children[insertIdx+1:])

	// Insert the middle key and right child reference
	parent.Keys[insertIdx] = midKey
	parent.Values[insertIdx] = midValue
	parent.Children[insertIdx+1] = rightBlockID

	// Store all updated nodes
	_, err = bt.storeNode(child)
	if err != nil {
		return err
	}

	_, err = bt.storeNode(rightNode)
	if err != nil {
		return err
	}

	_, err = bt.storeNode(parent)
	return err
}

// isNodeFull checks if a node is full
func (bt *BTree) isNodeFull(node *Node) bool {
	return len(node.Keys) >= 2*bt.metadata.Order-1
}

// findKeyIndex finds the index of the key in the sorted keys slice
func (bt *BTree) findKeyIndex(keys [][]byte, key []byte) int {
	return sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i], key) >= 0
	})
}

// storeNode stores a node in the block manager
func (bt *BTree) storeNode(node *Node) (int64, error) {
	data, err := bt.serializeNode(node)
	if err != nil {
		return -1, err
	}

	if node.BlockID != -1 && node.BlockID != 0 {

		// Update existing node
		blockID, err := bt.blockManager.Update(node.BlockID, data)
		if err != nil {
			return -1, err
		}

		node.BlockID = blockID
		return blockID, nil
	}

	blockID, err := bt.blockManager.Append(data)
	if err != nil {
		return -1, err
	}

	node.BlockID = blockID

	return blockID, nil
}

// loadNode loads a node from the block manager
func (bt *BTree) loadNode(blockID int64) (*Node, error) {
	if blockID == -1 {
		return nil, errors.New("invalid block ID")
	}

	data, _, err := bt.blockManager.Read(blockID)
	if err != nil {
		return nil, err
	}

	node, err := bt.deserializeNode(data)
	if err != nil {
		return nil, err
	}

	node.BlockID = blockID

	return node, nil
}

// GetExtraMeta retrieves extra metadata associated with the B-tree.
// This is set on btree creation.
func (bt *BTree) GetExtraMeta() interface{} {
	return bt.metadata.Extra
}

// serializeNode converts a Node into a byte array
func (bt *BTree) serializeNode(node *Node) ([]byte, error) {
	serialNode := struct {
		IsLeaf   bool
		Keys     [][]byte
		Values   []interface{}
		Children []int64
		Parent   int64
		NextLeaf int64
		PrevLeaf int64
	}{
		IsLeaf:   node.IsLeaf,
		Keys:     node.Keys,
		Values:   node.Values,
		Children: node.Children,
		Parent:   node.Parent,
		NextLeaf: node.NextLeaf,
		PrevLeaf: node.PrevLeaf,
	}

	b, err := bson.Marshal(&serialNode)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// deserializeNode converts a byte array back into a Node
func (bt *BTree) deserializeNode(data []byte) (*Node, error) {

	var serialNode struct {
		IsLeaf   bool
		Keys     [][]byte
		Values   []interface{}
		Children []int64
		Parent   int64
		NextLeaf int64
		PrevLeaf int64
	}

	err := bson.Unmarshal(data, &serialNode)
	if err != nil {
		return nil, err
	}

	node := &Node{
		IsLeaf:   serialNode.IsLeaf,
		Keys:     serialNode.Keys,
		Values:   serialNode.Values,
		Children: serialNode.Children,
		Parent:   serialNode.Parent,
		NextLeaf: serialNode.NextLeaf,
		PrevLeaf: serialNode.PrevLeaf,
	}

	return node, nil
}

// Close closes the B-tree and its block manager
func (bt *BTree) Close() error {

	return bt.blockManager.Close()
}

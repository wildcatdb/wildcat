// Package wildcat
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
package wildcat

import (
	"bytes"
	"fmt"
	"github.com/guycipher/wildcat/blockmanager"
	"github.com/guycipher/wildcat/bloomfilter"
	"os"
	"strconv"
	"sync"
)

// SSTable represents a sorted string table
type SSTable struct {
	Id          int64                    // SStable ID
	Min         []byte                   // The minimum key in the SSTable
	Max         []byte                   // The maximum key in the SSTable
	Size        int64                    // The size of the SSTable in bytes
	EntryCount  int                      // The number of entries in the SSTable
	Level       int                      // The level of the SSTable
	BloomFilter *bloomfilter.BloomFilter // Optional bloom filter for fast lookups
	isMerging   int32                    // Atomic flag indicating if the SSTable is being merged
	db          *DB                      // Reference to the database (not exported)
}

// SSTableIterator is an iterator for the SSTable
type SSTableIterator struct {
	iterator    *blockmanager.Iterator
	blockSet    BlockSet    // Current block set being iterated
	blockSetIdx int64       // Index of the current block set
	lock        *sync.Mutex // Mutex for thread safety
}

// KLogEntry represents a key-value entry in the KLog
type KLogEntry struct {
	Key          []byte // Key of the entry
	Timestamp    int64  // Timestamp of the entry
	ValueBlockID int64  // Block ID of the value
}

// BlockSet is a specific block with a set of klog entries
type BlockSet struct {
	Entries []*KLogEntry // List of entries in the block
	Size    int64        // Size of the block set
}

// get retrieves a value from the SSTable using the key and timestamp
func (sst *SSTable) get(key []byte, timestamp int64) ([]byte, int64) {
	// Skip range check if Min or Max are empty
	// Empty Min/Max indicate either an empty SSTable (which we can skip safely)
	// or a corrupted range
	if len(sst.Min) > 0 && len(sst.Max) > 0 {
		// Only skip if key is definitely outside the range
		if bytes.Compare(key, sst.Min) < 0 || bytes.Compare(key, sst.Max) > 0 {
			return nil, 0 // Key not in range
		}
	} else if sst.EntryCount == 0 {
		// If the SSTable is empty (as confirmed by EntryCount),
		// we can safely skip it regardless of Min/Max
		return nil, 0
	}

	if sst.db.opts.BloomFilter {
		// Check if the key is in the bloom filter
		if !sst.BloomFilter.Contains(key) {
			return nil, 0 // Key not in SSTable
		}

	}

	// Get the KLog block manager
	klogPath := sst.kLogPath()
	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		sst.db.log(fmt.Sprintf("KLog not in LRU cache, opening: %s", klogPath))
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, sst.db.opts.Permission,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			sst.db.log(fmt.Sprintf("Warning: Failed to open KLog %s: %v", klogPath, err))
			return nil, 0
		}
		sst.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
			// Close the block manager when evicted from LRU
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})
	}

	// Variables to track the latest valid version
	var foundEntry *KLogEntry = nil

	// Iterate through all block sets in the KLog
	iter := klogBm.Iterator()

	// Skip the first block which contains SSTable metadata
	_, _, err = iter.Next()
	if err != nil {
		return nil, 0
	}

	for {
		data, _, err := iter.Next()
		if err != nil {
			break // No more entries
		}

		var blockset BlockSet
		err = blockset.deserializeBlockSet(data)
		if err != nil {
			continue
		}

		// Check each entry in the block set
		for _, entry := range blockset.Entries {
			if bytes.Equal(entry.Key, key) {
				// Found a key match, now check timestamp (MVCC)
				// We want the latest version that's not after our read timestamp
				if entry.Timestamp <= timestamp && (foundEntry == nil || entry.Timestamp > foundEntry.Timestamp) {
					foundEntry = entry
				}
			}
		}
	}

	// If we found a valid entry, retrieve the value
	if foundEntry != nil {
		// Check if this is a deletion marker
		if foundEntry.ValueBlockID == -1 { // Special marker for deletion
			return nil, 0 // Key was deleted
		}

		// Get the VLog block manager
		vlogPath := sst.vLogPath()
		var vlogBm *blockmanager.BlockManager

		if v, ok := sst.db.lru.Get(vlogPath); ok {
			vlogBm = v.(*blockmanager.BlockManager)
		} else {
			vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, sst.db.opts.Permission,
				blockmanager.SyncOption(sst.db.opts.SyncOption))
			if err != nil {
				return nil, 0
			}
			sst.db.lru.Put(vlogPath, vlogBm, func(key, value interface{}) {
				// Close the block manager when evicted from LRU
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})
		}

		// Read the value from VLog
		value, _, err := vlogBm.Read(foundEntry.ValueBlockID)
		if err != nil {
			return nil, 0
		}

		// Check if the value itself is a tombstone marker
		if value == nil || len(value) == 0 {
			return nil, 0 // Key was deleted
		}

		// Return the value with its timestamp so we can compare values from different SSTables
		return value, foundEntry.Timestamp
	}

	return nil, 0 // Key not found or no valid version for the timestamp
}

// kLogPath returns the path to the KLog file for this SSTable
func (sst *SSTable) kLogPath() string {
	return sst.db.opts.Directory + LevelPrefix + strconv.Itoa(sst.Level) +
		string(os.PathSeparator) + SSTablePrefix + strconv.FormatInt(sst.Id, 10) + KLogExtension
}

// vLogPath returns the path to the VLog file for this SSTable
func (sst *SSTable) vLogPath() string {
	return sst.db.opts.Directory + LevelPrefix + strconv.Itoa(sst.Level) +
		string(os.PathSeparator) + SSTablePrefix + strconv.FormatInt(sst.Id, 10) + VLogExtension
}

// iterator creates a new iterator for the SSTable
func (sst *SSTable) iterator() *SSTableIterator {
	// Open the KLog block manager
	klogPath := sst.kLogPath()
	klogBm, err := blockmanager.Open(klogPath, os.O_RDONLY, sst.db.opts.Permission,
		blockmanager.SyncOption(sst.db.opts.SyncOption))
	if err != nil {
		return nil
	}

	// Create a new iterator for the KLog
	iter := klogBm.Iterator()

	// Skip the first block which contains SSTable metadata
	_, _, err = iter.Next()
	if err != nil {
		return nil
	}

	// Read first block
	data, _, err := iter.Next()
	if err != nil {
		return nil
	}

	// Deserialize the block set
	var blockset BlockSet
	err = blockset.deserializeBlockSet(data)
	if err != nil {
		return nil
	}

	return &SSTableIterator{iterator: iter, blockSetIdx: 0, lock: &sync.Mutex{}, blockSet: blockset}
}

// next retrieves the next key-value pair from the iterator
func (it *SSTableIterator) next() ([]byte, []byte, int64, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	// We check if blockSet idx is at end
	if it.blockSetIdx >= int64(len(it.blockSet.Entries)) {
		// Read the next block
		data, _, err := it.iterator.Next()
		if err != nil {
			return nil, nil, 0, err
		}

		// Deserialize the block set
		err = it.blockSet.deserializeBlockSet(data)
		if err != nil {
			return nil, nil, 0, err
		}

		it.blockSetIdx = 0

		// Get value from the block set entry
		val, _, err := it.iterator.BlockManager().Read(it.blockSet.Entries[it.blockSetIdx].ValueBlockID)
		if err != nil {
			return nil, nil, 0, err

		}

		return it.blockSet.Entries[it.blockSetIdx].Key, val, it.blockSet.Entries[it.blockSetIdx].Timestamp, nil
	} else {
		it.blockSetIdx++
		if it.blockSetIdx >= int64(len(it.blockSet.Entries)) {
			return nil, nil, 0, fmt.Errorf("end of block set reached")
		}

		val, _, err := it.iterator.BlockManager().Read(it.blockSet.Entries[it.blockSetIdx].ValueBlockID)
		if err != nil {
			return nil, nil, 0, err

		}

		return it.blockSet.Entries[it.blockSetIdx].Key, val, it.blockSet.Entries[it.blockSetIdx].Timestamp, nil
	}

}

// prev go back one entry in the iterator
func (it *SSTableIterator) prev() ([]byte, []byte, int64, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	// Check if we can go back
	if it.blockSetIdx <= 0 {
		// Go prev on the iterator
		data, _, err := it.iterator.Prev()
		if err != nil {
			return nil, nil, 0, err
		}

		// Deserialize the block set
		err = it.blockSet.deserializeBlockSet(data)
		if err != nil {
			return nil, nil, 0, err
		}

		// Set the block set index to the last entry
		it.blockSetIdx = int64(len(it.blockSet.Entries)) - 1
		if it.blockSetIdx < 0 {
			return nil, nil, 0, fmt.Errorf("no more entries in block set")
		}

		// Get value from the block set entry
		val, _, err := it.iterator.BlockManager().Read(it.blockSet.Entries[it.blockSetIdx].ValueBlockID)
		if err != nil {
			return nil, nil, 0, err
		}

		return it.blockSet.Entries[it.blockSetIdx].Key, val, it.blockSet.Entries[it.blockSetIdx].Timestamp, nil

	}

	// Decrement the index to go back
	it.blockSetIdx--

	// Get value from the block set entry
	val, _, err := it.iterator.BlockManager().Read(it.blockSet.Entries[it.blockSetIdx].ValueBlockID)
	if err != nil {
		return nil, nil, 0, err
	}

	return it.blockSet.Entries[it.blockSetIdx].Key, val, it.blockSet.Entries[it.blockSetIdx].Timestamp, nil
}

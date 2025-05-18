// Package orindb
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
package orindb

import (
	"bytes"
	"fmt"
	"orindb/blockmanager"
	"os"
	"strconv"
)

// SSTable represents a sorted string table
type SSTable struct {
	Id         int64  // SStable ID
	Min        []byte // The minimum key in the SSTable
	Max        []byte // The maximum key in the SSTable
	isMerging  int32  // Atomic flag indicating if the SSTable is being merged
	Size       int64  // The size of the SSTable in bytes
	EntryCount int    // The number of entries in the SSTable
	Level      int    // The level of the SSTable
	db         *DB    // Reference to the database (not exported)
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

type ValueWithTimestamp struct {
	Value     []byte
	Timestamp int64
}

// get retrieves a value from the SSTable using the key and timestamp
func (sst *SSTable) get(key []byte, timestamp int64) ([]byte, int64) {
	// Fix the range check - only proceed if key is in range
	if bytes.Compare(key, sst.Min) < 0 || bytes.Compare(key, sst.Max) > 0 {
		return nil, 0 // Key not in range
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
		sst.db.lru.Put(klogPath, klogBm)
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
			sst.db.lru.Put(vlogPath, vlogBm)
		}

		// Read the value from VLog
		value, _, err := vlogBm.Read(foundEntry.ValueBlockID)
		if err != nil {
			return nil, 0
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

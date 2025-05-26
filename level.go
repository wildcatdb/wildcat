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
	"fmt"
	"github.com/guycipher/wildcat/blockmanager"
	"github.com/guycipher/wildcat/tree"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// Level is a disk level within Wildcat, which contains a list of immutable SSTables
type Level struct {
	id          int                        // The level ID
	path        string                     // The path to the level directory
	sstables    atomic.Pointer[[]*SSTable] // Atomic pointer to the list of SSTables
	capacity    int                        // The capacity of the level
	currentSize int64                      // Atomic size of the level
	db          *DB                        // Reference to the database
}

// reopen opens an existing level directories sstables
// sstables are loaded and sorted by id
func (l *Level) reopen() error {
	// Read the level directory
	files, err := os.ReadDir(l.path)
	if err != nil {
		return fmt.Errorf("failed to read level directory: %w", err)
	}

	// Find KLog files to identify SSTables
	var sstables []*SSTable

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), KLogExtension) {
			continue
		}

		// Extract SSTable ID from the filename
		if !strings.HasPrefix(file.Name(), SSTablePrefix) {
			continue
		}

		idStr := strings.TrimPrefix(file.Name(), SSTablePrefix)
		idStr = strings.TrimSuffix(idStr, KLogExtension)
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse SSTable ID from filename %s: %w", file.Name(), err)
		}

		levelPath := l.path
		if !strings.HasSuffix(levelPath, string(os.PathSeparator)) {
			levelPath += string(os.PathSeparator)
		}

		// Get corresponding VLog file path
		vlogPath := fmt.Sprintf("%s%s%d%s", levelPath, SSTablePrefix, id, VLogExtension)

		// Check if VLog file exists
		if _, err := os.Stat(vlogPath); os.IsNotExist(err) {
			l.db.log(fmt.Sprintf("Warning: VLog file not found for SSTable %d: %v - skipping", id, err))
			continue
		}

		// Create SSTable structure with basic info
		sstable := &SSTable{
			Id:    id,
			Level: l.id,
			db:    l.db,
		}

		// Get file paths
		klogPath := fmt.Sprintf("%s%s%d%s", levelPath, SSTablePrefix, id, KLogExtension)

		// Get file sizes to calculate SSTable size
		klogInfo, err := os.Stat(klogPath)
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to stat KLog file for SSTable %d: %v - skipping", id, err))
			continue
		}

		vlogInfo, err := os.Stat(vlogPath)
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to stat VLog file for SSTable %d: %v - skipping", id, err))
			continue
		}

		// Calculate total size from file system
		sstable.Size = klogInfo.Size() + vlogInfo.Size()

		// Open the KLog file to try to get metadata from B-tree
		klogBm, err := blockmanager.Open(klogPath, os.O_RDONLY, l.db.opts.Permission, blockmanager.SyncOption(l.db.opts.SyncOption))
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to open KLog block manager for SSTable %d: %v - skipping", id, err))
			continue
		}

		// Add the KLog to cache
		l.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})

		// Try to open the B-tree to get metadata
		// An immutable btree in wildcat can store extra metadata for the tree itself.
		t, err := tree.Open(klogBm, l.db.opts.SSTableBTreeOrder, nil)
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to open B-tree for SSTable %d: %v - using file system metadata only", id, err))
			// Set basic metadata and continue
			sstable.Min = []byte{}
			sstable.Max = []byte{}
			sstable.EntryCount = 0
			sstables = append(sstables, sstable)
			continue
		}

		// Get the extra metadata from the B-tree
		extraMeta := t.GetExtraMeta()
		if extraMeta != nil {
			// Handle different types that might be returned
			switch meta := extraMeta.(type) {
			case *SSTable:
				// Perfect - we got the SSTable directly
				sstable.Min = meta.Min
				sstable.Max = meta.Max
				sstable.EntryCount = meta.EntryCount
				sstable.BloomFilter = meta.BloomFilter
				if meta.Size > 0 {
					sstable.Size = meta.Size // Use metadata size if available and valid
				}

			case primitive.D:
				// BSON document - need to extract fields manually
				l.extractSSTableFromBSON(sstable, meta)

			case map[string]interface{}:
				// Map interface - extract fields
				l.extractSSTableFromMap(sstable, meta)

			default:
				l.db.log(fmt.Sprintf("Warning: Unknown metadata type %T for SSTable %d - using file system metadata", extraMeta, id))
				sstable.Min = []byte{}
				sstable.Max = []byte{}
				sstable.EntryCount = 0
			}
		} else {
			// No metadata available - use empty values
			sstable.Min = []byte{}
			sstable.Max = []byte{}
			sstable.EntryCount = 0
		}

		l.db.log(fmt.Sprintf("Loaded SSTable %d: Size=%d bytes, Entries=%d",
			sstable.Id, sstable.Size, sstable.EntryCount))

		sstables = append(sstables, sstable)
	}

	// Sort SSTables by ID
	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].Id < sstables[j].Id
	})

	// Update the level's total size
	var totalSize int64
	for _, sstable := range sstables {
		totalSize += sstable.Size
	}
	l.setSize(totalSize)

	// Store the sorted SSTables
	l.sstables.Store(&sstables)

	l.db.log(fmt.Sprintf("Level %d reopen completed: %d SSTables, total size %d bytes",
		l.id, len(sstables), totalSize))

	return nil
}

// extractSSTableFromBSON a helper method to extract SSTable metadata from BSON primitive.D
func (l *Level) extractSSTableFromBSON(sstable *SSTable, doc primitive.D) {
	for _, elem := range doc {
		switch elem.Key {
		case "id":
			if id, ok := elem.Value.(int64); ok {
				sstable.Id = id
			}
		case "min":
			if minData, ok := elem.Value.(primitive.Binary); ok {
				sstable.Min = minData.Data
			} else if minBytes, ok := elem.Value.([]byte); ok {
				sstable.Min = minBytes
			}
		case "max":
			if maxData, ok := elem.Value.(primitive.Binary); ok {
				sstable.Max = maxData.Data
			} else if maxBytes, ok := elem.Value.([]byte); ok {
				sstable.Max = maxBytes
			}
		case "size":
			if size, ok := elem.Value.(int64); ok && size > 0 {
				sstable.Size = size
			}
		case "entrycount":
			if count, ok := elem.Value.(int32); ok {
				sstable.EntryCount = int(count)
			} else if count, ok := elem.Value.(int64); ok {
				sstable.EntryCount = int(count)
			}
		case "level":
			if level, ok := elem.Value.(int32); ok {
				sstable.Level = int(level)
			} else if level, ok := elem.Value.(int64); ok {
				sstable.Level = int(level)
			}
		}
	}
}

// extractSSTableFromMap a helper method to extract SSTable metadata from map[string]interface{}
func (l *Level) extractSSTableFromMap(sstable *SSTable, meta map[string]interface{}) {
	if id, ok := meta["id"].(int64); ok {
		sstable.Id = id
	}
	if minBytes, ok := meta["min"].([]byte); ok {
		sstable.Min = minBytes
	}
	if maxBytes, ok := meta["max"].([]byte); ok {
		sstable.Max = maxBytes
	}
	if size, ok := meta["size"].(int64); ok && size > 0 {
		sstable.Size = size
	}
	if count, ok := meta["entrycount"].(int); ok {
		sstable.EntryCount = count
	} else if count, ok := meta["entrycount"].(int32); ok {
		sstable.EntryCount = int(count)
	} else if count, ok := meta["entrycount"].(int64); ok {
		sstable.EntryCount = int(count)
	}
	if level, ok := meta["level"].(int); ok {
		sstable.Level = level
	} else if level, ok := meta["level"].(int32); ok {
		sstable.Level = int(level)
	} else if level, ok := meta["level"].(int64); ok {
		sstable.Level = int(level)
	}
}

// getSize returns the current size of the level
func (l *Level) getSize() int64 {
	return atomic.LoadInt64(&l.currentSize)
}

// setSize sets the current size of the level
func (l *Level) setSize(size int64) {
	atomic.StoreInt64(&l.currentSize, size)
}

// SSTables returns the list of SSTables in the level
func (l *Level) SSTables() []*SSTable {
	return *l.sstables.Load()
}

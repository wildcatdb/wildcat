package wildcat

import (
	"fmt"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/tree"
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
	l.db.log(fmt.Sprintf("Opening level %d at path %s", l.id, l.path))

	// Read the level directory
	files, err := os.ReadDir(l.path)
	if err != nil {
		return fmt.Errorf("failed to read level directory: %w", err)
	}

	// We find KLog files to identify SSTables
	var sstables []*SSTable

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), KLogExtension) {
			continue
		}

		// If we find a file with TempFileExtension we remove it
		if strings.HasSuffix(file.Name(), TempFileExtension) {
			tempFilePath := fmt.Sprintf("%s%s", l.path, file.Name())
			if err := os.Remove(tempFilePath); err != nil {
				l.db.log(fmt.Sprintf("Warning: Failed to remove temporary file %s: %v", tempFilePath, err))
			}

			l.db.log(fmt.Sprintf("Removed temporary file: %s", tempFilePath))

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

		l.db.log(fmt.Sprintf("Found SSTable %d: KLog=%s, VLog=%s", id, file.Name(), vlogPath))

		// Get file paths
		klogPath := fmt.Sprintf("%s%s%d%s", levelPath, SSTablePrefix, id, KLogExtension)

		// Open the KLog file to try to get metadata from B-tree
		klogBm, err := blockmanager.Open(klogPath, os.O_RDONLY, l.db.opts.Permission, blockmanager.SyncOption(l.db.opts.SyncOption))
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to open KLog block manager for SSTable %d: %v - skipping", id, err))
			continue
		}

		// Add the KLog to cache
		l.db.lru.Put(klogPath, klogBm, func(key string, value interface{}) {
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})

		// Try to open the B-tree to get metadata
		// An immutable btree in wildcat can store extra metadata for the tree itself.
		t, err := tree.Open(klogBm, l.db.opts.SSTableBTreeOrder, nil)
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to open B-tree for SSTable %d: %v - using file system metadata only", id, err))

			// Set basic metadata and continue, the system is still usable and will attempt to search the tree still if no metadata is found
			sstable.Min = []byte{}
			sstable.Max = []byte{}
			sstable.EntryCount = 0
			sstables = append(sstables, sstable)
			continue
		}

		// Get the extra metadata from the B-tree
		extraMeta := t.GetExtraMeta()
		if extraMeta != nil {
			switch meta := extraMeta.(type) {
			case *SSTable:
				sstable.Min = meta.Min
				sstable.Max = meta.Max
				sstable.EntryCount = meta.EntryCount
				sstable.BloomFilter = meta.BloomFilter
				if meta.Size > 0 {
					sstable.Size = meta.Size // Use metadata size if available and valid
				}

			case primitive.D:
				l.extractSSTableFromBSON(sstable, meta)

			case map[string]interface{}:
				l.extractSSTableFromMap(sstable, meta)

			default:
				// This should not occur but if it does log a warning
				// The system will still function using the sstable but without proper metadata setting nil
				l.db.log(fmt.Sprintf("Warning: Unknown metadata type %T for SSTable %d - using file system metadata", extraMeta, id))

				sstable.Min = nil
				sstable.Max = nil
				sstable.EntryCount = 0
			}
		} else {
			sstable.Min = nil
			sstable.Max = nil
			sstable.EntryCount = 0
		}

		l.db.log(fmt.Sprintf("Loaded SSTable %d: Size=%d bytes, Entries=%d",
			sstable.Id, sstable.Size, sstable.EntryCount))

		sstables = append(sstables, sstable)
	}

	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].Id < sstables[j].Id
	})

	// Update the level's total size
	var totalSize int64
	for _, sstable := range sstables {
		totalSize += sstable.Size
	}
	l.setSize(totalSize)

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
		case "timestamp":
			if timestamp, ok := elem.Value.(int64); ok {
				sstable.Timestamp = timestamp
			}
		case "bloomfilter":
			if l.db.opts.BloomFilter {
				if err := sstable.reconstructBloomFilter(); err != nil {
					l.db.log(fmt.Sprintf("Warning: Failed to reconstruct bloom filter for SSTable %d: %v", sstable.Id, err))
				}
			}
		}
	}
}

// extractSSTableFromMap a helper method to extract SSTable metadata from map[string]interface{}
func (l *Level) extractSSTableFromMap(sstable *SSTable, meta map[string]interface{}) {
	for key, value := range meta {
		switch key {
		case "id":
			if id, ok := value.(int64); ok {
				sstable.Id = id
			}
		case "min":
			if minBytes, ok := value.([]byte); ok {
				sstable.Min = minBytes
			}
		case "max":
			if maxBytes, ok := value.([]byte); ok {
				sstable.Max = maxBytes
			}
		case "size":
			if size, ok := value.(int64); ok && size > 0 {
				sstable.Size = size
			}
		case "entrycount":
			switch count := value.(type) {
			case int:
				sstable.EntryCount = count
			case int32:
				sstable.EntryCount = int(count)
			case int64:
				sstable.EntryCount = int(count)
			}
		case "level":
			switch level := value.(type) {
			case int:
				sstable.Level = level
			case int32:
				sstable.Level = int(level)
			case int64:
				sstable.Level = int(level)
			}
		case "timestamp":
			if timestamp, ok := value.(int64); ok {
				sstable.Timestamp = timestamp
			}
		}
	}

	if l.db.opts.BloomFilter {
		if err := sstable.reconstructBloomFilter(); err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to reconstruct bloom filter for SSTable %d: %v", sstable.Id, err))
		}
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

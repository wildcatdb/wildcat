package wildcat

import (
	"bytes"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/bloomfilter"
	"github.com/wildcatdb/wildcat/v2/tree"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"strconv"
	"strings"
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
	Timestamp   int64                    // Timestamp of latest entry in the SSTable
	isMerging   int32                    // Atomic flag indicating if the SSTable is being merged
	db          *DB                      // Reference to the database (not exported)
}

// KLogEntry represents a key-value entry in the KLog
type KLogEntry struct {
	Key          []byte // Key of the entry
	Timestamp    int64  // Timestamp of the entry
	ValueBlockID int64  // Block ID of the value
}

// get retrieves a value from the SSTable using the key and timestamp
func (sst *SSTable) get(key []byte, readTimestamp int64) ([]byte, int64) {
	// Get the KLog block manager
	klogPath := sst.kLogPath()
	var klogBm *blockmanager.BlockManager
	var err error

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

	// If bloom filters are configured
	// we check if the key is in the bloom filter
	// if so we continue on if not we skip
	if sst.db.opts.BloomFilter {
		// Check if the key is in the bloom filter
		if !sst.BloomFilter.Contains(key) {
			return nil, 0 // Key not in SSTable
		}

	}

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, sst.db.opts.Permission, blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil, 0
		}
		sst.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})
	}

	t, err := tree.Open(klogBm, sst.db.opts.SSTableBTreeOrder, sst)
	if err != nil {
		return nil, 0
	}

	val, _, err := t.Get(key)
	if err != nil {
		if strings.Contains(err.Error(), "CRC mismatch") {
			// If we encounter a CRC mismatch, we assume the SSTable is corrupted
			sst.db.log("SSTable " + strconv.FormatInt(sst.Id, 10) + " at level " + strconv.FormatInt(int64(sst.Level), 10) + " block corruption detected: " + err.Error())
			return nil, 0
		}
		return nil, 0
	}

	if val == nil {
		return nil, 0
	}

	var entry *KLogEntry

	if klogEntry, ok := val.(*KLogEntry); ok {
		entry = klogEntry
	} else if doc, ok := val.(primitive.D); ok {
		entry = &KLogEntry{}

		// Extract fields from primitive.D (bson)
		for _, elem := range doc {
			switch elem.Key {
			case "key":
				if keyData, ok := elem.Value.(primitive.Binary); ok {
					entry.Key = keyData.Data
				}
			case "timestamp":
				if ts, ok := elem.Value.(int64); ok {
					entry.Timestamp = ts
				}
			case "valueblockid":
				if blockID, ok := elem.Value.(int64); ok {
					entry.ValueBlockID = blockID
				}
			}
		}
	} else {
		// Unknown type, try to convert via BSON
		bsonData, err := bson.Marshal(val)
		if err != nil {
			return nil, 0
		}

		entry = &KLogEntry{}
		err = bson.Unmarshal(bsonData, entry)
		if err != nil {
			return nil, 0
		}
	}

	// Only return if this version is visible to the read timestamp
	if entry.Timestamp <= readTimestamp {
		if entry.ValueBlockID == -1 {
			return nil, entry.Timestamp // Return nil value but valid timestamp for deletion
		}
		v := sst.readValueFromVLog(entry.ValueBlockID)
		return v, entry.Timestamp
	}

	return nil, 0
}

// readValueFromVLog reads a value from the VLog using the block ID
func (sst *SSTable) readValueFromVLog(valueBlockID int64) []byte {
	vlogPath := sst.vLogPath()
	var vlogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(vlogPath); ok {
		vlogBm = v.(*blockmanager.BlockManager)
	} else {
		vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, sst.db.opts.Permission, blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil
		}
		sst.db.lru.Put(vlogPath, vlogBm, func(key, value interface{}) {
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})
	}

	value, _, err := vlogBm.Read(valueBlockID)
	if err != nil {
		return nil
	}
	return value
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

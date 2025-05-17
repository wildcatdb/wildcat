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
	"errors"
	"fmt"
	"math"
	"orindb/blockmanager"
	"orindb/lru"
	"orindb/skiplist"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Constants for compaction policy
const (
	CompactionCooldownPeriod   = 100 * time.Millisecond
	CompactionBatchSize        = 4   // Max number of SSTables to compact at once
	CompactionSizeRatio        = 1.2 // Level size ratio that triggers compaction
	CompactionSizeThreshold    = 4   // Number of files to trigger size-tiered compaction
	CompactionScoreSizeWeight  = 0.7 // Weight for size-based score
	CompactionScoreCountWeight = 0.3 // Weight for count-based score
	MaxCompactionConcurrency   = 2   // Maximum concurrent compactions
)

const (
	SSTablePrefix    = "sst_"  // Prefix for SSTable files
	WALFileExtension = ".wal"  // Extension for Write Ahead Log files <timestamp>.wal
	KLogExtension    = ".klog" // Extension for KLog files
	VLogExtension    = ".vlog" // Extension for VLog files
	LevelPrefix      = "l"     // Prefix for level directories i.e. "l0", "l1", etc.
)

type SyncOption int

const (
	SyncNone SyncOption = iota
	SyncFull
	SyncPartial
)

// Defaults
const (
	DefaultWriteBufferSize     = 16 * 1024 * 1024 // 4MB
	DefaultSyncOption          = SyncNone
	DefaultSyncInterval        = 1 * time.Second
	DefaultLevelCount          = 7
	DefaultLevelMultiplier     = 4
	DefaultBlockManagerLRUSize = 128             // Size of the LRU cache for block managers
	DefaultBlockSetSize        = 8 * 1024 * 1024 // Size of the block set
	DefaultPermission          = 0750
)

// Options represents the configuration options for OrinDB
type Options struct {
	Directory           string        // Directory for OrinDB
	WriteBufferSize     int64         // Size of the write buffer
	SyncOption          SyncOption    // Sync option for write operations
	SyncInterval        time.Duration // Interval for syncing the write buffer
	LevelCount          int           // Number of levels in the LSM tree
	LevelMultiplier     int           // Multiplier for the number of levels
	BlockManagerLRUSize int           // Size of the LRU cache for block managers
	BlockSetSize        int64         // Amount of entries per klog block (in bytes)
	Permission          os.FileMode   // Permission for created files
	LogChannel          chan string   // Channel for logging
}

// DB represents the main OrinDB structure
type DB struct {
	opts           *Options                 // Configuration options
	txns           atomic.Pointer[[]*Txn]   // Atomic pointer to the transactions
	levels         atomic.Pointer[[]*Level] // Atomic pointer to the levels
	lru            *lru.LRU                 // LRU cache for block managers
	flusher        *Flusher                 // Flusher for memtables
	compactor      *Compactor               // Compactor for SSTables
	memtable       atomic.Value             // The current memtable
	wg             *sync.WaitGroup          // WaitGroup for synchronization
	closeCh        chan struct{}            // Channel for closing up
	sstIdGenerator *IDGenerator             // ID generator for SSTables
	walIdGenerator *IDGenerator             // ID generator for WAL files
	logChannel     chan string              // Log channel, instead of log file or standard output we log to a channel
}

// Open initializes a new OrinDB instance with the provided options
func Open(opts *Options) (*DB, error) {
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if opts.Directory == "" {
		return nil, errors.New("directory cannot be empty")
	}

	if opts.WriteBufferSize <= 0 {
		opts.WriteBufferSize = DefaultWriteBufferSize
	}

	if opts.SyncOption < SyncNone || opts.SyncOption > SyncPartial {
		opts.SyncOption = DefaultSyncOption
	}

	if opts.SyncInterval <= 0 {
		opts.SyncInterval = DefaultSyncInterval
	}

	if opts.LevelCount <= 0 {
		opts.LevelCount = DefaultLevelCount
	}

	if opts.LevelMultiplier <= 0 {
		opts.LevelMultiplier = DefaultLevelMultiplier
	}

	if opts.BlockManagerLRUSize <= 0 {
		opts.BlockManagerLRUSize = DefaultBlockManagerLRUSize
	}

	if opts.BlockSetSize <= 0 {
		opts.BlockSetSize = DefaultBlockSetSize
	}

	if opts.Permission == 0 {
		opts.Permission = DefaultPermission
	}

	db := &DB{
		lru:            lru.New(int64(opts.BlockManagerLRUSize), 0.25, 0.7),
		wg:             &sync.WaitGroup{},
		opts:           opts,
		txns:           atomic.Pointer[[]*Txn]{},
		closeCh:        make(chan struct{}),
		sstIdGenerator: NewIDGenerator(),
		walIdGenerator: NewIDGenerator(),
	}

	db.flusher = newFlusher(db)
	db.compactor = newCompactor(db, MaxCompactionConcurrency)

	if !strings.HasSuffix(db.opts.Directory, string(os.PathSeparator)) {
		db.opts.Directory += string(os.PathSeparator)
	}

	if _, err := os.Stat(db.opts.Directory); os.IsNotExist(err) {
		db.log(fmt.Sprintf("Directory %s does not exist, creating it...", db.opts.Directory))
		// Create the directory if it does not exist
		if err := os.MkdirAll(db.opts.Directory, db.opts.Permission); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}

	}

	// Initialize the levels array
	levels := make([]*Level, db.opts.LevelCount)
	for i := 0; i < db.opts.LevelCount; i++ {
		level := &Level{
			id:       i + 1,
			capacity: int(float64(db.opts.WriteBufferSize) * math.Pow(float64(db.opts.LevelMultiplier), float64(i))),
		}

		level.sstables = atomic.Pointer[[]*SSTable]{}
		level.path = fmt.Sprintf("%s%s%d%s", db.opts.Directory, LevelPrefix, i+1, string(os.PathSeparator))

		levels[i] = level

		db.log(fmt.Sprintf("Creating level %d with capacity %d bytes at path %s", level.id, level.capacity, level.path))

		// Create or ensure the level directory exists
		if err := os.MkdirAll(level.path, db.opts.Permission); err != nil {
			return nil, fmt.Errorf("failed to create level directory: %v", err)
		}

		// Reopen the level to load existing SSTable information
		if err := level.reopen(); err != nil {
			return nil, fmt.Errorf("failed to reopen level: %v", err)
		}

	}

	// Set the levels in the LSM tree
	db.levels.Store(&levels)

	// We open the available write ahead log files in set directory
	// the WAL files are used for recovery in case of a crash.
	// We sort the WAL files by their timestamp and open them in order.
	// The last added WAL is our current WAL, the rest are immutable and enqueued to flusher to be flushed.
	if err := db.reinstate(); err != nil {
		return nil, fmt.Errorf("failed to open WALs: %w", err)
	}

	// Start the background flusher
	db.wg.Add(1)
	go db.flusher.backgroundProcess()

	// Start the compaction manager
	db.wg.Add(1)
	go db.compactor.backgroundProcess()

	return db, nil

}

// Close closes the database and all open resources
func (db *DB) Close() error {
	if db == nil {
		return errors.New("database is nil")
	}

	db.log("Closing database...")

	// Send signal to close the database
	select {
	case <-db.closeCh:
		// Channel is already closed, do nothing
	default:
		// Close the channel to stop background sync
		close(db.closeCh)
	}
	db.wg.Wait()

	// Close open block managers
	db.lru.ForEach(func(key, value interface{}, accessCount uint64) bool {
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
		return true
	})

	db.log("Database closed successfully.")

	if db.opts.LogChannel != nil {
		close(db.opts.LogChannel)
	}

	return nil
}

// reinstate reopens the WAL files and replays them to restore the state of the database
func (db *DB) reinstate() error {
	db.log("Reinstating state...")
	walDir, err := os.ReadDir(db.opts.Directory)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	var walFiles []string
	for _, file := range walDir {
		if file.IsDir() || !strings.HasSuffix(file.Name(), WALFileExtension) {
			continue
		}
		walFiles = append(walFiles, file.Name())
	}

	// Sort WAL files by timestamp (ascending order - oldest first)
	sort.Slice(walFiles, func(i, j int) bool {
		tsI := extractTimestampFromFilename(walFiles[i])
		tsJ := extractTimestampFromFilename(walFiles[j])
		return tsI < tsJ
	})

	if len(walFiles) == 0 {
		db.log("No WAL files found, creating new memtable and WAL...")
		// No WAL files found, create a new memtable and WAL

		db.memtable.Store(&Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: fmt.Sprintf("%s%d%s", db.opts.Directory, time.Now().UnixNano(), WALFileExtension),
			},
			db: db,
		})

		walBm, err := blockmanager.Open(db.memtable.Load().(*Memtable).wal.path, os.O_RDWR|os.O_CREATE, db.opts.Permission, blockmanager.SyncOption(db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add the WAL to the LRU cache
		db.lru.Put(db.memtable.Load().(*Memtable).wal.path, walBm)

		// Initialize empty transactions slice
		txns := make([]*Txn, 0)
		db.txns.Store(&txns)

		db.log(fmt.Sprintf("Created new memtable and WAL at %s", db.memtable.Load().(*Memtable).wal.path))

		return nil // No WAL files, just return as we created a new memtable and WAL
	}

	// Initialize the transactions map
	allTxns := make([]*Txn, 0)

	// Process all but the latest WAL file as immutable memtables
	for _, walFile := range walFiles[:len(walFiles)-1] {
		walPath := fmt.Sprintf("%s%s", db.opts.Directory, walFile)

		// Create a memtable for this WAL
		immutableMemt := &Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: walPath,
			},
			db: db,
		}

		// Open the WAL file
		walBm, err := blockmanager.Open(walPath, os.O_RDONLY, db.opts.Permission, blockmanager.SyncOption(db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add WAL to LRU cache
		db.lru.Put(walPath, walBm)

		// Replay transactions from this WAL to the memtable
		// We don't need to track transactions from immutable WALs as they should all be committed
		err = immutableMemt.replay(nil)
		if err != nil {
			return fmt.Errorf("failed to replay WAL: %w", err)
		}

		db.flusher.enqueueMemtable(immutableMemt)

	}

	// Open the latest WAL file as the active WAL
	activeWAL := walFiles[len(walFiles)-1]
	activeWALPath := fmt.Sprintf("%s%s", db.opts.Directory, activeWAL)

	db.memtable.Store(&Memtable{
		skiplist: skiplist.New(),
		wal: &WAL{
			path: activeWALPath,
		},
		db: db,
	})

	// Open the active WAL
	walBm, err := blockmanager.Open(activeWALPath, os.O_RDWR, db.opts.Permission, blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open active WAL block manager: %w", err)
	}

	// Add the WAL to the LRU cache
	db.lru.Put(activeWALPath, walBm)

	// For the active WAL, we need to track transactions that are not yet committed
	activeTxns := make([]*Txn, 0)

	// Replay transactions from active WAL to the memtable and collect active transactions
	if err := db.memtable.Load().(*Memtable).replay(&activeTxns); err != nil {
		return fmt.Errorf("failed to replay active WAL: %w", err)
	}

	// Update the transaction state in the database
	allTxns = append(allTxns, activeTxns...)
	db.txns.Store(&allTxns)

	db.log(fmt.Sprintf("Reinstatement of state completed with memory table size: %d and %d wal files", atomic.LoadInt64(&db.memtable.Load().(*Memtable).size), len(walFiles[:len(walFiles)-1])))

	return nil
}

// log logs a message to the log channel
func (db *DB) log(msg string) {
	if db.opts.LogChannel != nil {
		db.opts.LogChannel <- msg
	}
}

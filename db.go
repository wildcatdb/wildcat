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
	SSTablePrefix    = "sst_"     // Prefix for SSTable files
	LevelPrefix      = "l"        // Prefix for level directories i.e. "l0", "l1", etc.
	WALFileExtension = ".wal"     // Extension for Write Ahead Log files <timestamp>.wal
	KLogExtension    = ".klog"    // Extension for KLog files
	VLogExtension    = ".vlog"    // Extension for VLog files
	IDGSTFileName    = "idgstate" // Filename for ID generator state
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
	txnIdGenerator *IDGenerator             // ID generator for transactions
	logChannel     chan string              // Log channel, instead of log file or standard output we log to a channel
	idgs           *IDGeneratorState        // ID generator state
}

// IDGeneratorState represents the state of the ID generator.
// When system shuts down the state is saved to disk and restored on next startup.
type IDGeneratorState struct {
	lastSstID int64 // Last SSTable ID
	lastWalID int64 // Last WAL ID
	lastTxnID int64 // Last transaction ID
	db        *DB   // Pointer to the database instance
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
		lru:     lru.New(int64(opts.BlockManagerLRUSize), 0.25, 0.7),
		wg:      &sync.WaitGroup{},
		opts:    opts,
		txns:    atomic.Pointer[[]*Txn]{},
		closeCh: make(chan struct{}),
	}

	db.idgs = &IDGeneratorState{
		lastSstID: 0,
		lastWalID: 0,
		lastTxnID: 0,
		db:        db,
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

	// Check if the ID generator state file exists
	idgsFilePath := fmt.Sprintf("%s%s", db.opts.Directory, IDGSTFileName)
	if _, err := os.Stat(idgsFilePath); os.IsNotExist(err) {
		db.idgs = &IDGeneratorState{
			lastSstID: 0,
			lastWalID: 0,
			lastTxnID: 0,
			db:        db,
		}

		db.txnIdGenerator = newIDGenerator()
		db.walIdGenerator = newIDGenerator()
		db.sstIdGenerator = newIDGenerator()
	} else {
		// Directory exists, load the ID generator state
		if err := db.idgs.loadState(); err != nil {
			return nil, fmt.Errorf("failed to load ID generator state: %w", err)
		}

	}

	// Initialize the levels array
	levels := make([]*Level, db.opts.LevelCount)
	for i := 0; i < db.opts.LevelCount; i++ {
		level := &Level{
			id:       i + 1,
			capacity: int(float64(db.opts.WriteBufferSize) * math.Pow(float64(db.opts.LevelMultiplier), float64(i))),
			db:       db,
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

	// Save the ID generator state to disk
	if err := db.idgs.saveState(); err != nil {
		return fmt.Errorf("failed to save ID generator state: %w", err)
	}

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

	// Sort WAL files by id (ascending order - oldest first)
	sort.Slice(walFiles, func(i, j int) bool {
		tsI := extractIDFromFilename(walFiles[i])
		tsJ := extractIDFromFilename(walFiles[j])
		return tsI < tsJ
	})

	if len(walFiles) == 0 {
		db.log("No WAL files found, creating new memtable and WAL...")
		// No WAL files found, create a new memtable and WAL

		db.memtable.Store(&Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: fmt.Sprintf("%s%d%s", db.opts.Directory, db.walIdGenerator.nextID(), WALFileExtension),
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
	for i, walFile := range walFiles[:len(walFiles)-1] {
		walPath := fmt.Sprintf("%s%s", db.opts.Directory, walFile)
		db.log(fmt.Sprintf("Processing immutable WAL %d of %d: %s", i+1, len(walFiles)-1, walPath))

		// Create a memtable for this WAL
		immutableMemt := &Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: walPath,
			},
			db: db,
		}

		// Open the WAL file with improved error handling
		walBm, err := blockmanager.Open(walPath, os.O_RDONLY, db.opts.Permission, blockmanager.SyncOption(db.opts.SyncOption))
		if err != nil {
			db.log(fmt.Sprintf("Warning: Failed to open immutable WAL %s: %v - skipping", walPath, err))
			continue // Skip this WAL instead of failing completely
		}

		// Add WAL to LRU cache
		db.lru.Put(walPath, walBm)

		// Replay transactions from this WAL to the memtable
		// We pass an empty slice to track any uncommitted transactions
		activeTxns := make([]*Txn, 0)
		err = immutableMemt.replay(&activeTxns)
		if err != nil {
			db.log(fmt.Sprintf("Warning: Failed to replay immutable WAL %s: %v - skipping", walPath, err))
			continue // Skip this WAL instead of failing completely
		}

		// Log any uncommitted transactions found in immutable WALs (this shouldn't happen)
		if len(activeTxns) > 0 {
			db.log(fmt.Sprintf("Warning: Found %d uncommitted transactions in immutable WAL %s", len(activeTxns), walPath))
			// Add any uncommitted transactions to our tracking
			allTxns = append(allTxns, activeTxns...)
		}

		db.flusher.enqueueMemtable(immutableMemt)
		db.log(fmt.Sprintf("Successfully processed immutable WAL %s", walPath))
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

// loadState loads the ID generator state from a file
func (idgs *IDGeneratorState) loadState() error {
	if idgs == nil {
		return errors.New("IDGeneratorState is nil")
	}

	// Open the ID generator state file
	idgsFilePath := fmt.Sprintf("%s%s", idgs.db.opts.Directory, IDGSTFileName)
	idgsFile, err := os.Open(idgsFilePath)
	if err != nil {
		return fmt.Errorf("failed to open ID generator state file: %w", err)
	}
	defer idgsFile.Close()

	// Read the ID generator state from the file
	if _, err := fmt.Fscanf(idgsFile, "%d %d %d", &idgs.lastSstID, &idgs.lastWalID, &idgs.lastTxnID); err != nil {
		return fmt.Errorf("failed to read ID generator state: %w", err)
	}

	idgs.db.txnIdGenerator = reloadIDGenerator(idgs.lastTxnID)
	idgs.db.walIdGenerator = reloadIDGenerator(idgs.lastWalID)
	idgs.db.sstIdGenerator = reloadIDGenerator(idgs.lastSstID)
	idgs.db.log(fmt.Sprintf("Loaded ID generator state: %d %d %d", idgs.lastSstID, idgs.lastWalID, idgs.lastTxnID))

	return nil
}

// saveState saves the current state of the ID generator to a file
func (idgs *IDGeneratorState) saveState() error {
	if idgs == nil {
		return errors.New("IDGeneratorState is nil")
	}

	idgs.lastTxnID = idgs.db.txnIdGenerator.save()
	idgs.lastWalID = idgs.db.walIdGenerator.save()
	idgs.lastSstID = idgs.db.sstIdGenerator.save()
	idgs.db.log(fmt.Sprintf("Saving ID generator state: %d %d %d", idgs.lastSstID, idgs.lastWalID, idgs.lastTxnID))

	// Open the ID generator state file
	idgsFilePath := fmt.Sprintf("%s%s", idgs.db.opts.Directory, IDGSTFileName)
	idgsFile, err := os.OpenFile(idgsFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, idgs.db.opts.Permission)
	if err != nil {
		return fmt.Errorf("failed to open ID generator state file: %w", err)
	}
	defer idgsFile.Close()

	// Write the ID generator state to the file
	if _, err := fmt.Fprintf(idgsFile, "%d %d %d", idgs.lastSstID, idgs.lastWalID, idgs.lastTxnID); err != nil {
		return fmt.Errorf("failed to write ID generator state: %w", err)
	}

	// Sync the file to ensure data is written
	if err := idgsFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync ID generator state file: %w", err)
	}

	return nil
}

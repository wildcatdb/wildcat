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
	"errors"
	"fmt"
	"github.com/guycipher/wildcat/blockmanager"
	"github.com/guycipher/wildcat/lru"
	"github.com/guycipher/wildcat/skiplist"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SyncOption int

const (
	SyncNone SyncOption = iota
	SyncFull
	SyncPartial
)

// Prefixes, filenames, extensions constants
const (
	SSTablePrefix    = "sst_"     // Prefix for SSTable files
	LevelPrefix      = "l"        // Prefix for level directories i.e. "l0", "l1", etc.
	WALFileExtension = ".wal"     // Extension for Write Ahead Log files <timestamp>.wal
	KLogExtension    = ".klog"    // Extension for KLog files
	VLogExtension    = ".vlog"    // Extension for VLog files
	IDGSTFileName    = "idgstate" // Filename for ID generator state
)

// Defaults
const (
	DefaultWriteBufferSize             = 128 * 1024 * 1024    // Default write buffer size
	DefaultSyncOption                  = SyncNone             // Default sync option for write operations
	DefaultSyncInterval                = 16 * time.Nanosecond // Default sync interval for write operations
	DefaultLevelCount                  = 7                    // Default number of levels in the LSM tree
	DefaultLevelMultiplier             = 4                    // Multiplier for the number of levels
	DefaultBlockManagerLRUSize         = 1024                 // Size of the LRU cache for block managers
	DefaultBlockManagerLRUEvictRatio   = 0.25                 // Eviction ratio for the LRU cache
	DefaultBlockManagerLRUAccessWeight = 0.7                  // Access weight for the LRU cache
	DefaultBlockSetSize                = 8 * 1024 * 1024      // Size of the block set
	DefaultPermission                  = 0750                 // Default permission for created files
	DefaultBloomFilter                 = false                // Default Bloom filter option
	DefaultMaxCompactionConcurrency    = 2                    // Default max compaction concurrency
	DefaultCompactionCooldownPeriod    = 1 * time.Second      // Default cooldown period for compaction
	DefaultCompactionBatchSize         = 4                    // Default max number of SSTables to compact at once
	DefaultCompactionSizeRatio         = 1.2                  // Default level size ratio that triggers compaction
	DefaultCompactionSizeThreshold     = 4                    // Default number of files to trigger size-tiered compaction
	DefaultCompactionScoreSizeWeight   = 0.7                  // Default weight for size-based score
	DefaultCompactionScoreCountWeight  = 0.3                  // Default weight for count-based score
	DefaultFlusherTickerInterval       = 64 * time.Microsecond
	DefaultCompactorTickerInterval     = 64 * time.Millisecond  // Default interval for compactor ticker
	DefaultBloomFilterProbability      = 0.01                   // Default probability for Bloom filter
	DefaultWALAppendRetry              = 10                     // Default number of retries for WAL append
	DefaultWALAppendBackoff            = 128 * time.Microsecond // Default backoff duration for WAL append

)

// Options represents the configuration options for Wildcat
type Options struct {
	Directory                  string        // Directory for Wildcat
	WriteBufferSize            int64         // Size of the write buffer
	SyncOption                 SyncOption    // Sync option for write operations
	SyncInterval               time.Duration // Interval for syncing the write buffer
	LevelCount                 int           // Number of levels in the LSM tree
	LevelMultiplier            int           // Multiplier for the number of levels
	BlockManagerLRUSize        int           // Size of the LRU cache for block managers
	BlockManagerLRUEvictRatio  float64       // Eviction ratio for the LRU cache
	BlockManagerLRUAccesWeight float64       // Access weight for the LRU cache
	BlockSetSize               int64         // Amount of entries per klog block (in bytes)
	Permission                 os.FileMode   // Permission for created files
	LogChannel                 chan string   // Channel for logging
	BloomFilter                bool          // Enable Bloom filter for SSTables
	MaxCompactionConcurrency   int           // Maximum number of concurrent compactions
	CompactionCooldownPeriod   time.Duration // Cooldown period for compaction
	CompactionBatchSize        int           // Max number of SSTables to compact at once
	CompactionSizeRatio        float64       // Level size ratio that triggers compaction
	CompactionSizeThreshold    int           // Number of files to trigger size-tiered compaction
	CompactionScoreSizeWeight  float64       // Weight for size-based score
	CompactionScoreCountWeight float64       // Weight for count-based score
	FlusherTickerInterval      time.Duration // Interval for flusher ticker
	CompactorTickerInterval    time.Duration // Interval for compactor ticker
	BloomFilterFPR             float64       // False positive rate for Bloom filter
	WalAppendRetry             int           // Number of retries for WAL append
	WalAppendBackoff           time.Duration // Backoff duration for WAL append
}

// DB represents the main Wildcat structure
type DB struct {
	opts             *Options                 // Configuration options
	txns             atomic.Pointer[[]*Txn]   // Atomic pointer to the transactions
	levels           atomic.Pointer[[]*Level] // Atomic pointer to the levels
	lru              *lru.LRU                 // LRU cache for block managers
	flusher          *Flusher                 // Flusher for memtables
	compactor        *Compactor               // Compactor for SSTables
	memtable         atomic.Value             // The current memtable
	wg               *sync.WaitGroup          // WaitGroup for synchronization
	closeCh          chan struct{}            // Channel for closing up
	sstIdGenerator   *IDGenerator             // ID generator for SSTables
	walIdGenerator   *IDGenerator             // ID generator for WAL files
	txnIdGenerator   *IDGenerator             // ID generator for transactions
	txnTSGenerator   *IDGenerator             // Generator for transaction timestamps
	logChannel       chan string              // Log channel, instead of log file or standard output we log to a channel
	idgs             *IDGeneratorState        // ID generator state
	oldestActiveRead int64                    // Oldest active read timestamp
}

// IDGeneratorState represents the state of the ID generator.
// When system shuts down the state is saved to disk and restored on next startup.
type IDGeneratorState struct {
	lastSstID int64 // Last SSTable ID
	lastWalID int64 // Last WAL ID
	lastTxnID int64 // Last transaction ID
	db        *DB   // Pointer to the database instance
}

// Open initializes a new Wildcat instance with the provided options
func Open(opts *Options) (*DB, error) {
	// Options are required when opening a new instance of Wildcat.  Only a directory is required, the rest are automatically set.
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if opts.Directory == "" {
		return nil, errors.New("directory cannot be empty")
	}

	// Set default values for what options are not set
	opts.setDefaults()

	db := &DB{
		lru:     lru.New(int64(opts.BlockManagerLRUSize), opts.BlockManagerLRUEvictRatio, opts.BlockManagerLRUAccesWeight),
		wg:      &sync.WaitGroup{},
		opts:    opts,
		txns:    atomic.Pointer[[]*Txn]{},
		closeCh: make(chan struct{}),
		txnTSGenerator: newIDGeneratorWithTimestamp(),
	}

	db.idgs = &IDGeneratorState{
		lastSstID: 0,
		lastWalID: 0,
		lastTxnID: 0,
		db:        db,
	}

	db.flusher = newFlusher(db)
	db.compactor = newCompactor(db, db.opts.MaxCompactionConcurrency)

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

// setDefaults checks and sets default values for db options
func (opts *Options) setDefaults() {
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

	if opts.BlockSetSize <= 0 {
		opts.BlockSetSize = DefaultBlockSetSize
	}

	if opts.Permission == 0 {
		opts.Permission = DefaultPermission
	}

	if opts.BloomFilter {
		opts.BloomFilter = DefaultBloomFilter
	}

	if opts.MaxCompactionConcurrency <= 0 {
		opts.MaxCompactionConcurrency = DefaultMaxCompactionConcurrency
	}

	if opts.CompactionCooldownPeriod <= 0 {
		opts.CompactionCooldownPeriod = DefaultCompactionCooldownPeriod
	}

	if opts.CompactionBatchSize <= 0 {
		opts.CompactionBatchSize = DefaultCompactionBatchSize
	}

	if opts.CompactionSizeRatio <= 0 {
		opts.CompactionSizeRatio = DefaultCompactionSizeRatio
	}

	if opts.CompactionSizeThreshold <= 0 {
		opts.CompactionSizeThreshold = DefaultCompactionSizeThreshold
	}

	if opts.CompactionScoreSizeWeight <= 0 {
		opts.CompactionScoreSizeWeight = DefaultCompactionScoreSizeWeight
	}

	if opts.CompactionScoreCountWeight <= 0 {
		opts.CompactionScoreCountWeight = DefaultCompactionScoreCountWeight
	}

	if opts.FlusherTickerInterval <= 0 {
		opts.FlusherTickerInterval = DefaultFlusherTickerInterval
	}

	if opts.CompactorTickerInterval <= 0 {
		opts.CompactorTickerInterval = DefaultCompactorTickerInterval
	}

	if opts.BloomFilterFPR <= 0 {
		opts.BloomFilterFPR = DefaultBloomFilterProbability
	}

	if opts.WalAppendRetry <= 0 {
		opts.WalAppendRetry = DefaultWALAppendRetry
	}

	if opts.WalAppendBackoff <= 0 {
		opts.WalAppendBackoff = DefaultWALAppendBackoff
	}

	if opts.BlockManagerLRUSize <= 0 {
		opts.BlockManagerLRUSize = DefaultBlockManagerLRUSize
	}

	if opts.BlockManagerLRUEvictRatio <= 0 {
		opts.BlockManagerLRUEvictRatio = DefaultBlockManagerLRUEvictRatio
	}

	if opts.BlockManagerLRUAccesWeight <= 0 {
		opts.BlockManagerLRUAccesWeight = DefaultBlockManagerLRUAccessWeight
	}

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
		newWalPath := fmt.Sprintf("%s%d%s", db.opts.Directory, db.walIdGenerator.nextID(), WALFileExtension)

		db.memtable.Store(&Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: newWalPath,
			},
			db: db,
		})

		walBm, err := blockmanager.Open(newWalPath, os.O_RDWR|os.O_CREATE, db.opts.Permission, blockmanager.SyncOption(db.opts.SyncOption), db.opts.SyncInterval)
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add the WAL to the LRU cache
		db.lru.Put(newWalPath, walBm, func(key, value interface{}) {
			// Close the block manager when evicted from LRU
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})

		// Initialize empty transactions slice
		txns := make([]*Txn, 0)
		db.txns.Store(&txns)

		db.log(fmt.Sprintf("Created new memtable and WAL at %s", newWalPath))

		return nil // No WAL files, just return as we created a new memtable and WAL
	}

	// Global transaction map to track transactions across all WAL files
	// We'll organize this as a map of transaction ID to a slice of transaction entries
	// This helps preserve the order of operations within a transaction
	txnTimeline := make(map[int64][]*Txn)
	var totalEntries int

	// Process all WAL files in order (chronological)
	for i, walFile := range walFiles {
		walPath := fmt.Sprintf("%s%s", db.opts.Directory, walFile)
		db.log(fmt.Sprintf("Processing WAL %d of %d: %s", i+1, len(walFiles), walPath))

		// Open the WAL file
		walBm, err := blockmanager.Open(walPath,
			os.O_RDONLY,
			db.opts.Permission,
			blockmanager.SyncOption(db.opts.SyncOption), db.opts.SyncInterval)

		if err != nil {
			db.log(fmt.Sprintf("Warning: Failed to open WAL %s: %v - skipping", walPath, err))
			continue // Skip this WAL instead of failing completely
		}

		// Add WAL to LRU cache
		db.lru.Put(walPath, walBm, func(key, value interface{}) {
			// Close the block manager when evicted from LRU
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})

		// Process all transaction entries in this WAL
		iter := walBm.Iterator()
		var walEntries int

		for {
			data, _, err := iter.Next()
			if err != nil {
				break // End of WAL
			}

			walEntries++
			totalEntries++

			var txn Txn
			if err := txn.deserializeTransaction(data); err != nil {
				db.log(fmt.Sprintf("Warning: Failed to deserialize transaction in %s: %v - skipping entry",
					walPath, err))
				continue // Skip this entry
			}

			// Set database reference
			txn.db = db

			// Create a deep copy of the transaction
			txnCopy := &Txn{
				Id:        txn.Id,
				Timestamp: txn.Timestamp,
				WriteSet:  make(map[string][]byte),
				DeleteSet: make(map[string]bool),
				ReadSet:   make(map[string]int64),
				Committed: txn.Committed,
				db:        db,
			}

			// Copy the sets
			for k, v := range txn.WriteSet {
				txnCopy.WriteSet[k] = v
			}
			for k, v := range txn.DeleteSet {
				txnCopy.DeleteSet[k] = v
			}
			for k, v := range txn.ReadSet {
				txnCopy.ReadSet[k] = v
			}

			// Add to timeline in order
			txnTimeline[txn.Id] = append(txnTimeline[txn.Id], txnCopy)
		}

		db.log(fmt.Sprintf("Processed %d entries from WAL %s", walEntries, walPath))
	}

	// Now consolidate the transaction timeline into the final transaction state
	// Each operation should be applied in order, and the last operation for a key wins
	globalTxnMap := make(map[int64]*Txn)

	for txnID, txnEntries := range txnTimeline {
		// Create an empty final transaction state
		finalTxn := &Txn{
			Id:        txnID,
			WriteSet:  make(map[string][]byte),
			DeleteSet: make(map[string]bool),
			ReadSet:   make(map[string]int64),
			Committed: false,
			db:        db,
		}

		// Track the status of each key to determine the final state
		keyStates := make(map[string]string) // Map of key to its current state ("write", "delete", or "read")

		// Apply transactions in chronological order
		for _, entry := range txnEntries {
			// Update final transaction timestamp to the latest
			if entry.Timestamp > finalTxn.Timestamp {
				finalTxn.Timestamp = entry.Timestamp
			}

			// A commit flag on any entry means the transaction is committed
			if entry.Committed {
				finalTxn.Committed = true
			}

			// Process writes - newer operations override older ones
			for key, value := range entry.WriteSet {
				finalTxn.WriteSet[key] = value
				keyStates[key] = "write" // Mark as written
				// If it was previously deleted, remove it from delete set
				delete(finalTxn.DeleteSet, key)
			}

			// Process deletes - delete always wins over write
			for key := range entry.DeleteSet {
				delete(finalTxn.WriteSet, key) // Remove from write set
				finalTxn.DeleteSet[key] = true
				keyStates[key] = "delete" // Mark as deleted
			}

			// Process reads - always keep the latest read timestamp
			for key, timestamp := range entry.ReadSet {
				// Only update if this read is newer
				if ts, exists := finalTxn.ReadSet[key]; !exists || timestamp > ts {
					finalTxn.ReadSet[key] = timestamp
				}
				// Reads don't change the write/delete status
			}
		}

		// Clean up any empty sets
		if len(finalTxn.WriteSet) == 0 && len(finalTxn.DeleteSet) == 0 && len(finalTxn.ReadSet) == 0 && !finalTxn.Committed {
			// Skip this transaction as it has no actual impact
			continue
		}

		// Store the consolidated transaction
		globalTxnMap[txnID] = finalTxn
	}

	// Now create immutable memtables for all but the last WAL
	for i, walFile := range walFiles[:len(walFiles)-1] {
		walPath := fmt.Sprintf("%s%s", db.opts.Directory, walFile)

		immutableMemt := &Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: walPath,
			},
			db: db,
		}

		// For immutable memtables, we'll apply all transactions that were committed
		populateMemtableFromTxns(immutableMemt, globalTxnMap, true)

		// Enqueue the memtable for flushing
		db.flusher.enqueueMemtable(immutableMemt)
		db.log(fmt.Sprintf("Enqueued immutable memtable %d of %d from %s for flushing",
			i+1, len(walFiles)-1, walPath))
	}

	// Create the active memtable from the latest WAL
	activeWALPath := fmt.Sprintf("%s%s", db.opts.Directory, walFiles[len(walFiles)-1])
	activeMemt := &Memtable{
		skiplist: skiplist.New(),
		wal: &WAL{
			path: activeWALPath,
		},
		db: db,
	}

	// Open the active WAL for read/write
	activeWalBm, err := blockmanager.Open(activeWALPath,
		os.O_RDWR, // Open for read/write
		db.opts.Permission,
		blockmanager.SyncOption(db.opts.SyncOption), db.opts.SyncInterval)

	if err != nil {
		return fmt.Errorf("failed to reopen active WAL for writing: %w", err)
	}

	// Update or add to LRU cache
	db.lru.Put(activeWALPath, activeWalBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	// Populate the active memtable with all committed transactions
	populateMemtableFromTxns(activeMemt, globalTxnMap, false)

	// Store the active memtable
	db.memtable.Store(activeMemt)

	// Collect active (uncommitted) transactions
	activeTxns := make([]*Txn, 0)
	for _, txn := range globalTxnMap {
		if !txn.Committed &&
			(len(txn.WriteSet) > 0 || len(txn.DeleteSet) > 0 || len(txn.ReadSet) > 0) {
			// Add a deep copy to avoid mutation issues
			txnCopy := &Txn{
				Id:        txn.Id,
				Timestamp: txn.Timestamp,
				WriteSet:  make(map[string][]byte),
				DeleteSet: make(map[string]bool),
				ReadSet:   make(map[string]int64),
				Committed: txn.Committed,
				db:        db,
				mutex:     sync.Mutex{},
			}

			for k, v := range txn.WriteSet {
				txnCopy.WriteSet[k] = v
			}
			for k := range txn.DeleteSet {
				txnCopy.DeleteSet[k] = true
			}
			for k, v := range txn.ReadSet {
				txnCopy.ReadSet[k] = v
			}

			activeTxns = append(activeTxns, txnCopy)
		}
	}

	// Store active transactions
	db.txns.Store(&activeTxns)

	// Log summary statistics
	db.log(fmt.Sprintf("Reinstatement completed: processed %d total entries across %d WAL files",
		totalEntries, len(walFiles)))
	db.log(fmt.Sprintf("Recovered %d transactions total, %d committed, %d active",
		len(globalTxnMap), len(globalTxnMap)-len(activeTxns), len(activeTxns)))
	db.log(fmt.Sprintf("Active memtable size: %d bytes with %d entries",
		atomic.LoadInt64(&activeMemt.size), activeMemt.skiplist.Count(time.Now().UnixNano()+10000000000)))

	return nil
}

// Helper function to populate a memtable from a transaction map
func populateMemtableFromTxns(memt *Memtable, txnMap map[int64]*Txn, includeUncommitted bool) {
	var committedCount, uncommittedCount int
	var totalBytes int64

	// First, collect all keys that should be in the memtable and their final state
	keyFinalStates := make(map[string]struct {
		isDeleted bool
		value     []byte
		timestamp int64
		txnID     int64
	})

	// Process transactions in order of their timestamp
	type txnWithID struct {
		id  int64
		txn *Txn
	}

	txnSlice := make([]txnWithID, 0, len(txnMap))
	for id, txn := range txnMap {
		txnSlice = append(txnSlice, txnWithID{id, txn})
	}

	// Sort by timestamp
	sort.Slice(txnSlice, func(i, j int) bool {
		return txnSlice[i].txn.Timestamp < txnSlice[j].txn.Timestamp
	})

	for _, t := range txnSlice {
		txn := t.txn

		// Skip uncommitted transactions unless explicitly requested
		if !txn.Committed && !includeUncommitted {
			uncommittedCount++
			continue
		}

		committedCount++

		// Process all keys in this transaction
		// Writes first
		for key, value := range txn.WriteSet {
			// Only update if this key wasn't deleted by this transaction
			if !txn.DeleteSet[key] {
				keyFinalStates[key] = struct {
					isDeleted bool
					value     []byte
					timestamp int64
					txnID     int64
				}{
					isDeleted: false,
					value:     value,
					timestamp: txn.Timestamp,
					txnID:     txn.Id,
				}
			}
		}

		// Then deletes, which override writes
		for key := range txn.DeleteSet {
			keyFinalStates[key] = struct {
				isDeleted bool
				value     []byte
				timestamp int64
				txnID     int64
			}{
				isDeleted: true,
				value:     nil,
				timestamp: txn.Timestamp,
				txnID:     txn.Id,
			}
		}
	}

	// Now apply the final states to the memtable
	for key, state := range keyFinalStates {
		byteKey := []byte(key)

		if state.isDeleted {
			// Apply deletion
			memt.skiplist.Delete(byteKey, state.timestamp)
			// We don't subtract from totalBytes for deleted keys to keep calculations consistent
		} else {
			// Apply write
			memt.skiplist.Put(byteKey, state.value, state.timestamp)
			totalBytes += int64(len(key) + len(state.value))
		}
	}

	// Update the memtable size
	atomic.StoreInt64(&memt.size, totalBytes)

	memt.db.log(fmt.Sprintf("Populated memtable with %d keys from %d committed transactions, skipped %d uncommitted. Size: %d bytes",
		len(keyFinalStates), committedCount, uncommittedCount, totalBytes))
}

// log logs a message to the log channel
func (db *DB) log(msg string) {
	if db.opts.LogChannel != nil {
		db.opts.LogChannel <- msg
	}
}

// ForceFlush forces the flush of all memtables and immutable memtables
func (db *DB) ForceFlush() error {
	if db == nil {
		return errors.New("database is nil")
	}

	// Force flush all memtables
	err := db.flusher.flushMemtable(db.memtable.Load().(*Memtable))
	if err != nil {
		return fmt.Errorf("failed to force flush memtable: %w", err)
	}

	// Force flush all immutable memtables
	db.flusher.immutable.ForEach(func(item interface{}) bool {
		if memt, ok := item.(*Memtable); ok {
			err := db.flusher.flushMemtable(memt)
			if err != nil {
				db.log(fmt.Sprintf("Failed to flush immutable memtable: %v", err))
				return false // Stop iteration on error
			}
		}
		return true // Continue iteration
	})

	return nil
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
	defer func(idgsFile *os.File) {
		_ = idgsFile.Close()
	}(idgsFile)

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
	idgs.db.log(fmt.Sprintf("Saving ID generator state:\nLAST SST ID: %d\nLAST WAL ID: %d\nLAST TXN ID: %d", idgs.lastSstID, idgs.lastWalID, idgs.lastTxnID))

	// Open the ID generator state file
	idgsFilePath := fmt.Sprintf("%s%s", idgs.db.opts.Directory, IDGSTFileName)
	idgsFile, err := os.OpenFile(idgsFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, idgs.db.opts.Permission)
	if err != nil {
		return fmt.Errorf("failed to open ID generator state file: %w", err)
	}
	defer func(idgsFile *os.File) {
		_ = idgsFile.Close()
	}(idgsFile)

	// Write the ID generator state to the file
	if _, err = fmt.Fprintf(idgsFile, "%d %d %d", idgs.lastSstID, idgs.lastWalID, idgs.lastTxnID); err != nil {
		return fmt.Errorf("failed to write ID generator state: %w", err)
	}

	// Sync the file to ensure data is written
	if err = idgsFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync ID generator state file: %w", err)
	}

	return nil
}

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
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/buffer"
	"github.com/wildcatdb/wildcat/v2/lru"
	"github.com/wildcatdb/wildcat/v2/skiplist"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// SyncOption is a block manager sync option that can be set by the user.
type SyncOption int

const (
	SyncNone    SyncOption = iota // We don't sync
	SyncFull                      // We sync the entire block manager after each write
	SyncPartial                   // We sync based at intervals based on the SyncInterval option.. So every SyncInterval we sync the block manager.
)

// Prefixes, filenames, extensions constants
const (
	SSTablePrefix     = "sst_"     // Prefix for SSTable files
	LevelPrefix       = "l"        // Prefix for level directories i.e. "l1", "l2", etc.
	WALFileExtension  = ".wal"     // Extension for Write Ahead Log files <id>.wal
	KLogExtension     = ".klog"    // Extension for KLog files
	VLogExtension     = ".vlog"    // Extension for VLog files
	IDGSTFileName     = "idgstate" // Filename for ID generator state
	TempFileExtension = ".tmp"     // Temporary file extension for intermediate files
)

// Defaults
const (
	DefaultWriteBufferSize = 64 * 1024 * 1024     // Default write buffer size
	DefaultSyncOption      = SyncNone             // Default sync option for write operations
	DefaultSyncInterval    = 16 * time.Nanosecond // Default sync interval for write operations
	DefaultLevelCount      = 6                    // Default number of levels in the LSM tree
	DefaultLevelMultiplier = 10                   // Multiplier for the number of levels
	// 64MB -> 640MB -> 6.4GB ->  64GB -> 640GB ->  6.4TB
	DefaultBlockManagerLRUSize                 = 1024                 // Size of the LRU cache for block managers
	DefaultBlockManagerLRUEvictRatio           = 0.20                 // Eviction ratio for the LRU cache
	DefaultBlockManagerLRUAccessWeight         = 0.8                  // Access weight for the LRU cache
	DefaultPermission                          = 0750                 // Default permission for created files
	DefaultBloomFilter                         = false                // Default Bloom filter option
	DefaultMaxCompactionConcurrency            = 4                    // Default max compaction concurrency
	DefaultCompactionCooldownPeriod            = 5 * time.Second      // Default cooldown period for compaction
	DefaultCompactionBatchSize                 = 8                    // Default max number of SSTables to compact at once
	DefaultCompactionSizeRatio                 = 1.1                  // Default level size ratio that triggers compaction
	DefaultCompactionSizeThreshold             = 8                    // Default number of files to trigger size-tiered compaction
	DefaultCompactionScoreSizeWeight           = 0.8                  // Default weight for size-based score
	DefaultCompactionScoreCountWeight          = 0.2                  // Default weight for count-based score
	DefaultCompactionSizeTieredSimilarityRatio = 1.5                  // Default similarity ratio for size-tiered compaction
	DefaultCompactionActiveSSTReadWaitBackoff  = 8 * time.Microsecond // Backoff is used to avoid busy waiting when checking if sstables are safe to remove during compaction process final steps
	DefaultFlusherTickerInterval               = 1 * time.Millisecond
	DefaultCompactorTickerInterval             = 250 * time.Millisecond // Default interval for compactor ticker
	DefaultBloomFilterFPR                      = 0.01                   // Default false positive rate for Bloom filter
	DefaultWALAppendRetry                      = 10                     // Default number of retries for WAL append
	DefaultWALAppendBackoff                    = 128 * time.Microsecond // Default backoff duration for WAL append
	DefaultSSTableBTreeOrder                   = 10                     // Default order of the B-tree for SSTables
	DefaultMaxConcurrentTxns                   = 65536                  // Default max concurrent transactions
	DefaultTxnBeginRetry                       = 10                     // Default retries for Begin()
	DefaultTxnBeginBackoff                     = 1 * time.Microsecond   // Default initial backoff
	DefaultTxnBeginMaxBackoff                  = 100 * time.Millisecond // Default max backoff
)

// Options represents the configuration options for Wildcat
type Options struct {
	Directory                           string        // Directory for Wildcat
	WriteBufferSize                     int64         // Size of the write buffer
	SyncOption                          SyncOption    // Sync option for write operations
	SyncInterval                        time.Duration // Interval for syncing the write buffer
	LevelCount                          int           // Number of levels in the LSM tree
	LevelMultiplier                     int           // Multiplier for the number of levels
	BlockManagerLRUSize                 int           // Size of the LRU cache for block managers
	BlockManagerLRUEvictRatio           float64       // Eviction ratio for the LRU cache
	BlockManagerLRUAccesWeight          float64       // Access weight for the LRU cache
	Permission                          os.FileMode   // Permission for created files
	LogChannel                          chan string   // Channel for logging
	BloomFilter                         bool          // Enable Bloom filter for SSTables
	MaxCompactionConcurrency            int           // Maximum number of concurrent compactions
	CompactionCooldownPeriod            time.Duration // Cooldown period for compaction
	CompactionBatchSize                 int           // Max number of SSTables to compact at once
	CompactionSizeRatio                 float64       // Level size ratio that triggers compaction
	CompactionSizeThreshold             int           // Number of files to trigger size-tiered compaction
	CompactionScoreSizeWeight           float64       // Weight for size-based score
	CompactionScoreCountWeight          float64       // Weight for count-based score
	CompactionSizeTieredSimilarityRatio float64       // Similarity ratio for size-tiered compaction.  For grouping SSTables that are "roughly the same size" together for compaction.
	CompactionActiveSSTReadWaitBackoff  time.Duration // Backoff time for active SSTable read wait during compaction, to avoid busy waiting
	FlusherTickerInterval               time.Duration // Interval for flusher ticker
	CompactorTickerInterval             time.Duration // Interval for compactor ticker
	BloomFilterFPR                      float64       // False positive rate for Bloom filter
	WalAppendRetry                      int           // Number of retries for WAL append
	WalAppendBackoff                    time.Duration // Backoff duration for WAL append
	SSTableBTreeOrder                   int           // Order of the B-tree for SSTables
	STDOutLogging                       bool          // Enable logging to standard output (default is false and if set, channel is ignored)
	MaxConcurrentTxns                   int           // Maximum concurrent transactions (buffer size)
	TxnBeginRetry                       int           // Number of retries for Begin() when buffer full
	TxnBeginBackoff                     time.Duration // Initial backoff duration for Begin() retries
	TxnBeginMaxBackoff                  time.Duration // Maximum backoff duration for Begin() retries
	RecoverUncommittedTxns              bool          // Whether to recover uncommitted transactions on startup
}

// DB represents the main Wildcat structure
type DB struct {
	opts           *Options                 // Configuration options for db instance
	txnBuffer      *buffer.Buffer           // Atomic buffer for transactions
	levels         atomic.Pointer[[]*Level] // Atomic pointer to the levels
	lru            *lru.LRU                 // LRU cache for block managers
	flusher        *Flusher                 // Flusher queues memtables and flushes them to disk to level 1
	compactor      *Compactor               // Compactor for compacting levels
	memtable       atomic.Value             // The current memtable
	wg             *sync.WaitGroup          // WaitGroup for synchronization
	closeCh        chan struct{}            // Channel for closing up
	sstIdGenerator *IDGenerator             // ID generator for SSTables
	walIdGenerator *IDGenerator             // ID generator for WAL files
	txnTSGenerator *IDGenerator             // Generator for transaction timestamps
	logChannel     chan string              // Log channel, instead of log file or standard output we log to a channel
	idgs           *IDGeneratorState        // ID state for sstable, wal, and txn timestamps
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
	// Options are required when opening a new Wildcat db instance.
	// Only a directory is required, a user can choose what options they want to set; The system set's defaults to what
	// options are not set.
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if opts.Directory == "" {
		return nil, errors.New("directory cannot be empty")
	}

	// Set default values for what options are not set
	opts.setDefaults()

	buff, err := buffer.New(opts.MaxConcurrentTxns)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction buffer: %w", err)
	}

	// We create a db instance with the provided options
	db := &DB{
		lru:            lru.New(int64(opts.BlockManagerLRUSize), opts.BlockManagerLRUEvictRatio, opts.BlockManagerLRUAccesWeight), // New block manager LRU cache
		wg:             &sync.WaitGroup{},                                                                                         // Wait group for background operations
		opts:           opts,                                                                                                      // Set the options
		txnBuffer:      buff,                                                                                                      // Create a new buffer for transactions with the specified cap
		closeCh:        make(chan struct{}),                                                                                       // Channel for closing the database
		txnTSGenerator: newIDGeneratorWithTimestamp(),                                                                             // We use timestamp generator for monotonic generation (1579134612000000004, next ID will be 1579134612000000005)
	}

	// Initialize flusher and compactor
	db.flusher = newFlusher(db)
	db.compactor = newCompactor(db)

	// Check if the DB path does not end with i.e an /
	// We add one if not
	if !strings.HasSuffix(db.opts.Directory, string(os.PathSeparator)) {
		db.opts.Directory += string(os.PathSeparator)
	}

	// We check if the directory exists, if not we create it
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

		db.walIdGenerator = newIDGenerator()
		db.sstIdGenerator = newIDGenerator()
	} else {
		db.idgs = &IDGeneratorState{
			lastSstID: 0,
			lastWalID: 0,
			lastTxnID: 0,
			db:        db,
		}

		// Directory exists, load persisted ID generator state
		if err := db.idgs.loadState(); err != nil {
			return nil, fmt.Errorf("failed to load ID generator state: %w", err)
		}

	}

	// Initialize the levels array
	levels := make([]*Level, db.opts.LevelCount)
	for i := 0; i < db.opts.LevelCount; i++ {
		level := &Level{
			id:       i + 1, // We start at level 1
			capacity: int(float64(db.opts.WriteBufferSize) * math.Pow(float64(db.opts.LevelMultiplier), float64(i))),
			db:       db,
		}

		level.sstables = atomic.Pointer[[]*SSTable]{}                                                       // Atomic pointer to SSTables in this level
		level.path = fmt.Sprintf("%s%s%d%s", db.opts.Directory, LevelPrefix, i+1, string(os.PathSeparator)) // Path for the level directory

		// Set level
		levels[i] = level

		db.log(fmt.Sprintf("Creating level %d with capacity %d bytes at path %s", level.id, level.capacity, level.path))

		// Create or ensure the level directory exists
		if err := os.MkdirAll(level.path, db.opts.Permission); err != nil {
			return nil, fmt.Errorf("failed to create level directory: %v", err)
		}

		// Reopen the level to load existing SSTables
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
		opts.BloomFilterFPR = DefaultBloomFilterFPR
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

	if opts.SSTableBTreeOrder <= 0 {
		opts.SSTableBTreeOrder = DefaultSSTableBTreeOrder
	}

	if opts.CompactionSizeTieredSimilarityRatio <= 0 {
		opts.CompactionSizeTieredSimilarityRatio = DefaultCompactionSizeTieredSimilarityRatio
	}

	if opts.TxnBeginMaxBackoff <= 0 {
		opts.TxnBeginMaxBackoff = DefaultTxnBeginMaxBackoff
	}

	if opts.TxnBeginRetry <= 0 {
		opts.TxnBeginRetry = DefaultTxnBeginRetry
	}

	if opts.TxnBeginBackoff <= 0 {
		opts.TxnBeginBackoff = DefaultTxnBeginBackoff
	}

	if opts.MaxConcurrentTxns == 0 {
		opts.MaxConcurrentTxns = DefaultMaxConcurrentTxns
	}

	if opts.CompactionActiveSSTReadWaitBackoff <= 0 {
		opts.CompactionActiveSSTReadWaitBackoff = DefaultCompactionActiveSSTReadWaitBackoff
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

	// Because we can have many immutable WAL files we will read all WAL files in the directory
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

	// If no WAL files found, we create a new memtable and WAL
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

		db.log(fmt.Sprintf("Created new memtable and WAL at %s", newWalPath))

		return nil // No WAL files, just return as we created a new memtable and WAL
	}

	db.log(fmt.Sprintf("Found %d WAL files, processing them...", len(walFiles)))

	// Global transaction map to track transactions across all WAL files
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
				// If it was previously deleted, remove it from delete set
				delete(finalTxn.DeleteSet, key)
			}

			// Process deletes - delete always wins over write
			for key := range entry.DeleteSet {
				delete(finalTxn.WriteSet, key) // Remove from write set
				finalTxn.DeleteSet[key] = true
			}

			// Process reads - always keep the latest read timestamp
			for key, timestamp := range entry.ReadSet {
				// Only update if this read is newer
				if ts, exists := finalTxn.ReadSet[key]; !exists || timestamp > ts {
					finalTxn.ReadSet[key] = timestamp
				}
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

	// Create immutable memtables for all but the last WAL
	// and include committed transactions in them
	for i, walFile := range walFiles[:len(walFiles)-1] {
		walPath := fmt.Sprintf("%s%s", db.opts.Directory, walFile)

		immutableMemt := &Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: walPath,
			},
			db: db,
		}

		// For immutable memtables, include committed transactions
		// This ensures that data is available for reading even before flushing completes
		populateMemtableFromTxns(immutableMemt, globalTxnMap, db.opts.RecoverUncommittedTxns)

		// Enqueue the memtable for flushing
		db.flusher.immutable.Enqueue(immutableMemt)
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
	populateMemtableFromTxns(activeMemt, globalTxnMap, db.opts.RecoverUncommittedTxns)

	// Store the active memtable
	db.memtable.Store(activeMemt)

	// Collect active (uncommitted) transactions and add to buffer
	var activeCount int64
	for _, txn := range globalTxnMap {
		if !txn.Committed &&
			(len(txn.WriteSet) > 0 || len(txn.DeleteSet) > 0 || len(txn.ReadSet) > 0) {

			// Create a deep copy to avoid mutation issues
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

			// Copy the sets
			for k, v := range txn.WriteSet {
				txnCopy.WriteSet[k] = v
			}
			for k := range txn.DeleteSet {
				txnCopy.DeleteSet[k] = true
			}
			for k, v := range txn.ReadSet {
				txnCopy.ReadSet[k] = v
			}

			txnCopy.Id, err = db.txnBuffer.Add(txnCopy)
			if err != nil {
				db.log(fmt.Sprintf("Warning: Failed to add transaction %d to buffer during recovery - buffer full", txn.Id))
			} else {
				activeCount++
			}
		}
	}

	committedCount := int64(len(globalTxnMap)) - activeCount
	db.log(fmt.Sprintf("Reinstatement completed: processed %d total entries across %d WAL files",
		totalEntries, len(walFiles)))
	db.log(fmt.Sprintf("Recovered %d transactions total, %d committed, %d active",
		len(globalTxnMap), committedCount, activeCount))
	db.log(fmt.Sprintf("Active memtable size: %d bytes with %d entries",
		atomic.LoadInt64(&activeMemt.size), activeMemt.skiplist.Count(time.Now().UnixNano()+10000000000)))

	return nil
}

// populateMemtableFromTxns helper function to populate a memtable from a transaction map
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

	// Sort by timestamp to ensure proper ordering
	sort.Slice(txnSlice, func(i, j int) bool {
		return txnSlice[i].txn.Timestamp < txnSlice[j].txn.Timestamp
	})

	for _, t := range txnSlice {
		txn := t.txn

		// For immutable memtables, we should include ALL committed transactions
		// For active memtables, we include committed transactions but not uncommitted ones
		shouldInclude := false
		if txn.Committed {
			shouldInclude = true
			committedCount++
		} else if includeUncommitted {
			shouldInclude = true
			uncommittedCount++
		} else {
			uncommittedCount++
		}

		if !shouldInclude {
			continue
		}

		// Process all keys in this transaction
		// Writes first
		for key, value := range txn.WriteSet {

			// Only update if this key wasn't deleted by this same transaction
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
	if db.opts.LogChannel != nil && !db.opts.STDOutLogging {
		select {
		case db.opts.LogChannel <- msg:
		default:
			// Channel full or closed, skip logging..
		}
	}

	if db.opts.STDOutLogging {
		fmt.Println(msg) // Print to stdout if STDOutLogging is enabled, is concurrent safe
		return
	}
}

// ForceFlush forces the flush of main current memtable and immutable memtables in flusher queue
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

// totalEntries returns the total number of entries in the database,
func (db *DB) totalEntries() int64 {
	if db == nil {
		return 0
	}

	total := int64(0)

	// Count entries in the active memtable
	if activeMemt, ok := db.memtable.Load().(*Memtable); ok {
		total += int64(activeMemt.skiplist.Count(time.Now().UnixNano() + 10000000000))
	}

	// We need to check if we have a flushing memtable
	fmem := db.flusher.flushing.Load()
	if fmem != nil {
		total += int64(fmem.skiplist.Count(time.Now().UnixNano() + 10000000000))
	}

	// Count entries in immutable memtables
	db.flusher.immutable.ForEach(func(item interface{}) bool {
		if memt, ok := item.(*Memtable); ok {
			total += int64(memt.skiplist.Count(time.Now().UnixNano() + 10000000000))
		}
		return true // Continue iteration
	})

	for _, level := range *db.levels.Load() {
		if level != nil {
			for _, sstable := range level.SSTables() {
				total += int64(sstable.EntryCount)
			}
		}
	}

	return total
}

// Sync escalates the Fdatasync operation for the current memtable's write ahead log.  To only be used when sync option is set to SyncNone.
func (db *DB) Sync() error {
	if db == nil {
		return errors.New("database is nil")
	}

	if db.opts.SyncOption != SyncNone {
		return errors.New("sync option must be set to SyncNone to escalate syncs")
	}

	// Sync current memtable's WAL
	if memt, ok := db.memtable.Load().(*Memtable); ok && memt.wal != nil {
		// Get block manager for WAL from lru
		bm, ok := db.lru.Get(memt.wal.path)
		if !ok {
			return fmt.Errorf("no block manager found for WAL at path %s", memt.wal.path)
		}

		if walBm, ok := bm.(*blockmanager.BlockManager); ok {

			// Sync it up!!
			if err := walBm.Sync(); err != nil {
				return fmt.Errorf("failed to sync active memtable's WAL: %w", err)
			}
		} else {
			return fmt.Errorf("expected BlockManager for WAL, got %T", bm)

		}
	}

	return nil
}

// Stats returns a string with the current statistics of the Wildcat database
func (db *DB) Stats() string {
	if db == nil {
		return "┌─────────────────────────────┐\n│   ⚠ Database is nil   │\n└─────────────────────────────┘"
	}

	type Section struct {
		Title  string
		Labels []string
		Values []any
	}

	// Define sections
	sections := []Section{
		{
			Title: "Wildcat DB Stats and Configuration",
			Labels: []string{
				"Write Buffer Size", "Sync Option", "Level Count", "Bloom Filter Enabled",
				"Max Compaction Concurrency", "Compaction Cooldown", "Compaction Batch Size",
				"Compaction Size Ratio", "Compaction Threshold", "Score Size Weight",
				"Score Count Weight", "Flusher Interval", "Compactor Interval", "Bloom FPR",
				"WAL Retry", "WAL Backoff", "SSTable B-Tree Order", "LRU Size",
				"LRU Evict Ratio", "LRU Access Weight",
				"File Version",
				"Magic Number",
				"Directory",
			},
			Values: []any{
				db.opts.WriteBufferSize, db.opts.SyncOption, db.opts.LevelCount, db.opts.BloomFilter,
				db.opts.MaxCompactionConcurrency, db.opts.CompactionCooldownPeriod, db.opts.CompactionBatchSize,
				db.opts.CompactionSizeRatio, db.opts.CompactionSizeThreshold, db.opts.CompactionScoreSizeWeight,
				db.opts.CompactionScoreCountWeight, db.opts.FlusherTickerInterval, db.opts.CompactorTickerInterval,
				db.opts.BloomFilterFPR, db.opts.WalAppendRetry, db.opts.WalAppendBackoff,
				db.opts.SSTableBTreeOrder, db.opts.BlockManagerLRUSize, db.opts.BlockManagerLRUEvictRatio,
				db.opts.BlockManagerLRUAccesWeight,
				blockmanager.Version,
				blockmanager.MagicNumber,
				db.opts.Directory,
			},
		},
		{
			Title:  "ID Generator State",
			Labels: []string{"Last SST ID", "Last WAL ID"},
			Values: []any{db.sstIdGenerator.last(), db.walIdGenerator.last()},
		},
		{
			Title: "Runtime Statistics",
			Labels: []string{
				"Active Memtable Size", "Active Memtable Entries", "Active Transactions",
				"WAL Files", "Total SSTables",
				"Total Entries",
			},
			Values: []any{
				atomic.LoadInt64(&db.memtable.Load().(*Memtable).size),
				db.memtable.Load().(*Memtable).skiplist.Count(time.Now().UnixNano() + 10000000000),
				db.txnBuffer.Count(), len(db.flusher.immutable.List()), func() int {
					sstables := 0
					levels := db.levels.Load()
					if levels != nil {
						for _, level := range *levels {
							sstables += len(level.SSTables())
						}
					}
					return sstables
				}(),
				db.totalEntries(),
			},
		},
	}

	// Calculate maximum widths for proper alignment
	maxLabelWidth, maxValueWidth := 0, 0
	maxTitleWidth := 0

	for _, section := range sections {
		// Check title width
		if len(section.Title) > maxTitleWidth {
			maxTitleWidth = len(section.Title)
		}

		// Check label and value widths
		for i, label := range section.Labels {
			if len(label) > maxLabelWidth {
				maxLabelWidth = len(label)
			}
			valueStr := fmt.Sprintf("%v", section.Values[i])
			// Use utf8.RuneCountInString for proper Unicode character counting
			valueRuneCount := utf8.RuneCountInString(valueStr)
			if valueRuneCount > maxValueWidth {
				maxValueWidth = valueRuneCount
			}
		}
	}

	// Calculate total content width: label + " : " + value
	contentWidth := maxLabelWidth + 3 + maxValueWidth

	// Ensure title fits
	if maxTitleWidth > contentWidth {
		contentWidth = maxTitleWidth
	}

	// Total table width: borders (2) + padding (2) + content
	tableWidth := contentWidth + 4

	// Create border elements
	top := "┌" + strings.Repeat("─", tableWidth-2) + "┐"
	divider := "├" + strings.Repeat("─", tableWidth-2) + "┤"
	bottom := "└" + strings.Repeat("─", tableWidth-2) + "┘"

	// Helper function to write data rows
	writeRow := func(b *strings.Builder, label string, value any) {
		valueStr := fmt.Sprintf("%v", value)
		// Use utf8.RuneCountInString for proper Unicode character counting
		valueRuneCount := utf8.RuneCountInString(valueStr)
		padding := contentWidth - maxLabelWidth - 3 - valueRuneCount
		b.WriteString(fmt.Sprintf("│ %-*s : %s%s │\n",
			maxLabelWidth, label, valueStr, strings.Repeat(" ", padding)))
	}

	// Helper function to write title rows
	writeTitle := func(b *strings.Builder, title string) {
		padding := contentWidth - len(title)
		b.WriteString(fmt.Sprintf("│ %s%s │\n", title, strings.Repeat(" ", padding)))
	}

	// Build the table
	var b strings.Builder
	b.WriteString(top + "\n")

	for i, section := range sections {
		if i > 0 {
			b.WriteString(divider + "\n")
		}

		writeTitle(&b, section.Title)
		b.WriteString(divider + "\n")

		for j, label := range section.Labels {
			writeRow(&b, label, section.Values[j])
		}
	}

	b.WriteString(bottom)
	return b.String()
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

	idgs.db.walIdGenerator = reloadIDGenerator(idgs.lastWalID)
	idgs.db.sstIdGenerator = reloadIDGenerator(idgs.lastSstID)
	idgs.db.txnTSGenerator = reloadIDGenerator(idgs.lastTxnID)

	idgs.db.log(fmt.Sprintf("Loaded ID generator state: %d %d %d", idgs.lastSstID, idgs.lastWalID, idgs.lastTxnID))

	return nil
}

// saveState saves the current state of the ID generator to a file
func (idgs *IDGeneratorState) saveState() error {
	if idgs == nil {
		return errors.New("IDGeneratorState is nil")
	}

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

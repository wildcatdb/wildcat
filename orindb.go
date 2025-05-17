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
	"container/heap"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"orindb/blockmanager" // atomic block manager, non blocking
	"orindb/lru"          // atomic lru
	"orindb/queue"        // atomic queue
	"orindb/skiplist"     // Atomic mvcc skip list
	"os"
	"sort"
	"strconv"
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
)

type Options struct {
	Directory           string
	WriteBufferSize     int64
	SyncOption          SyncOption
	SyncInterval        time.Duration
	LevelCount          int
	LevelMultiplier     int
	BlockManagerLRUSize int   // Size of the LRU cache for block managers
	BlockSetSize        int64 // Size of the block set
}

type WAL struct {
	path string // The WAL path
}

type Memtable struct {
	skiplist *skiplist.SkipList // The skiplist for the memtable
	wal      *WAL               // The write-ahead log for durability.
	Size     int64              // atomic size of the memtable
}

type Level struct {
	id          int                        // The level ID
	path        string                     // The path to the level directory
	sstables    atomic.Pointer[[]*SSTable] // Atomic pointer to the list of SSTables
	capacity    int                        // The capacity of the level
	currentSize int64                      // atomic size of the level
}

// SSTable represents a sorted string table
type SSTable struct {
	Id         int64  // SStable ID
	Min        []byte // The minimum key in the SSTable
	Max        []byte // The maximum key in the SSTable
	isMerging  int32  // Atomic flag indicating if the SSTable is being merged
	Size       int64  // The size of the SSTable in bytes
	EntryCount int    // The number of entries in the SSTable
	db         *DB    // Reference to the database
	Level      int    // The level of the SSTable
}

type SSTableIDGenerator struct {
	mu     sync.Mutex
	lastID int64
}

type DB struct {
	opts        *Options                 // Configuration options
	lru         *lru.LRU                 // Atomic LRU used for block managers.
	immutable   *queue.Queue             // Atomic queue for immutable memtables.
	memtable    atomic.Value             // The current memtable
	flushLock   *sync.Mutex              // Mutex for flushing the memtable (mainly swapping)
	levels      atomic.Pointer[[]*Level] // Atomic pointer to the levels
	wg          *sync.WaitGroup          // WaitGroup for synchronization
	txns        atomic.Pointer[[]*Txn]   // Atomic pointer to the transactions
	closeCh     chan struct{}            // Channel for closing the database
	idGenerator *SSTableIDGenerator      // ID generator for SSTables
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

// Txn represents a transaction in the database
type Txn struct {
	id        string
	db        *DB
	ReadSet   map[string]int64
	WriteSet  map[string][]byte
	DeleteSet map[string]bool
	Timestamp int64
	mutex     sync.Mutex
	Committed bool
}

// CompactionJob represents a scheduled compaction
type CompactionJob struct {
	Level       int
	Priority    float64
	SSTables    []*SSTable
	TargetLevel int
	InProgress  bool
}

// Compactor manages database compactions
type Compactor struct {
	db              *DB
	compactionQueue []*CompactionJob
	activeJobs      int32
	maxConcurrency  int
	lastCompaction  time.Time
	scoreLock       sync.Mutex
}

// Open opens or creates a new database at the specified directory
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

	db := &DB{
		lru:         lru.New(int64(opts.BlockManagerLRUSize), 0.25, 0.7),
		immutable:   queue.New(),
		wg:          &sync.WaitGroup{},
		opts:        opts,
		txns:        atomic.Pointer[[]*Txn]{},
		flushLock:   &sync.Mutex{},
		closeCh:     make(chan struct{}),
		idGenerator: newSSTableIDGenerator(),
	}

	if !strings.HasSuffix(db.opts.Directory, string(os.PathSeparator)) {
		db.opts.Directory += string(os.PathSeparator)
	}

	if _, err := os.Stat(db.opts.Directory); os.IsNotExist(err) {
		// Create the directory if it does not exist
		if err := os.MkdirAll(db.opts.Directory, 0755); err != nil {
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
		level.path = fmt.Sprintf("%s%s%s%d%s", db.opts.Directory, string(os.PathSeparator), LevelPrefix, i+1, string(os.PathSeparator))

		levels[i] = level

		// Create or ensure the level directory exists

		if err := os.MkdirAll(level.path, 0755); err != nil {
			return nil, fmt.Errorf("failed to create level directory: %v", err)
		}

		log.Println("Level directory created:", level.path, "with capacity:", level.capacity)
	}

	// Set the levels in the LSM tree
	db.levels.Store(&levels)

	if err := db.openWALs(); err != nil {
		return nil, fmt.Errorf("failed to open WALs: %w", err)
	}

	// Start the background flusher
	db.wg.Add(1)
	go db.backgroundFlusher()

	// Start the compaction manager
	db.wg.Add(1)
	go db.backgroundCompactor()

	return db, nil

}

func (db *DB) openWALs() error {
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
		// No WAL files found, create a new memtable and WAL
		log.Println("No WAL files found, creating new memtable and WAL")

		db.memtable.Store(&Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: fmt.Sprintf("%s%s%d%s", db.opts.Directory, string(os.PathSeparator), time.Now().UnixNano(), WALFileExtension),
			},
		})

		walBm, err := blockmanager.Open(db.memtable.Load().(*Memtable).wal.path, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add the WAL to the LRU cache
		db.lru.Put(db.memtable.Load().(*Memtable).wal.path, walBm)

		// Initialize empty transactions slice
		txns := make([]*Txn, 0)
		db.txns.Store(&txns)

		log.Println("Created new WAL:", db.memtable.Load().(*Memtable).wal.path)

		return nil
	}

	// Initialize the transactions map
	allTxns := make([]*Txn, 0)

	// Process all but the latest WAL file as immutable memtables
	for _, walFile := range walFiles[:len(walFiles)-1] {
		walPath := fmt.Sprintf("%s%s", db.opts.Directory, walFile)
		log.Println("Processing immutable WAL:", walPath)

		// Create a memtable for this WAL
		immutableMemt := &Memtable{
			skiplist: skiplist.New(),
			wal: &WAL{
				path: walPath,
			},
		}

		// Open the WAL file
		walBm, err := blockmanager.Open(walPath, os.O_RDONLY, 0666, blockmanager.SyncOption(db.opts.SyncOption))
		if err != nil {
			return fmt.Errorf("failed to open WAL block manager: %w", err)
		}

		// Add WAL to LRU cache
		db.lru.Put(walPath, walBm)

		// Replay transactions from this WAL to the memtable
		// We don't need to track transactions from immutable WALs as they should all be committed
		err = db.replayWAL(walBm, immutableMemt, nil)
		if err != nil {
			return fmt.Errorf("failed to replay WAL: %w", err)
		}

		db.immutable.Enqueue(immutableMemt)

		log.Printf("Added immutable memtable from WAL %s to stack (size: %d)\n", walFile, immutableMemt.Size)
	}

	// Open the latest WAL file as the active WAL
	activeWAL := walFiles[len(walFiles)-1]
	activeWALPath := fmt.Sprintf("%s%s", db.opts.Directory, activeWAL)
	log.Println("Processing active WAL:", activeWALPath)

	db.memtable.Store(&Memtable{
		skiplist: skiplist.New(),
		wal: &WAL{
			path: activeWALPath,
		},
	})

	// Open the active WAL
	walBm, err := blockmanager.Open(activeWALPath, os.O_RDWR, 0666, blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open active WAL block manager: %w", err)
	}

	// Add the WAL to the LRU cache
	db.lru.Put(activeWALPath, walBm)

	// For the active WAL, we need to track transactions that are not yet committed
	activeTxns := make([]*Txn, 0)

	// Replay transactions from active WAL to the memtable and collect active transactions
	err = db.replayWAL(walBm, db.memtable.Load().(*Memtable), &activeTxns)
	if err != nil {
		return fmt.Errorf("failed to replay active WAL: %w", err)
	}

	// Update the transaction state in the database
	allTxns = append(allTxns, activeTxns...)
	db.txns.Store(&allTxns)

	log.Printf("Reopened active WAL and memtable: %s (size: %d) with %d active transactions\n",
		activeWAL, db.memtable.Load().(*Memtable).Size, len(activeTxns))

	return nil
}

// replayWAL reads all transactions from a WAL and applies them to the given memtable
// If activeTxns is not nil, it collects transactions that are still active
func (db *DB) replayWAL(walBm *blockmanager.BlockManager, memtable *Memtable, activeTxns *[]*Txn) error {
	iter := walBm.Iterator()
	var memtSize int64

	// Track the latest state of each transaction by ID
	txnMap := make(map[string]*Txn)

	for {
		data, _, err := iter.Next()
		if err != nil {
			// End of WAL
			break
		}

		// Deserialize the transaction
		var txn Txn
		err = txn.deserializeTransaction(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize transaction: %w", err)
		}

		// Set the database reference for the transaction
		txn.db = db

		// Update our transaction map with the latest state of this transaction
		txnMap[txn.id] = &txn

		// Only apply committed transactions to the memtable
		if txn.Committed {
			// Apply writes to the memtable
			for key, value := range txn.WriteSet {
				memtable.skiplist.Put([]byte(key), value, txn.Timestamp)
				memtSize += int64(len(key) + len(value))
			}

			// Apply deletes to the memtable
			for key := range txn.DeleteSet {
				memtable.skiplist.Delete([]byte(key), txn.Timestamp)
				memtSize -= int64(len(key))
			}
		}
	}

	// Update the memtable size
	atomic.StoreInt64(&memtable.Size, memtSize)

	// If we're tracking active transactions, collect them now
	if activeTxns != nil {
		for _, txn := range txnMap {
			// Only include uncommitted transactions that haven't been rolled back
			if !txn.Committed && len(txn.WriteSet) > 0 || len(txn.DeleteSet) > 0 || len(txn.ReadSet) > 0 {
				*activeTxns = append(*activeTxns, txn)
			}
		}
	}

	return nil
}

// Close closes the database and all open block managers
func (db *DB) Close() error {
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
			if err := bm.Close(); err != nil {
				log.Printf("Failed to close block manager %s: %v", key, err)
			}
		}
		return true
	})

	return nil
}

// appendWal appends state of transaction to a WAL
func (db *DB) appendWal(txn *Txn) error {
	// serialize the transaction
	data, err := txn.serializeTransaction()
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}

	wal, ok := db.lru.Get(db.memtable.Load().(*Memtable).wal.path)
	if !ok {
		return fmt.Errorf("failed to get WAL from LRU cache")
	}

	// Append the serialized transaction to the WAL
	_, err = wal.(*blockmanager.BlockManager).Append(data)
	if err != nil {
		return err
	}

	return nil
}

// GetTxn retrieves a transaction by ID
func (db *DB) GetTxn(id string) (*Txn, error) {
	// Find the transaction by ID
	txns := db.txns.Load()
	if txns == nil {
		return nil, fmt.Errorf("transaction not found")
	}

	for _, txn := range *txns {
		if txn.id == id {
			return txn, nil
		}
	}

	return nil, fmt.Errorf("transaction not found")
}

// remove removes the transaction from the database
func (txn *Txn) remove() {

	// Clear all sets
	txn.ReadSet = make(map[string]int64)
	txn.WriteSet = make(map[string][]byte)
	txn.DeleteSet = make(map[string]bool)

	txn.Committed = false

	// Remove from the transaction list
	txns := txn.db.txns.Load()
	if txns != nil {
		for i, t := range *txns {
			if t.id == txn.id {
				*txns = append((*txns)[:i], (*txns)[i+1:]...)
				break
			}
		}
		txn.db.txns.Store(txns)

	}
}

func (db *DB) Begin() *Txn {
	txn := &Txn{
		id:        uuid.New().String(),
		db:        db,
		ReadSet:   make(map[string]int64),
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		Timestamp: time.Now().UnixNano(),
		Committed: false,
		mutex:     sync.Mutex{},
	}

	// Add the transaction to the list of transactions, do a swap to make it atomic
	txnList := db.txns.Load()
	if txnList == nil {
		txns := make([]*Txn, 0)

		txns = append(txns, txn)
		db.txns.Store(&txns)
	}

	return txn

}

func (db *DB) stackMemtable() error {
	// Create a new memtable
	newMemtable := &Memtable{
		skiplist: skiplist.New(),
		wal: &WAL{
			path: fmt.Sprintf("%s%s%d%s", db.opts.Directory, string(os.PathSeparator), time.Now().UnixNano(), WALFileExtension),
		}}
	// Open the new WAL

	walBm, err := blockmanager.Open(newMemtable.wal.path, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open WAL block manager: %w", err)
	}

	// Add the new WAL to the LRU cache
	db.lru.Put(newMemtable.wal.path, walBm)

	// Push the current memtable to the immutable stack
	db.immutable.Enqueue(db.memtable.Load().(*Memtable))

	// Reset the current memtable size
	atomic.StoreInt64(&db.memtable.Load().(*Memtable).Size, 0)

	// Update the current memtable to the new one
	db.memtable.Store(newMemtable)
	return nil
}

func (txn *Txn) Put(key []byte, value []byte) error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add to write set
	txn.WriteSet[string(key)] = value
	delete(txn.DeleteSet, string(key)) // Remove from delete set if exists

	// We append to the WAL here
	err := txn.db.appendWal(txn)
	if err == nil {
		return err
	}

	return nil
}

func (txn *Txn) Delete(key []byte) error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add to delete set
	txn.DeleteSet[string(key)] = true
	delete(txn.WriteSet, string(key)) // Remove from write set if exists

	// We append to the WAL here
	err := txn.db.appendWal(txn)
	if err == nil {
		return err
	}

	return nil
}

// Commit commits the transaction
func (txn *Txn) Commit() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	if txn.Committed {
		return nil // Already committed
	}

	// Apply writes
	for key, value := range txn.WriteSet {
		txn.db.memtable.Load().(*Memtable).skiplist.Put([]byte(key), value, txn.Timestamp)
		// Increment the size of the memtable
		atomic.AddInt64(&txn.db.memtable.Load().(*Memtable).Size, int64(len(key)+len(value)))
	}

	// Apply deletes
	for key := range txn.DeleteSet {
		txn.db.memtable.Load().(*Memtable).skiplist.Delete([]byte(key), txn.Timestamp)
		// Decrement the size of the memtable
		atomic.AddInt64(&txn.db.memtable.Load().(*Memtable).Size, -int64(len(key)))
	}

	txn.Committed = true

	// We append to the WAL here
	err := txn.db.appendWal(txn)
	if err != nil {
		return err
	}

	// Check if we need to flush the memtable to stack
	if atomic.LoadInt64(&txn.db.memtable.Load().(*Memtable).Size) > txn.db.opts.WriteBufferSize {
		err = txn.db.stackMemtable()
		if err != nil {
			return fmt.Errorf("failed to stack memtable: %w", err)
		}
	}

	return nil
}

func (txn *Txn) Rollback() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()
	defer txn.remove()

	// Clear all pending changes
	txn.WriteSet = make(map[string][]byte)
	txn.DeleteSet = make(map[string]bool)
	txn.ReadSet = make(map[string]int64)

	txn.Committed = false

	// We append to the WAL here
	err := txn.db.appendWal(txn)
	if err == nil {
		return err
	}

	return nil
}

func (txn *Txn) Get(key []byte) ([]byte, error) {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Check write set first
	if val, exists := txn.WriteSet[string(key)]; exists {
		return val, nil
	}

	// Record read for conflict detection
	txn.ReadSet[string(key)] = txn.Timestamp

	// Fetch from skiplist
	val := txn.db.memtable.Load().(*Memtable).skiplist.Get(key, txn.Timestamp)
	if val != nil {

		return val.([]byte), nil
	}

	// Check immutable memtables
	txn.db.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		val = memt.skiplist.Get(key, txn.Timestamp)
		if val != nil {
			return false // Found in immutable memtable
		}
		return true // Continue searching
	})

	if val != nil {
		return val.([]byte), nil
	}

	// Check levels
	levels := txn.db.levels.Load()
	for _, level := range *levels {
		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		for _, sstable := range *sstables {
			val = sstable.get(key, txn.Timestamp)
			if val != nil {
				return val.([]byte), nil
			}
		}
	}

	return nil, fmt.Errorf("key not found")
}

// Update performs an atomic update using a transaction
func (db *DB) Update(fn func(txn *Txn) error) error {

	txn := db.Begin()
	err := fn(txn)
	if err != nil {
		err = txn.Rollback()
		if err != nil {
			return err
		}
		return err
	}
	return txn.Commit()
}

// serializeSSTable uses gob to serialize the sstable metadata
func (sst *SSTable) serializeSSTable() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the sst
	err := encoder.Encode(sst)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeSSTable uses gob to deserialize the sstable metadata
func (sst *SSTable) deserializeSSTable(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the sst
	err := decoder.Decode(sst)
	if err != nil {
		return err
	}

	return nil
}

// serializeTransaction uses gob to serialize the transaction
func (txn *Txn) serializeTransaction() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the transaction
	err := encoder.Encode(txn)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeTransaction uses gob to deserialize the transaction
func (txn *Txn) deserializeTransaction(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the transaction
	err := decoder.Decode(txn)
	if err != nil {
		return err
	}

	return nil
}

// serializeBlockSet uses gob to serialize the block set
func (bs *BlockSet) serializeBlockSet() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the block set
	err := encoder.Encode(bs)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeBlockSet uses gob to deserialize the block set
func (bs *BlockSet) deserializeBlockSet(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the block set
	err := decoder.Decode(bs)
	if err != nil {
		return err
	}

	return nil

}

// Helper function to extract timestamp from a WAL filename
func extractTimestampFromFilename(filename string) int64 {
	// Filename format is <timestamp>.wal
	parts := strings.Split(filename, ".")
	if len(parts) != 2 {
		return 0
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}

	return ts
}

// backgroundFlusher dequeues immutable memtables and flushes them to disk
func (db *DB) backgroundFlusher() {
	defer db.wg.Done()
	ticker := time.NewTicker(time.Millisecond * 24)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			log.Println("Flusher stopped")
			return
		case <-ticker.C:
			immutableMemt := db.immutable.Dequeue()
			if immutableMemt == nil {
				continue // No immutable memtable to flush
			}

			log.Println("Flushing immutable memtable to disk:", immutableMemt.(*Memtable).wal.path)

			// Flush the immutable memtable to disk
			err := db.flushMemtable(immutableMemt.(*Memtable))
			if err != nil {
				log.Printf("Failed to flush immutable memtable: %v", err)
				continue
			}
		}
	}
}

// flushMemtable flushes the memtable to disk as an SSTable at level 1
func (db *DB) flushMemtable(memt *Memtable) error {
	// Create a new SSTable
	sstable := &SSTable{
		Id:    db.idGenerator.nextID(),
		db:    db,
		Level: 1,
	}

	log.Println("Flushing memtable to disk with ID:", sstable.Id)

	minKey, _, exists := memt.skiplist.GetMin(time.Now().UnixMicro())
	if exists {
		sstable.Min = minKey
	}

	maxKey, _, exists := memt.skiplist.GetMax(time.Now().UnixNano())
	if exists {
		sstable.Max = maxKey
	}

	log.Println("Min key:", string(sstable.Min), "Max key:", string(sstable.Max))

	// Calculate the approx size of the memtable
	sstable.Size = atomic.LoadInt64(&memt.Size)
	sstable.EntryCount = memt.skiplist.Count(time.Now().UnixNano())

	// We create new sstable files (.klog and .vlog) here
	klogPath := fmt.Sprintf("%s%s1%s%s%d%s", db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s1%s%s%d%s", db.opts.Directory, LevelPrefix, string(os.PathSeparator), SSTablePrefix, sstable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, 0666, blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// Encode metadata
	sstableData, err := sstable.serializeSSTable()
	if err != nil {
		return fmt.Errorf("failed to serialize SSTable: %w", err)
	}

	// Write first block to KLog
	_, err = klogBm.Append(sstableData)
	if err != nil {
		return fmt.Errorf("failed to write KLog: %w", err)
	}

	blockset := &BlockSet{
		Entries: make([]*KLogEntry, 0),
		Size:    0,
	}

	// Now we create a memtable iter
	iter := memt.skiplist.NewIterator(nil, time.Now().UnixNano())
	for {
		key, value, ts, ok := iter.Next()
		if !ok {
			break // No more entries
		}

		// Write the key-value pair to the VLog
		id, err := vlogBm.Append(value.([]byte)[:])
		if err != nil {
			return fmt.Errorf("failed to write VLog: %w", err)
		}

		klogEntry := &KLogEntry{
			Key:          key,
			Timestamp:    ts,
			ValueBlockID: id,
		}

		blockset.Entries = append(blockset.Entries, klogEntry)
		blockset.Size += int64(len(key) + len(value.([]byte)))

		sstable.EntryCount++

		if blockset.Size >= db.opts.BlockSetSize {
			// Write the blockset to KLog
			blocksetData, err := blockset.serializeBlockSet()
			if err != nil {
				return fmt.Errorf("failed to serialize BlockSet: %w", err)
			}

			_, err = klogBm.Append(blocksetData)
			if err != nil {
				return fmt.Errorf("failed to write BlockSet to KLog: %w", err)
			}

			blockset.Entries = make([]*KLogEntry, 0)
			blockset.Size = 0
		}
	}

	// Write any remaining blockset to KLog
	if len(blockset.Entries) > 0 {
		blocksetData, err := blockset.serializeBlockSet()
		if err != nil {
			return fmt.Errorf("failed to serialize BlockSet: %w", err)
		}

		_, err = klogBm.Append(blocksetData)
		if err != nil {
			return fmt.Errorf("failed to write BlockSet to KLog: %w", err)
		}

	}

	// Add both KLog and VLog to the LRU cache
	db.lru.Put(klogPath, klogBm)
	db.lru.Put(vlogPath, vlogBm)

	// Add the SSTable to level 1
	levels := db.levels.Load()
	if levels == nil {
		return fmt.Errorf("levels not initialized")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	var sstablesList []*SSTable

	if sstables != nil {
		sstablesList = *sstables
	} else {
		sstablesList = make([]*SSTable, 0)
	}

	sstablesList = append(sstablesList, sstable)

	level1.sstables.Store(&sstablesList)

	// Update the current size of the level
	atomic.AddInt64(&level1.currentSize, sstable.Size)

	log.Println("Flushed memtable to disk with ID:", sstable.Id, "Size:", sstable.Size)

	return nil
}

// newSSTableIDGenerator creates a new SSTable ID generator
func newSSTableIDGenerator() *SSTableIDGenerator {
	return &SSTableIDGenerator{
		lastID: time.Now().UnixNano(),
	}
}

// NextID generates the next unique SSTable ID
func (g *SSTableIDGenerator) nextID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Get current timestamp
	ts := time.Now().UnixNano()

	// Ensure monotonicity by using max of current time and last ID + 1
	if ts <= g.lastID {
		ts = g.lastID + 1
	}

	// Update last ID
	g.lastID = ts

	return ts
}

// Get retrieves a value from the SSTable using the key and timestamp
func (sst *SSTable) get(key []byte, timestamp int64) interface{} {
	// Fix the range check - only proceed if key is in range
	if bytes.Compare(key, sst.Min) < 0 || bytes.Compare(key, sst.Max) > 0 {
		return nil // Key not in range
	}

	// Get the KLog block manager
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", sst.db.opts.Directory, LevelPrefix, sst.Level, string(os.PathSeparator),
		SSTablePrefix, sst.Id, KLogExtension)
	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil
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
		return nil
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
		vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", sst.db.opts.Directory, LevelPrefix, sst.Level, string(os.PathSeparator),
			SSTablePrefix, sst.Id, VLogExtension)
		var vlogBm *blockmanager.BlockManager

		if v, ok := sst.db.lru.Get(vlogPath); ok {
			vlogBm = v.(*blockmanager.BlockManager)
		} else {
			vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, 0666,
				blockmanager.SyncOption(sst.db.opts.SyncOption))
			if err != nil {
				return nil
			}
			sst.db.lru.Put(vlogPath, vlogBm)
		}

		// Read the value from VLog
		value, _, err := vlogBm.Read(foundEntry.ValueBlockID)
		if err != nil {
			return nil
		}

		return value
	}

	return nil // Key not found or no valid version for the timestamp
}

// MergeIterator is a bidirectional iterator that merges results from multiple sources
// while maintaining transaction isolation at the given read timestamp
type MergeIterator struct {
	txn            *Txn                 // Transaction for isolation
	memtableIter   *skiplist.Iterator   // Iterator for current memtable
	immutableIters []*skiplist.Iterator // Iterators for immutable memtables
	sstableIters   [][]*SSTableIterator // Iterators for SSTables by level

	// Current position data
	current       *iteratorHeapItem // Current item
	lastDirection int               // Last direction of iteration (1 for forward, -1 for backward)

	// Heap for merging
	minHeap minHeapItems // Min heap for Next operations
	maxHeap maxHeapItems // Max heap for Prev operations

	// Track seen keys to avoid duplicates (MVCC)
	seenKeys map[string]bool
}

// iteratorHeapItem represents an item in the iterator heaps
type iteratorHeapItem struct {
	key       []byte      // Key of the item
	value     interface{} // Value of the item
	source    int         // Source identifier (0=memtable, 1=immutable, 2+=sstables)
	sourceIdx int         // Index within the source type
	timestamp int64       // Timestamp of the item
}

// SSTableIterator is an iterator for a specific SSTable
type SSTableIterator struct {
	sstable *SSTable
	readTs  int64
	db      *DB
	klogBm  *blockmanager.BlockManager
	vlogBm  *blockmanager.BlockManager

	// Current state
	currentSet *BlockSet
	currentIdx int
	currentKey []byte

	// For bidirectional traversal
	history     []blockIterState
	initialized bool
	finished    bool
}

// blockIterState tracks state for bidirectional traversal
type blockIterState struct {
	blockID   int64
	setIdx    int
	key       []byte
	timestamp int64
	value     interface{}
}

// minHeapItems implements heap.Interface for min heap (Next operations)
type minHeapItems []*iteratorHeapItem

func (h minHeapItems) Len() int { return len(h) }
func (h minHeapItems) Less(i, j int) bool {
	// First compare keys
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// For same keys, newer versions come first (higher timestamp)
	return h[i].timestamp > h[j].timestamp
}
func (h minHeapItems) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeapItems) Push(x interface{}) { *h = append(*h, x.(*iteratorHeapItem)) }
func (h *minHeapItems) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// maxHeapItems implements heap.Interface for max heap (Prev operations)
type maxHeapItems []*iteratorHeapItem

func (h maxHeapItems) Len() int { return len(h) }
func (h maxHeapItems) Less(i, j int) bool {
	// First compare keys in reverse order
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp > 0 // Note the > instead of < for max heap
	}
	// For same keys, newer versions come first (higher timestamp)
	return h[i].timestamp > h[j].timestamp
}
func (h maxHeapItems) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeapItems) Push(x interface{}) { *h = append(*h, x.(*iteratorHeapItem)) }
func (h *maxHeapItems) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NewMergeIterator creates a new iterator that merges all sources
// startKey can be nil to start from the beginning
func (txn *Txn) NewMergeIterator(startKey []byte) *MergeIterator {
	iter := &MergeIterator{
		txn:           txn,
		minHeap:       make(minHeapItems, 0),
		maxHeap:       make(maxHeapItems, 0),
		seenKeys:      make(map[string]bool),
		lastDirection: 0, // No direction yet
	}

	// Initialize the heap
	heap.Init(&iter.minHeap)
	heap.Init(&iter.maxHeap)

	// Initialize memtable iterator
	iter.memtableIter = txn.db.memtable.Load().(*Memtable).skiplist.NewIterator(startKey, txn.Timestamp)

	// Initialize immutable memtable iterators
	immutableMemtables := txn.db.immutable.List()
	iter.immutableIters = make([]*skiplist.Iterator, len(immutableMemtables))
	for i, memt := range immutableMemtables {
		if memt == nil {
			continue
		}
		iter.immutableIters[i] = memt.(*Memtable).skiplist.NewIterator(startKey, txn.Timestamp)
	}

	// Initialize SSTable iterators by level
	levels := txn.db.levels.Load()
	if levels != nil {
		iter.sstableIters = make([][]*SSTableIterator, len(*levels))

		for levelIdx, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}

			iter.sstableIters[levelIdx] = make([]*SSTableIterator, len(*sstables))
			for sstIdx, sst := range *sstables {
				// Skip SSTables that don't contain the key range we're looking for
				if startKey != nil && bytes.Compare(startKey, sst.Min) < 0 {
					continue
				}
				if startKey != nil && bytes.Compare(startKey, sst.Max) > 0 {
					continue
				}

				iter.sstableIters[levelIdx][sstIdx] = newSSTableIterator(sst, txn.Timestamp)
			}
		}
	}

	// Prime the iterators
	iter.initHeaps()

	return iter
}

// initHeaps initializes both min and max heaps with the first elements from each iterator
func (iter *MergeIterator) initHeaps() {
	// Clear existing heaps
	iter.minHeap = make(minHeapItems, 0)
	iter.maxHeap = make(maxHeapItems, 0)
	iter.seenKeys = make(map[string]bool)

	// Add first items from memtable iterator
	key, value, ts, ok := iter.memtableIter.Next()
	if ok {
		item := &iteratorHeapItem{
			key:       key,
			value:     value,
			timestamp: ts,
			source:    0, // Memtable source
			sourceIdx: 0,
		}
		heap.Push(&iter.minHeap, item)
		heap.Push(&iter.maxHeap, item)
	}

	// Add first items from immutable memtable iterators
	for i, immutableIter := range iter.immutableIters {
		if immutableIter == nil {
			continue
		}

		key, value, ts, ok := immutableIter.Next()
		if ok {
			item := &iteratorHeapItem{
				key:       key,
				value:     value,
				timestamp: ts,
				source:    1, // Immutable memtable source
				sourceIdx: i,
			}
			heap.Push(&iter.minHeap, item)
			heap.Push(&iter.maxHeap, item)
		}
	}

	// Add first items from SSTable iterators
	for levelIdx, levelIters := range iter.sstableIters {
		for sstIdx, sstIter := range levelIters {
			if sstIter == nil {
				continue
			}

			key, value, ts, ok := sstIter.Next()
			if ok {
				item := &iteratorHeapItem{
					key:       key,
					value:     value,
					timestamp: ts,
					source:    2 + levelIdx, // SSTable source (level + 2)
					sourceIdx: sstIdx,
				}
				heap.Push(&iter.minHeap, item)
				heap.Push(&iter.maxHeap, item)
			}
		}
	}
}

// Next returns the next key-value pair in ascending order
func (iter *MergeIterator) Next() ([]byte, interface{}, bool) {
	// If we were going backward, we need to reset and reinitialize
	if iter.lastDirection == -1 {
		iter.initHeaps()
	}
	iter.lastDirection = 1

	// Get items from the min heap until we find one that hasn't been seen
	// This ensures we only return one version of each key (MVCC)
	for iter.minHeap.Len() > 0 {
		item := heap.Pop(&iter.minHeap).(*iteratorHeapItem)
		keyStr := string(item.key)

		// Skip this key if we've seen it before
		if iter.seenKeys[keyStr] {
			// Get the next item from this source to replace what we just popped
			iter.advanceSourceForward(item.source, item.sourceIdx)
			continue
		}

		// Mark this key as seen
		iter.seenKeys[keyStr] = true
		iter.current = item

		// Get the next item from this source to replace what we just popped
		iter.advanceSourceForward(item.source, item.sourceIdx)

		// Check if this key is in our transaction's write set (would override anything from storage)
		if val, exists := iter.txn.WriteSet[keyStr]; exists {
			return item.key, val, true
		}

		// Check if this key is in the delete set
		if iter.txn.DeleteSet[keyStr] {
			// This key was deleted in the current transaction, skip it
			continue
		}

		return item.key, item.value, true
	}

	return nil, nil, false
}

// Prev returns the previous key-value pair in descending order
func (iter *MergeIterator) Prev() ([]byte, interface{}, bool) {
	// If we were going forward, we need to reset and reinitialize
	if iter.lastDirection == 1 {
		iter.initHeaps()
	}
	iter.lastDirection = -1

	// Get items from the max heap until we find one that hasn't been seen
	for iter.maxHeap.Len() > 0 {
		item := heap.Pop(&iter.maxHeap).(*iteratorHeapItem)
		keyStr := string(item.key)

		// Skip this key if we've seen it before
		if iter.seenKeys[keyStr] {
			// Get the previous item from this source to replace what we just popped
			iter.advanceSourceBackward(item.source, item.sourceIdx)
			continue
		}

		// Mark this key as seen
		iter.seenKeys[keyStr] = true
		iter.current = item

		// Get the previous item from this source to replace what we just popped
		iter.advanceSourceBackward(item.source, item.sourceIdx)

		// Check if this key is in our transaction's write set (would override anything from storage)
		if val, exists := iter.txn.WriteSet[keyStr]; exists {
			return item.key, val, true
		}

		// Check if this key is in the delete set
		if iter.txn.DeleteSet[keyStr] {
			// This key was deleted in the current transaction, skip it
			continue
		}

		return item.key, item.value, true
	}

	return nil, nil, false
}

// advanceSourceForward advances the specified source and adds the next item to the min heap
func (iter *MergeIterator) advanceSourceForward(source int, sourceIdx int) {
	var key []byte
	var value interface{}
	var ts int64
	var ok bool

	// Get the next item from the appropriate source
	if source == 0 {
		// Memtable
		key, value, ts, ok = iter.memtableIter.Next()
	} else if source == 1 {
		// Immutable memtable
		if sourceIdx < len(iter.immutableIters) && iter.immutableIters[sourceIdx] != nil {
			key, value, ts, ok = iter.immutableIters[sourceIdx].Next()
		}
	} else {
		// SSTable
		levelIdx := source - 2
		if levelIdx < len(iter.sstableIters) &&
			sourceIdx < len(iter.sstableIters[levelIdx]) &&
			iter.sstableIters[levelIdx][sourceIdx] != nil {
			key, value, ts, ok = iter.sstableIters[levelIdx][sourceIdx].Next()
		}
	}

	// If we got a valid item, add it to the min heap
	if ok {
		item := &iteratorHeapItem{
			key:       key,
			value:     value,
			timestamp: ts,
			source:    source,
			sourceIdx: sourceIdx,
		}
		heap.Push(&iter.minHeap, item)
	}
}

// advanceSourceBackward advances the specified source backward and adds the previous item to the max heap
func (iter *MergeIterator) advanceSourceBackward(source int, sourceIdx int) {
	var key []byte
	var value interface{}
	var ts int64
	var ok bool

	// Get the previous item from the appropriate source
	if source == 0 {
		// Memtable
		key, value, ts, ok = iter.memtableIter.Prev()
	} else if source == 1 {
		// Immutable memtable
		if sourceIdx < len(iter.immutableIters) && iter.immutableIters[sourceIdx] != nil {
			key, value, ts, ok = iter.immutableIters[sourceIdx].Prev()
		}
	} else {
		// SSTable
		levelIdx := source - 2
		if levelIdx < len(iter.sstableIters) &&
			sourceIdx < len(iter.sstableIters[levelIdx]) &&
			iter.sstableIters[levelIdx][sourceIdx] != nil {
			key, value, ts, ok = iter.sstableIters[levelIdx][sourceIdx].Prev()
		}
	}

	// If we got a valid item, add it to the max heap
	if ok {
		item := &iteratorHeapItem{
			key:       key,
			value:     value,
			timestamp: ts,
			source:    source,
			sourceIdx: sourceIdx,
		}
		heap.Push(&iter.maxHeap, item)
	}
}

// Seek positions the iterator at the specified key or the next key if exact match not found
func (iter *MergeIterator) Seek(key []byte) ([]byte, interface{}, bool) {
	// Reinitialize with the new start key
	iter.memtableIter = iter.txn.db.memtable.Load().(*Memtable).skiplist.NewIterator(key, iter.txn.Timestamp)

	// Reinitialize immutable iterators
	for i, memt := range iter.txn.db.immutable.List() {
		if memt == nil {
			continue
		}
		iter.immutableIters[i] = memt.(*Memtable).skiplist.NewIterator(key, iter.txn.Timestamp)
	}

	// Reinitialize SSTable iterators
	levels := iter.txn.db.levels.Load()
	if levels != nil {
		for levelIdx, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}

			for sstIdx, sst := range *sstables {
				// Skip SSTables that don't contain the key range we're looking for
				if bytes.Compare(key, sst.Min) < 0 || bytes.Compare(key, sst.Max) > 0 {
					continue
				}

				if len(iter.sstableIters) > levelIdx && len(iter.sstableIters[levelIdx]) > sstIdx {
					iter.sstableIters[levelIdx][sstIdx] = newSSTableIterator(sst, iter.txn.Timestamp)
				}
			}
		}
	}

	// Reset and prime the heaps
	iter.initHeaps()
	iter.lastDirection = 1 // Set direction to forward

	// Return the first item
	return iter.Next()
}

// newSSTableIterator creates a new iterator for an SSTable
func newSSTableIterator(sst *SSTable, readTs int64) *SSTableIterator {
	// Create KLog and VLog paths
	klogPath := sst.db.getSSTablKLogPath(sst)
	vlogPath := sst.db.getSSTablVLogPath(sst)

	// Get or open the KLog block manager
	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil
		}
		sst.db.lru.Put(klogPath, klogBm)
	}

	// Get or open the VLog block manager
	var vlogBm *blockmanager.BlockManager

	if v, ok := sst.db.lru.Get(vlogPath); ok {
		vlogBm = v.(*blockmanager.BlockManager)
	} else {
		vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return nil
		}
		sst.db.lru.Put(vlogPath, vlogBm)
	}

	return &SSTableIterator{
		sstable: sst,
		readTs:  readTs,
		db:      sst.db,
		klogBm:  klogBm,
		vlogBm:  vlogBm,
		history: make([]blockIterState, 0),
	}
}

// Next returns the next key-value pair from the SSTable
func (iter *SSTableIterator) Next() ([]byte, interface{}, int64, bool) {
	// If we've finished, no more items
	if iter.finished {
		return nil, nil, 0, false
	}

	// If we're not initialized, get the first block of data
	if !iter.initialized {
		if !iter.initializeForward() {
			return nil, nil, 0, false
		}
	}

	// If we were previously going backward, we need to handle that special case
	if len(iter.history) > 0 {
		// Pop the last history item and return it
		lastState := iter.history[len(iter.history)-1]
		iter.history = iter.history[:len(iter.history)-1]

		// Update the current state
		iter.currentKey = lastState.key

		return lastState.key, lastState.value, lastState.timestamp, true
	}

	// Keep looking until we find a valid entry or exhaust the SSTable
	for {
		// If we've reached the end of the current block set, load the next one
		if iter.currentSet == nil || iter.currentIdx >= len(iter.currentSet.Entries) {
			if !iter.loadNextBlockSet() {
				iter.finished = true
				return nil, nil, 0, false
			}
			iter.currentIdx = 0
		}

		// Get the current entry
		entry := iter.currentSet.Entries[iter.currentIdx]
		iter.currentIdx++

		// Skip entries with timestamps after our read timestamp (MVCC)
		if entry.Timestamp > iter.readTs {
			continue
		}

		// Read the value from VLog
		value, _, err := iter.vlogBm.Read(entry.ValueBlockID)
		if err != nil {
			continue
		}

		// Update current key and save state to history for bidirectional traversal
		iter.currentKey = entry.Key
		iter.history = append(iter.history, blockIterState{
			key:       entry.Key,
			timestamp: entry.Timestamp,
			value:     value,
		})

		return entry.Key, value, entry.Timestamp, true
	}
}

// Prev returns the previous key-value pair from the SSTable
func (iter *SSTableIterator) Prev() ([]byte, interface{}, int64, bool) {
	// If we're not initialized, initialize backward
	if !iter.initialized {
		if !iter.initializeBackward() {
			return nil, nil, 0, false
		}
	}

	// If we have history items, use the most recent one
	if len(iter.history) > 0 {
		lastState := iter.history[len(iter.history)-1]
		iter.history = iter.history[:len(iter.history)-1]

		// Update the current state
		iter.currentKey = lastState.key

		return lastState.key, lastState.value, lastState.timestamp, true
	}

	// If we don't have history and we're going backward, we need to build it
	// This is complex and requires scanning the SSTable from the beginning up to
	// the current position, which is inefficient but necessary for bidirectional traversal
	// in SSTable format which is inherently forward-oriented

	// Reset to beginning and build history
	iter.initialized = false
	if !iter.buildHistoryUntil(iter.currentKey) {
		return nil, nil, 0, false
	}

	// Now try again with rebuilt history
	return iter.Prev()
}

// initializeForward initializes the iterator for forward traversal
func (iter *SSTableIterator) initializeForward() bool {
	// Skip the first block which contains SSTable metadata
	blockData, _, err := iter.klogBm.Read(1)
	if err != nil {
		return false
	}

	// Get the second block which should be the first block set
	blockData, _, err = iter.klogBm.Read(2)
	if err != nil {
		return false
	}

	// Deserialize the block set
	var blockset BlockSet
	err = blockset.deserializeBlockSet(blockData)
	if err != nil {
		return false
	}

	iter.currentSet = &blockset
	iter.currentIdx = 0
	iter.initialized = true

	return true
}

// initializeBackward initializes the iterator for backward traversal
// This is more complex as we need to find the last valid entry
func (iter *SSTableIterator) initializeBackward() bool {
	// We need to read all blocks and build history from the beginning
	// then we'll traverse from the end

	// First, build complete history
	if !iter.buildCompleteHistory() {
		return false
	}

	// Mark as initialized
	iter.initialized = true

	// Sort history by key in descending order
	sort.Slice(iter.history, func(i, j int) bool {
		// First by key in descending order
		cmp := bytes.Compare(iter.history[i].key, iter.history[j].key)
		if cmp != 0 {
			return cmp > 0 // Note: > for descending
		}
		// For same key, newer timestamps first
		return iter.history[i].timestamp > iter.history[j].timestamp
	})

	// Deduplicate history to keep only the newest version of each key
	if len(iter.history) > 0 {
		dedupedHistory := make([]blockIterState, 0, len(iter.history))
		lastKey := iter.history[0].key
		dedupedHistory = append(dedupedHistory, iter.history[0])

		for i := 1; i < len(iter.history); i++ {
			if !bytes.Equal(iter.history[i].key, lastKey) {
				dedupedHistory = append(dedupedHistory, iter.history[i])
				lastKey = iter.history[i].key
			}
		}

		iter.history = dedupedHistory
	}

	return len(iter.history) > 0
}

// buildCompleteHistory reads all valid entries from the SSTable and builds complete history
func (iter *SSTableIterator) buildCompleteHistory() bool {
	// Skip the first block which contains SSTable metadata
	_, _, err := iter.klogBm.Read(1)
	if err != nil {
		return false
	}

	// Read all block sets
	blockIter := iter.klogBm.Iterator()

	// Skip first block (metadata)
	_, _, err = blockIter.Next()
	if err != nil {
		return false
	}

	// Process all blocks
	for {
		blockData, _, err := blockIter.Next()
		if err != nil {
			break // End of blocks
		}

		// Deserialize the block set
		var blockset BlockSet
		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			continue
		}

		// Process all entries in the block set
		for _, entry := range blockset.Entries {
			// Skip entries with timestamps after our read timestamp (MVCC)
			if entry.Timestamp > iter.readTs {
				continue
			}

			// Read the value from VLog
			value, _, err := iter.vlogBm.Read(entry.ValueBlockID)
			if err != nil {
				continue
			}

			// Add to history
			iter.history = append(iter.history, blockIterState{
				key:       entry.Key,
				timestamp: entry.Timestamp,
				value:     value,
			})
		}
	}

	return true
}

// buildHistoryUntil builds history up to the specified key
func (iter *SSTableIterator) buildHistoryUntil(targetKey []byte) bool {
	// Similar to buildCompleteHistory but stops when it reaches targetKey

	// Skip the first block which contains SSTable metadata
	_, _, err := iter.klogBm.Read(1)
	if err != nil {
		return false
	}

	// Read all block sets
	blockIter := iter.klogBm.Iterator()

	// Skip first block (metadata)
	_, _, err = blockIter.Next()
	if err != nil {
		return false
	}

	// Process all blocks
	for {
		blockData, _, err := blockIter.Next()
		if err != nil {
			break // End of blocks
		}

		// Deserialize the block set
		var blockset BlockSet
		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			continue
		}

		// Process all entries in the block set
		for _, entry := range blockset.Entries {
			// Stop if we've reached our target key
			if bytes.Compare(entry.Key, targetKey) >= 0 {
				// Sort history and deduplicate before returning
				sort.Slice(iter.history, func(i, j int) bool {
					// First by key in descending order
					cmp := bytes.Compare(iter.history[i].key, iter.history[j].key)
					if cmp != 0 {
						return cmp > 0 // Note: > for descending
					}
					// For same key, newer timestamps first
					return iter.history[i].timestamp > iter.history[j].timestamp
				})
				return true
			}

			// Skip entries with timestamps after our read timestamp (MVCC)
			if entry.Timestamp > iter.readTs {
				continue
			}

			// Read the value from VLog
			value, _, err := iter.vlogBm.Read(entry.ValueBlockID)
			if err != nil {
				continue
			}

			// Add to history
			iter.history = append(iter.history, blockIterState{
				key:       entry.Key,
				timestamp: entry.Timestamp,
				value:     value,
			})
		}
	}

	// If we get here, we've read all blocks
	// Sort history in descending order for Prev
	sort.Slice(iter.history, func(i, j int) bool {
		// First by key in descending order
		cmp := bytes.Compare(iter.history[i].key, iter.history[j].key)
		if cmp != 0 {
			return cmp > 0 // Note: > for descending
		}
		// For same key, newer timestamps first
		return iter.history[i].timestamp > iter.history[j].timestamp
	})

	return len(iter.history) > 0
}

// loadNextBlockSet loads the next block set from the SSTable
func (iter *SSTableIterator) loadNextBlockSet() bool {
	if iter.currentSet == nil {
		// First time loading, get the first data block (skip metadata)
		blockData, _, err := iter.klogBm.Read(2)
		if err != nil {
			return false
		}

		var blockset BlockSet
		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			return false
		}

		iter.currentSet = &blockset
		return true
	}

	// Get the next block in sequence
	blockIter := iter.klogBm.Iterator()

	// Skip blocks until we find where we were
	_, _, err := blockIter.Next() // Skip metadata
	if err != nil {
		return false
	}

	foundCurrent := false
	var blockset BlockSet

	for !foundCurrent {
		blockData, _, err := blockIter.Next()
		if err != nil {
			return false
		}

		err = blockset.deserializeBlockSet(blockData)
		if err != nil {
			continue
		}

		// Compare with current block set to see if we found it
		if len(blockset.Entries) > 0 && len(iter.currentSet.Entries) > 0 {
			if bytes.Equal(blockset.Entries[0].Key, iter.currentSet.Entries[0].Key) {
				foundCurrent = true
			}
		}
	}

	// Now get the next block
	blockData, _, err := blockIter.Next()
	if err != nil {
		return false
	}

	err = blockset.deserializeBlockSet(blockData)
	if err != nil {
		return false
	}

	iter.currentSet = &blockset
	return true
}

// getSSTablKLogPath and getSSTablVLogPath are helper methods for SSTable paths
func (db *DB) getSSTablKLogPath(sst *SSTable) string {
	return db.opts.Directory + LevelPrefix + strconv.Itoa(sst.Level) +
		string(os.PathSeparator) + SSTablePrefix + strconv.FormatInt(sst.Id, 10) + KLogExtension
}

func (db *DB) getSSTablVLogPath(sst *SSTable) string {
	return db.opts.Directory + LevelPrefix + strconv.Itoa(sst.Level) +
		string(os.PathSeparator) + SSTablePrefix + strconv.FormatInt(sst.Id, 10) + VLogExtension
}

// Transaction Iterator Methods

// NewIterator creates a bidirectional iterator for the transaction
func (txn *Txn) NewIterator(startKey []byte) *MergeIterator {
	return txn.NewMergeIterator(startKey)
}

// GetRange retrieves all key-value pairs in the given key range
func (txn *Txn) GetRange(startKey, endKey []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// Get an iterator starting at the start key
	iter := txn.NewIterator(startKey)

	// Iterate until we reach the end key or run out of keys
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}

		// Check if we've gone past the end key
		if endKey != nil && bytes.Compare(key, endKey) > 0 {
			break
		}

		// Add to result
		result[string(key)] = value.([]byte)
	}

	return result, nil
}

// Count returns the number of entries in the given key range
func (txn *Txn) Count(startKey, endKey []byte) (int, error) {
	count := 0

	// Get an iterator starting at the start key
	iter := txn.NewIterator(startKey)

	// Iterate until we reach the end key or run out of keys
	for {
		key, _, ok := iter.Next()
		if !ok {
			break
		}

		// Check if we've gone past the end key
		if endKey != nil && bytes.Compare(key, endKey) > 0 {
			break
		}

		count++
	}

	return count, nil
}

// Scan executes a function on each key-value pair in the given range
func (txn *Txn) Scan(startKey, endKey []byte, fn func(key []byte, value []byte) bool) error {
	// Get an iterator starting at the start key
	iter := txn.NewIterator(startKey)

	// Iterate until we reach the end key or run out of keys
	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}

		// Check if we've gone past the end key
		if endKey != nil && bytes.Compare(key, endKey) > 0 {
			break
		}

		// Apply the function
		// If it returns false, stop scanning
		if !fn(key, value.([]byte)) {
			break
		}
	}

	return nil
}

// ForEach iterates through all entries in the database
func (txn *Txn) ForEach(fn func(key []byte, value []byte) bool) error {
	return txn.Scan(nil, nil, fn)
}

// Iterate is a higher-level bidirectional iterator using callbacks
func (txn *Txn) Iterate(startKey []byte, direction int, fn func(key []byte, value []byte) bool) error {
	// Get a merge iterator starting at the given key
	iter := txn.NewIterator(startKey)

	// Iterate in the specified direction
	var key []byte
	var value interface{}
	var ok bool

	for {
		if direction >= 0 {
			// Forward iteration
			key, value, ok = iter.Next()
		} else {
			// Backward iteration
			key, value, ok = iter.Prev()
		}

		if !ok {
			break
		}

		// Apply the function
		// If it returns false, stop iterating
		if !fn(key, value.([]byte)) {
			break
		}
	}

	return nil
}

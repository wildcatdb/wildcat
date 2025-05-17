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

// CompactionManager manages database compactions
type CompactionManager struct {
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
		idGenerator: NewSSTableIDGenerator(),
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
	defer txn.remove()

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
		Id:    db.idGenerator.NextID(),
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
			log.Printf("Failed to open KLog block manager: %v", err)
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
			log.Printf("Failed to deserialize block set: %v", err)
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
				log.Printf("Failed to open VLog block manager: %v", err)
				return nil
			}
			sst.db.lru.Put(vlogPath, vlogBm)
		}

		// Read the value from VLog
		value, _, err := vlogBm.Read(foundEntry.ValueBlockID)
		if err != nil {
			log.Printf("Failed to read value from VLog: %v", err)
			return nil
		}

		return value
	}

	return nil // Key not found or no valid version for the timestamp
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(db *DB, maxConcurrency int) *CompactionManager {
	if maxConcurrency <= 0 {
		maxConcurrency = MaxCompactionConcurrency
	}

	return &CompactionManager{
		db:              db,
		compactionQueue: make([]*CompactionJob, 0),
		maxConcurrency:  maxConcurrency,
		lastCompaction:  time.Now(),
	}
}

// backgroundCompactor is the main compaction routine
func (db *DB) backgroundCompactor() {
	defer db.wg.Done()
	cm := NewCompactionManager(db, MaxCompactionConcurrency)
	ticker := time.NewTicker(time.Millisecond * 24)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			log.Println("Compactor stopped")
			return
		case <-ticker.C:
			// Check and schedule compactions
			cm.checkAndScheduleCompactions()

			// Execute pending compactions if under concurrency limit
			cm.executeCompactions()
		}
	}
}

// checkAndScheduleCompactions evaluates all levels for needed compactions
func (cm *CompactionManager) checkAndScheduleCompactions() {
	cm.scoreLock.Lock()
	defer cm.scoreLock.Unlock()

	// Only check for new compactions after cooldown period
	if time.Since(cm.lastCompaction) < CompactionCooldownPeriod {
		return
	}

	levels := cm.db.levels.Load()
	if levels == nil {
		return
	}

	// Evaluate each level for compaction
	for i, level := range *levels {
		// Skip last level
		if i == len(*levels)-1 {
			continue
		}

		sstables := level.sstables.Load()
		if sstables == nil || len(*sstables) == 0 {
			continue
		}

		// Calculate compaction score
		sizeScore := float64(atomic.LoadInt64(&level.currentSize)) / float64(level.capacity)
		countScore := float64(len(*sstables)) / float64(CompactionSizeThreshold)

		// Weight the scores
		score := sizeScore*CompactionScoreSizeWeight + countScore*CompactionScoreCountWeight

		// Schedule compaction if score exceeds threshold
		if score > 1.0 {
			// For lower levels (0-2), use size-tiered compaction
			if i < 2 {
				cm.scheduleSizeTieredCompaction(level, i, score)
			} else {
				// For higher levels, use leveled compaction
				cm.scheduleLeveledCompaction(level, i, score)
			}

			cm.lastCompaction = time.Now()
		}
	}

	// Sort compaction queue by priority
	sort.Slice(cm.compactionQueue, func(i, j int) bool {
		return cm.compactionQueue[i].Priority > cm.compactionQueue[j].Priority
	})
}

// scheduleSizeTieredCompaction schedules a size-tiered compaction
func (cm *CompactionManager) scheduleSizeTieredCompaction(level *Level, levelNum int, score float64) {
	sstables := level.sstables.Load()
	if len(*sstables) < CompactionSizeThreshold {
		return
	}

	// Sort SSTables by size for size-tiered compaction
	sortedTables := make([]*SSTable, len(*sstables))
	copy(sortedTables, *sstables)

	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Size < sortedTables[j].Size
	})

	// Find similar-sized SSTables
	var selectedTables []*SSTable

	// Select up to CompactionBatchSize tables with similar size
	for i := 0; i < len(sortedTables); {
		size := sortedTables[i].Size
		similarSized := []*SSTable{sortedTables[i]}

		j := i + 1
		for j < len(sortedTables) && float64(sortedTables[j].Size)/float64(size) <= 1.5 && len(similarSized) < CompactionBatchSize {
			similarSized = append(similarSized, sortedTables[j])
			j++
		}

		// If we found enough similar-sized tables, select them
		if len(similarSized) >= 2 {
			selectedTables = similarSized
			break
		}

		i = j
	}

	// If we couldn't find similar-sized tables, just take the smallest ones
	if len(selectedTables) < 2 && len(sortedTables) >= 2 {
		selectedTables = sortedTables[:min(CompactionBatchSize, len(sortedTables))]
	}

	if len(selectedTables) >= 2 {
		cm.compactionQueue = append(cm.compactionQueue, &CompactionJob{
			Level:       levelNum,
			Priority:    score,
			SSTables:    selectedTables,
			TargetLevel: levelNum + 1,
			InProgress:  false,
		})
	}
}

// scheduleLeveledCompaction schedules a leveled compaction
func (cm *CompactionManager) scheduleLeveledCompaction(level *Level, levelNum int, score float64) {
	sstables := level.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		return
	}

	// For leveled compaction, pick the oldest SSTable
	// (often this would be picking by smallest key range, but we'll use oldest for simplicity)
	oldestTable := (*sstables)[0]
	for _, table := range *sstables {
		if table.Id < oldestTable.Id {
			oldestTable = table
		}
	}

	// Find overlapping SSTables in the next level
	nextLevelNum := levelNum + 1
	if nextLevelNum >= len(*cm.db.levels.Load()) {
		return
	}

	nextLevel := (*cm.db.levels.Load())[nextLevelNum]
	nextLevelTables := nextLevel.sstables.Load()
	if nextLevelTables == nil {
		// No tables in next level, just move the table down
		cm.compactionQueue = append(cm.compactionQueue, &CompactionJob{
			Level:       levelNum,
			Priority:    score,
			SSTables:    []*SSTable{oldestTable},
			TargetLevel: nextLevelNum,
			InProgress:  false,
		})
		return
	}

	// Find overlapping tables in next level
	var overlappingTables []*SSTable
	for _, table := range *nextLevelTables {
		if bytes.Compare(table.Max, oldestTable.Min) >= 0 && bytes.Compare(table.Min, oldestTable.Max) <= 0 {
			overlappingTables = append(overlappingTables, table)
		}
	}

	// Create compaction job with selected table and overlapping tables
	selectedTables := []*SSTable{oldestTable}
	selectedTables = append(selectedTables, overlappingTables...)

	cm.compactionQueue = append(cm.compactionQueue, &CompactionJob{
		Level:       levelNum,
		Priority:    score,
		SSTables:    selectedTables,
		TargetLevel: nextLevelNum,
		InProgress:  false,
	})
}

// executeCompactions processes pending compaction jobs
func (cm *CompactionManager) executeCompactions() {
	// Skip if we've reached max concurrency
	if atomic.LoadInt32(&cm.activeJobs) >= int32(cm.maxConcurrency) {
		return
	}

	// Find the highest priority non-in-progress job
	var selectedJob *CompactionJob
	var selectedIdx int

	for i, job := range cm.compactionQueue {
		if !job.InProgress {
			selectedJob = job
			selectedIdx = i
			break
		}
	}

	if selectedJob == nil {
		return
	}

	// Mark the job as in progress
	selectedJob.InProgress = true
	atomic.AddInt32(&cm.activeJobs, 1)

	// Execute the compaction in a goroutine
	go func(job *CompactionJob, idx int) {
		defer func() {
			atomic.AddInt32(&cm.activeJobs, -1)

			// Remove job from queue when done
			cm.scoreLock.Lock()
			defer cm.scoreLock.Unlock()

			// Only remove if it's still in the queue at the same position
			if idx < len(cm.compactionQueue) && cm.compactionQueue[idx] == job {
				cm.compactionQueue = append(cm.compactionQueue[:idx], cm.compactionQueue[idx+1:]...)
			}
		}()

		// Execute the actual compaction
		err := cm.db.compactSSTables(job.SSTables, job.Level+1, job.TargetLevel)
		if err != nil {
			log.Printf("Failed to compact SSTables: %v", err)
		}
	}(selectedJob, selectedIdx)
}

// compactSSTables performs the actual compaction of SSTables
func (db *DB) compactSSTables(sstables []*SSTable, sourceLevel, targetLevel int) error {
	if len(sstables) == 0 {
		return nil
	}

	log.Printf("Starting compaction: %d SSTables from level %d to level %d",
		len(sstables), sourceLevel, targetLevel)

	// Create a new SSTable for the target level
	newSSTable := &SSTable{
		Id:    db.idGenerator.NextID(),
		db:    db,
		Level: targetLevel,
	}

	// Find min and max keys across all input tables
	newSSTable.Min = sstables[0].Min
	newSSTable.Max = sstables[0].Max

	for _, table := range sstables {
		if bytes.Compare(table.Min, newSSTable.Min) < 0 {
			newSSTable.Min = table.Min
		}
		if bytes.Compare(table.Max, newSSTable.Max) > 0 {
			newSSTable.Max = table.Max
		}

		// Mark table as being merged to prevent concurrent access
		atomic.StoreInt32(&table.isMerging, 1)
	}

	// Create new SSTable files for the compacted result
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, 0666,
		blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, 0666,
		blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// Merge the SSTables
	err = db.mergeSSTables(sstables, klogBm, vlogBm, newSSTable)
	if err != nil {
		// Clean up on error
		os.Remove(klogPath)
		os.Remove(vlogPath)
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	// Add the new SSTable to the target level
	targetLevelPtr := (*db.levels.Load())[targetLevel]
	currSSTables := targetLevelPtr.sstables.Load()

	var newSSTables []*SSTable
	if currSSTables != nil {
		newSSTables = make([]*SSTable, len(*currSSTables)+1)
		copy(newSSTables, *currSSTables)
		newSSTables[len(*currSSTables)] = newSSTable
	} else {
		newSSTables = []*SSTable{newSSTable}
	}

	targetLevelPtr.sstables.Store(&newSSTables)

	// Update the level size
	atomic.AddInt64(&targetLevelPtr.currentSize, newSSTable.Size)

	// Remove the original SSTables from the source level
	if sourceLevel != targetLevel {
		sourceLevelPtr := (*db.levels.Load())[sourceLevel]
		currentSSTables := sourceLevelPtr.sstables.Load()

		if currentSSTables != nil {
			// Create a map for fast lookup of SSTables to remove
			toRemove := make(map[int64]bool)
			for _, table := range sstables {
				toRemove[table.Id] = true
			}

			// Filter out the SSTables that were merged
			remainingSSTables := make([]*SSTable, 0, len(*currentSSTables))
			for _, table := range *currentSSTables {
				if !toRemove[table.Id] {
					remainingSSTables = append(remainingSSTables, table)
				} else {
					// Update level size
					atomic.AddInt64(&sourceLevelPtr.currentSize, -table.Size)
				}
			}

			sourceLevelPtr.sstables.Store(&remainingSSTables)
		}
	}

	// Add KLog and VLog managers to LRU cache
	db.lru.Put(klogPath, klogBm)
	db.lru.Put(vlogPath, vlogBm)

	// Clean up the old SSTable files (asynchronously to avoid blocking)
	go func() {
		for _, table := range sstables {
			oldKlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, sourceLevel,
				string(os.PathSeparator), SSTablePrefix, table.Id, KLogExtension)
			oldVlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, sourceLevel,
				string(os.PathSeparator), SSTablePrefix, table.Id, VLogExtension)

			// Wait a bit before deleting to ensure no ongoing reads
			time.Sleep(100 * time.Millisecond)

			// Remove from LRU first
			if bm, ok := db.lru.Get(oldKlogPath); ok {
				if bm, ok := bm.(*blockmanager.BlockManager); ok {
					bm.Close()
				}
				db.lru.Delete(oldKlogPath)
			}

			if bm, ok := db.lru.Get(oldVlogPath); ok {
				if bm, ok := bm.(*blockmanager.BlockManager); ok {
					bm.Close()
				}
				db.lru.Delete(oldVlogPath)
			}

			// Delete the files
			os.Remove(oldKlogPath)
			os.Remove(oldVlogPath)
		}
	}()

	log.Printf("Completed compaction: %d SSTables from level %d to level %d, new table size: %d",
		len(sstables), sourceLevel, targetLevel, newSSTable.Size)

	return nil
}

// mergeSSTables merges multiple SSTables into a new SSTable
func (db *DB) mergeSSTables(sstables []*SSTable, klogBm, vlogBm *blockmanager.BlockManager, newSSTable *SSTable) error {
	// Write metadata as first block
	sstableData, err := newSSTable.serializeSSTable()
	if err != nil {
		return fmt.Errorf("failed to serialize SSTable: %w", err)
	}

	_, err = klogBm.Append(sstableData)
	if err != nil {
		return fmt.Errorf("failed to write KLog: %w", err)
	}

	// Create a merged iterator over all input SSTables
	iterators := make([]*SSTIterator, len(sstables))
	for i, table := range sstables {
		iterators[i] = newSSTIterator(table)
	}

	mergeIter := newMergeIterator(iterators)

	// Create a block set for the merged data
	blockset := &BlockSet{
		Entries: make([]*KLogEntry, 0),
		Size:    0,
	}

	entryCount := 0
	var totalSize int64 = 0

	// Iterate through all entries and merge them
	for {
		key, value, ts, valid := mergeIter.Next()
		if !valid {
			break
		}

		// Write the value to VLog
		id, err := vlogBm.Append(value.([]byte))
		if err != nil {
			return fmt.Errorf("failed to write VLog: %w", err)
		}

		klogEntry := &KLogEntry{
			Key:          key,
			Timestamp:    ts,
			ValueBlockID: id,
		}

		blockset.Entries = append(blockset.Entries, klogEntry)
		entrySize := int64(len(key) + len(value.([]byte)))
		blockset.Size += entrySize
		totalSize += entrySize
		entryCount++

		// Flush the block set if it reaches the threshold
		if blockset.Size >= db.opts.BlockSetSize {
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

	// Write any remaining entries
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

	// Update the SSTable metadata
	newSSTable.Size = totalSize
	newSSTable.EntryCount = entryCount

	return nil
}

// Helper iterators for compaction

// SSTIterator allows iteration over an SSTable
type SSTIterator struct {
	sstable    *SSTable
	klogIter   *blockmanager.Iterator
	blockset   *BlockSet
	blockIndex int
	eof        bool
}

// newSSTIterator creates a new iterator over an SSTable
func newSSTIterator(sst *SSTable) *SSTIterator {
	// Get the KLog block manager
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", sst.db.opts.Directory, LevelPrefix, sst.Level,
		string(os.PathSeparator), SSTablePrefix, sst.Id, KLogExtension)

	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			log.Printf("Failed to open KLog block manager: %v", err)
			return &SSTIterator{eof: true}
		}
		sst.db.lru.Put(klogPath, klogBm)
	}

	iter := &SSTIterator{
		sstable:    sst,
		klogIter:   klogBm.Iterator(),
		blockIndex: -1,
		eof:        false,
	}

	// Skip the first block (metadata)
	_, _, err = iter.klogIter.Next()
	if err != nil {
		iter.eof = true
		return iter
	}

	// Load the first block set
	err = iter.loadNextBlockSet()
	if err != nil {
		iter.eof = true
	}

	return iter
}

// loadNextBlockSet loads the next block set from the KLog
func (iter *SSTIterator) loadNextBlockSet() error {
	data, _, err := iter.klogIter.Next()
	if err != nil {
		return err
	}

	var blockset BlockSet
	err = blockset.deserializeBlockSet(data)
	if err != nil {
		return err
	}

	iter.blockset = &blockset
	iter.blockIndex = 0

	return nil
}

// Next returns the next key-value pair from the SSTable
func (iter *SSTIterator) Next() ([]byte, interface{}, int64, bool) {
	if iter.eof || iter.blockset == nil {
		return nil, nil, 0, false
	}

	// If we've reached the end of the current block set, load the next one
	if iter.blockIndex >= len(iter.blockset.Entries) {
		err := iter.loadNextBlockSet()
		if err != nil {
			iter.eof = true
			return nil, nil, 0, false
		}
	}

	// Get the current entry
	entry := iter.blockset.Entries[iter.blockIndex]
	iter.blockIndex++

	// Get the VLog block manager
	vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", iter.sstable.db.opts.Directory, LevelPrefix, iter.sstable.Level,
		string(os.PathSeparator), SSTablePrefix, iter.sstable.Id, VLogExtension)

	var vlogBm *blockmanager.BlockManager
	var err error

	if v, ok := iter.sstable.db.lru.Get(vlogPath); ok {
		vlogBm = v.(*blockmanager.BlockManager)
	} else {
		vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(iter.sstable.db.opts.SyncOption))
		if err != nil {
			log.Printf("Failed to open VLog block manager: %v", err)
			iter.eof = true
			return nil, nil, 0, false
		}
		iter.sstable.db.lru.Put(vlogPath, vlogBm)
	}

	// Read the value from VLog
	value, _, err := vlogBm.Read(entry.ValueBlockID)
	if err != nil {
		log.Printf("Failed to read value from VLog: %v", err)
		iter.eof = true
		return nil, nil, 0, false
	}

	return entry.Key, value, entry.Timestamp, true
}

// MergeIterator merges multiple SSTable iterators
type MergeIterator struct {
	iters   []*SSTIterator
	current []*KeyValueEntry
}

// KeyValueEntry represents a key-value entry with timestamp
type KeyValueEntry struct {
	Key       []byte
	Value     interface{}
	Timestamp int64
}

// newMergeIterator creates a new merge iterator
func newMergeIterator(iters []*SSTIterator) *MergeIterator {
	m := &MergeIterator{
		iters:   iters,
		current: make([]*KeyValueEntry, len(iters)),
	}

	// Initialize the current entries
	for i, iter := range iters {
		key, value, ts, valid := iter.Next()
		if valid {
			m.current[i] = &KeyValueEntry{
				Key:       key,
				Value:     value,
				Timestamp: ts,
			}
		}
	}

	return m
}

// Next returns the next key-value pair in sorted order
func (m *MergeIterator) Next() ([]byte, interface{}, int64, bool) {
	// Find the smallest key
	var smallestIdx = -1
	var smallestKey []byte
	var latestTS int64

	for i, entry := range m.current {
		if entry == nil {
			continue
		}

		if smallestIdx == -1 || bytes.Compare(entry.Key, smallestKey) < 0 {
			smallestIdx = i
			smallestKey = entry.Key
			latestTS = entry.Timestamp
		} else if bytes.Equal(entry.Key, smallestKey) && entry.Timestamp > latestTS {
			// If keys are equal, take the one with the latest timestamp
			smallestIdx = i
			latestTS = entry.Timestamp
		}
	}

	if smallestIdx == -1 {
		return nil, nil, 0, false // No more entries
	}

	// Get the current smallest entry
	result := m.current[smallestIdx]

	// Advance the iterator that provided this entry
	key, value, ts, valid := m.iters[smallestIdx].Next()
	if valid {
		m.current[smallestIdx] = &KeyValueEntry{
			Key:       key,
			Value:     value,
			Timestamp: ts,
		}
	} else {
		m.current[smallestIdx] = nil
	}

	// For keys that match but have lower timestamps, skip them
	for i, entry := range m.current {
		if entry != nil && bytes.Equal(entry.Key, result.Key) && entry.Timestamp < result.Timestamp {
			// Advance this iterator
			key, value, ts, valid := m.iters[i].Next()
			if valid {
				m.current[i] = &KeyValueEntry{
					Key:       key,
					Value:     value,
					Timestamp: ts,
				}
			} else {
				m.current[i] = nil
			}
		}
	}

	return result.Key, result.Value, result.Timestamp, true
}

// shouldCompact determines if compaction is needed
func (db *DB) shouldCompact() bool {
	levels := db.levels.Load()
	if levels == nil {
		return false
	}

	// Check if any level has reached its capacity
	for i, level := range *levels {
		if i == len(*levels)-1 {
			continue // Skip the last level
		}

		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		// Size-based criteria - if level is above capacity * ratio
		if atomic.LoadInt64(&level.currentSize) > int64(float64(level.capacity)*CompactionSizeRatio) {
			return true
		}

		// Count-based criteria - if level has too many files
		if len(*sstables) >= CompactionSizeThreshold {
			return true
		}
	}

	return false
}

// NewSSTableIDGenerator creates a new SSTable ID generator
func NewSSTableIDGenerator() *SSTableIDGenerator {
	return &SSTableIDGenerator{
		lastID: time.Now().UnixNano(),
	}
}

// NextID generates the next unique SSTable ID
func (g *SSTableIDGenerator) NextID() int64 {
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

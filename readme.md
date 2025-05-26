<div>
    <h1 align="left"><img width="128" src="artwork/wildcat-logo.png"></h1>
</div>

![License](https://img.shields.io/badge/license-MPL_2.0-blue)


Wildcat is a high-performance embedded key-value database (or storage engine) written in Go. It incorporates modern database design principles including LSM (Log-Structured Merge) tree architecture, MVCC (Multi-Version Concurrency Control), and lock-free data structures for its critical paths, along with automatic background operations to deliver excellent read/write performance with strong consistency guarantees.

## Features
- LSM (Log-Structured Merge) tree architecture optimized for high write throughput
- Lock-free MVCC ensures non-blocking reads and writes
- WAL logging captures full transaction state for recovery and rehydration
- Version-aware skip list for fast in-memory MVCC access
- Atomic write path, safe for multithreaded use
- Scalable design with background flusher and compactor
- Durable and concurrent block storage, leveraging direct, offset-based file I/O (using `pread`/`pwrite`) for optimal performance and control
- Atomic LRU for active block manager handles
- Memtable lifecycle management and snapshot durability
- SSTables are immutable BTree's
- Configurable Sync options such as `None`, `Partial (with background interval)`, `Full`
- Snapshot-isolated MVCC with read timestamps
- Crash recovery restores all in-flight and committed transactions
- Automatic multi-threaded background compaction
- Full ACID transaction support
- Range, prefix, and full iteration support with bidirectional traversal
- Sustains 100K+ txns/sec writes, and hundreds of thousands of reads/sec
- Optional Bloom filter per SSTable for fast key lookups
- Key value separation optimization (`.klog` for keys, `.vlog` for values, klog entries point to vlog entries)
- Tombstone-aware compaction with retention based on active transaction windows
- Transaction recovery with incomplete transactions are preserved and accessible after crashes

## Table of Contents
- [Version and Compatibility](#version-and-compatibility)
- [Basic Usage](#basic-usage)
  - [Opening a Wildcat DB instance](#opening-a-wildcat-db-instance)
  - [Advanced Configuration](#advanced-configuration)
  - [Simple Key-Value Operations](#simple-key-value-operations)
  - [Manual Transaction Management](#manual-transaction-management)
  - [Iterating Keys](#iterating-keys)
    - [Full Iterator (bidirectional)](#full-iterator-bidirectional)
    - [Range Iterator (bidirectional)](#range-iterator-bidirectional)
    - [Prefix Iterator (bidirectional)](#prefix-iterator-bidirectional)
  - [Read-Only Transactions with View](#read-only-transactions-with-view)
  - [Batch Operations](#batch-operations)
  - [Transaction Recovery](#transaction-recovery)
  - [Log Channel](#log-channel)
  - [Database Statistics](#database-statistics)
  - [Force Flushing](#force-flushing)
- [Shared C Library](#shared-c-library)
- [Overview](#overview)
    - [MVCC Model](#mvcc-model)
    - [WAL and Durability](#wal-and-durability)
    - [Memtable Lifecycle](#memtable-lifecycle)
    - [SSTables and Compaction](#sstables-and-compaction)
    - [SSTable Metadata](#sstable-metadata)
    - [SSTable Format](#sstable-format)
    - [Compaction Policy / Strategy](#compaction-policy--strategy)
    - [Concurrency Model](#concurrency-model)
    - [Isolation Levels](#isolation-levels)
    - [Recoverability Guarantee Order](#recoverability-guarantee-order)
    - [Block Manager](#block-manager)
    - [LRU Cache](#lru-cache)
    - [Lock-Free Queue](#lock-free-queue)
    - [BTree](#btree)
    - [SkipList](#skiplist)
- [Motivation](#motivation)
- [Contributing](#contributing)

## Version and Compatibility
- Go 1.24+
- Linux/macOS/Windows (64-bit)

## Basic Usage
Wildcat supports opening multiple `wildcat.DB` instances in parallel, each operating independently in separate directories.

### Import
```go
import (
    "github.com/guycipher/wildcat"
)
```

### Opening a Wildcat DB instance
The only required option is the database directory path.
```go
// Create default options
opts := &wildcat.Options{
Directory: "/path/to/db",
    // Use defaults for other settings
}

// Open or create a new Wildcat DB instance
db, err := wildcat.Open(opts)
if err != nil {
    // Handle error
}
defer db.Close()
```

### Advanced Configuration
Wildcat provides several configuration options for fine-tuning.
```go
opts := &wildcat.Options{
    Directory:                   "/path/to/database",     // Directory for database files
    WriteBufferSize:             32 * 1024 * 1024,        // 32MB memtable size
    SyncOption:                  wildcat.SyncFull,        // Full sync for maximum durability
    SyncInterval:                128 * time.Millisecond,  // Only set when using SyncPartial
    LevelCount:                  7,                       // Number of LSM levels
    LevelMultiplier:             10,                      // Size multiplier between levels
    BlockManagerLRUSize:         256,                     // Cache size for block managers
    SSTableBTreeOrder:           10,                      // BTree order for SSTable klog
    LogChannel:                  make(chan string, 1000), // Channel for real time logging
    BloomFilter:                 false,                   // Enable/disable sstable bloom filters
    MaxCompactionConcurrency:    4,                       // Maximum concurrent compactions
    CompactionCooldownPeriod:    5 * time.Second,         // Cooldown between compactions
    CompactionBatchSize:         8,                       // Max SSTables per compaction
    CompactionSizeRatio:         1.1,                     // Level size ratio trigger
    CompactionSizeThreshold:     8,                       // File count trigger
    CompactionScoreSizeWeight:   0.8,                     // Weight for size-based scoring
    CompactionScoreCountWeight:  0.2,                     // Weight for count-based scoring
    FlusherTickerInterval:       1 * time.Millisecond,    // Flusher check interval
    CompactorTickerInterval:     250 * time.Millisecond,  // Compactor check interval
    BloomFilterFPR:              0.01,                    // Bloom filter false positive rate
    WalAppendRetry:              10,                      // WAL append retry count
    WalAppendBackoff:            128 * time.Microsecond,  // WAL append retry backoff
    BlockManagerLRUEvictRatio:   0.20,                    // LRU eviction ratio
    BlockManagerLRUAccesWeight:  0.8,                     // LRU access weight
}
```

#### Configuration Options Explained
1. **Directory** The path where the database files will be stored
2. **WriteBufferSize** Size threshold for memtable before flushing to disk
3. **SyncOption** Controls durability vs performance tradeoff
    - **SyncNone** Fastest, but no durability guarantees
    - **SyncPartial** Balances performance and durability
    - **SyncFull** Maximum durability, slower performance
4. **SyncInterval** Time between background sync operations (only for SyncPartial)
5. **LevelCount** Number of levels in the LSM tree
6. **LevelMultiplier** Size ratio between adjacent levels
7. **BlockManagerLRUSize** Number of block managers to cache
8. **SSTableBTreeOrder** Size of SSTable klog block sets
9. **LogChannel** Channel for real-time logging, useful for debugging and monitoring
10. **BloomFilter** Enable or disable bloom filters for SSTables to speed up key lookups. Bloom filters use double hashing with FNV-1a and FNV hash functions.  Is automatically sized based on expected items and desired false positive rate.
11. **MaxCompactionConcurrency** Maximum number of concurrent compactions
12. **CompactionCooldownPeriod** Cooldown period between compactions to prevent thrashing
13. **CompactionBatchSize** Max number of SSTables to compact at once
14. **CompactionSizeRatio** Level size ratio that triggers compaction
15. **CompactionSizeThreshold** Number of files to trigger size-tiered compaction
16. **CompactionScoreSizeWeight** Weight for size-based compaction scoring
17. **CompactionScoreCountWeight** Weight for count-based compaction scoring
18. **FlusherTickerInterval** Interval for flusher background process
19. **CompactorTickerInterval** Interval for compactor background process
20. **BloomFilterFPR** False positive rate for Bloom filters
21. **WalAppendRetry** Number of retries for WAL append operations
22. **WalAppendBackoff** Backoff duration for WAL append retries
23. **BlockManagerLRUEvictRatio** Ratio for LRU eviction. Determines what percentage of the cache to evict when cleanup is needed.
24. **BlockManagerLRUAccesWeight** Weight for LRU access eviction. Balances how much to prioritize access frequency vs. age when deciding what to evict.

### Simple Key-Value Operations
The easiest way to interact with Wildcat is through the Update method, which handles transactions automatically.
```go
// Write a value
err := db.Update(func(txn *wildcat.Txn) error {
    return txn.Put([]byte("hello"), []byte("world"))
})
if err != nil {
    // Handle error
}

// Read a value
var result []byte
err = db.Update(func(txn *wildcat.Txn) error {
    var err error
    result, err = txn.Get([]byte("hello"))
    return err
})

if err != nil {
    // Handle error
} else {
    fmt.Println("Value:", string(result)) // Outputs: Value: world
}
```

### Manual Transaction Management
For more complex operations, you can manually manage transactions.
```go
// Begin a transaction
txn := db.Begin()

// Perform operations
err := txn.Put([]byte("key1"), []byte("value1"))
if err != nil {
    txn.Rollback()
    // Handle error
}

value, err := txn.Get([]byte("key1"))
if err != nil {
    txn.Rollback()
    // Handle error
}

// Commit or rollback
err = txn.Commit()
if err != nil {
    txn.Rollback()
    // Handle commit error
}
```

### Iterating Keys
Wildcat provides comprehensive iteration capabilities with MVCC consistency.

> [!TIP]
> You can set ascending or descending order, and iterate over all keys, a range of keys, or keys with a specific prefix.

#### Full Iterator (bidirectional)
```go
err := db.View(func(txn *wildcat.Txn) error {
    // Create ascending iterator
    iter, err := txn.NewIterator(true)
    if err != nil {
        return err
    }

    // Iterate forward
    for {
        key, value, timestamp, ok := iter.Next()
        if !ok {
            break
        }

        fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", key, value, timestamp)
    }

    // Change direction and iterate backward
    err = iter.SetDirection(false)
    if err != nil {
        return err
    }

    for {
        key, value, timestamp, ok := iter.Next()
        if !ok {
            break
        }

        fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", key, value, timestamp)
    }

    return nil
})
```

#### Range Iterator (bidirectional)
```go
err := db.View(func(txn *wildcat.Txn) error {
    // Create range iterator
    iter, err := txn.NewRangeIterator([]byte("start"), []byte("end"), true)
    if err != nil {
        return err
    }

    // Iterate forward
    for {
        key, value, timestamp, ok := iter.Next()
        if !ok {
            break
        }

        fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", key, value, timestamp)
    }

    // Change direction and iterate backward
    err = iter.SetDirection(false)
    if err != nil {
        return err
    }

    for {
        key, value, timestamp, ok := iter.Next()
        if !ok {
            break
        }

        fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", key, value, timestamp)
    }

    return nil
})
```

#### Prefix Iterator (bidirectional)
```go
err := db.View(func(txn *wildcat.Txn) error {
    // Create prefix iterator
    iter, err := txn.NewPrefixIterator([]byte("prefix"), true)
    if err != nil {
        return err
    }

    // Iterate forward
    for {
        key, value, timestamp, ok := iter.Next()
        if !ok {
            break
        }

        fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", key, value, timestamp)
    }

    // Change direction and iterate backward
    err = iter.SetDirection(false)
    if err != nil {
        return err
    }

    for {
        key, value, timestamp, ok := iter.Next()
        if !ok {
            break
        }

        fmt.Printf("Key: %s, Value: %s, Timestamp: %d\n", key, value, timestamp)
    }

    return nil
})
```

### Read-Only Transactions with View
```go
// Read a value with View
var result []byte
err = db.View(func(txn *wildcat.Txn) error {
    var err error
    result, err = txn.Get([]byte("hello"))
    return err
})
```


### Batch Operations
You can perform multiple operations in a single transaction.
```go
err := db.Update(func(txn *wildcat.Txn) error {
    // Write multiple key-value pairs
    for i := 0; i < 1000; i++ {
        key := []byte(fmt.Sprintf("key%d", i))
        value := []byte(fmt.Sprintf("value%d", i))

        if err := txn.Put(key, value); err != nil {
            return err
        }
    }
    return nil
})
```

### Transaction Recovery
```go
// After reopening a database, you can access recovered transactions
txn, err := db.GetTxn(transactionID)
if err != nil {
    // Transaction not found or error
    return err
}

// Inspect the recovered transaction state
fmt.Printf("Transaction %d status: committed=%v\n", txn.Id, txn.Committed)
fmt.Printf("Write set: %v\n", txn.WriteSet)
fmt.Printf("Delete set: %v\n", txn.DeleteSet)

// You can commit or rollback the recovered transaction
if !txn.Committed {
    err = txn.Commit() // or txn.Rollback()
}
```

### Log Channel
Wildcat provides a log channel for real-time logging. You can set up a goroutine to listen for log messages.
```go
// Create a log channel
logChannel := make(chan string, 100) // Buffer size of 100 messages

// Set up options with the log channel
opts := &wildcat.Options{
    Directory:       "/path/to/db",
    LogChannel:      logChannel,
    // Other options...
}

// Open the database
db, err := wildcat.Open(opts)
if err != nil {
    // Handle error
}

wg := &sync.WaitGroup{}

wg.Add(1)

// Start a goroutine to listen to the log channel
go func() {
    defer wg.Done()
    for msg := range logChannel {
        // Process log messages
        fmt.Println("wildcat:", msg)

        // You could also write to a file, send to a logging service, etc.
        // log.Println(msg)
    }
}()

// Use..

wg.Wait() // Wait for the goroutine to finish

// When you're done, close the database
defer db.Close()
```

### Database Statistics
Wildcat provides comprehensive statistics about database state
```go
stats := db.Stats()
fmt.Println(stats)
```

**Output example**
```bash
┌───────────────────────────────────────────────────────────────────────────┐
│ Wildcat DB Stats and Configuration                                        │
├───────────────────────────────────────────────────────────────────────────┤
│ Write Buffer Size          : 25                                           │
│ Sync Option                : 1                                            │
│ Level Count                : 6                                            │
│ Bloom Filter Enabled       : false                                        │
│ Max Compaction Concurrency : 4                                            │
│ Compaction Cooldown        : 5s                                           │
│ Compaction Batch Size      : 8                                            │
│ Compaction Size Ratio      : 1.1                                          │
│ Compaction Threshold       : 8                                            │
│ Score Size Weight          : 0.8                                          │
│ Score Count Weight         : 0.2                                          │
│ Flusher Interval           : 1ms                                          │
│ Compactor Interval         : 250ms                                        │
│ Bloom FPR                  : 0.01                                         │
│ WAL Retry                  : 10                                           │
│ WAL Backoff                : 128µs                                        │
│ SSTable B-Tree Order       : 10                                           │
│ LRU Size                   : 1024                                         │
│ LRU Evict Ratio            : 0.2                                          │
│ LRU Access Weight          : 0.8                                          │
│ File Version               : 1                                            │
│ Magic Number               : 1464421444                                   │
│ Directory                  : /tmp/db_merge_iterator_large_test1776741552/ │
├───────────────────────────────────────────────────────────────────────────┤
│ ID Generator State                                                        │
├───────────────────────────────────────────────────────────────────────────┤
│ Last SST ID                : 0                                            │
│ Last WAL ID                : 0                                            │
│ Last TXN ID                : 0                                            │
├───────────────────────────────────────────────────────────────────────────┤
│ Runtime Statistics                                                        │
├───────────────────────────────────────────────────────────────────────────┤
│ Active Memtable Size       : 0                                            │
│ Active Memtable Entries    : 0                                            │
│ Active Transactions        : 20                                           │
│ Oldest Read Timestamp      : 0                                            │
│ WAL Files                  : 4                                            │
│ Total SSTables             : 5                                            │
│ Total Entries              : 18                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

This returns detailed information including
- Configuration settings and tuning parameters
- Active memtable size and entry count
- Transaction counts and oldest active read timestamp
- Level statistics and SSTable counts
- ID generator states and WAL file counts
- Compaction and flushing statistics

### Force Flushing
You can force a flush of all memtables to disk using the `Flush` method. This is useful for ensuring durability before performing critical operations.
```go
// Force all memtables to flush to SSTables
err := db.ForceFlush()
if err != nil {
    // Handle error
}
```

## Shared C Library
You will require the latest Go toolchain to build the shared C library for Wildcat. This allows you to use Wildcat as a C library in other languages.
```bash
go build -buildmode=c-shared -o libwildcat.so wildcat_c.go
```

### C API
```c
extern void* wildcat_open(wildcat_opts_t* opts);
extern void wildcat_close(void* ptr);
extern long int wildcat_begin_txn(void* ptr);
extern int wildcat_txn_put(long int txnId, char* key, char* val);
extern char* wildcat_txn_get(long int txnId, char* key);
extern int wildcat_txn_commit(long int txnId);
extern int wildcat_txn_rollback(long int txnId);
extern char* wildcat_stats(void* ptr);
extern int wildcat_force_flush(void* ptr);
extern int wildcat_txn_delete(long int txnId, char* key);
extern void wildcat_txn_free(long int txnId);
extern long unsigned int wildcat_txn_new_iterator(long int txnId, int asc);
extern long unsigned int wildcat_txn_new_range_iterator(long int txnId, char* start, char* end, int asc);
extern long unsigned int wildcat_txn_new_prefix_iterator(long int txnId, char* prefix, int asc);
extern int wildcat_txn_iterate_next(long unsigned int id);
extern int wildcat_txn_iterate_prev(long unsigned int id);
extern int wildcat_txn_iter_valid(long unsigned int id);
extern char* wildcat_iterator_key(long unsigned int id);
extern char* wildcat_iterator_value(long unsigned int id);
extern void wildcat_iterator_free(long unsigned int id);
```

## Overview

### MVCC Model
- Each key stores a timestamped version chain. The timestamps used are physical nanosecond timestamps (derived from `time.Now().UnixNano())`, providing a simple yet effective global ordering for versions.
- Transactions read the latest version ≤ their timestamp.
- Writes are buffered and atomically committed.
- Delete operations are recorded as tombstones.

### WAL and Durability
- Shared WAL per memtable; transactions append full state.
- WAL replay restores all committed and in-flight transactions.
- WALs rotate when memtables flush.

### Memtable Lifecycle
- Active memtable is swapped atomically when full.
- Immutable memtables are flushed in background.
- Skip list implementation with MVCC version chains for concurrent access.

### SSTables and Compaction
- Immutable SSTables are organized into levels.
- L1–L2 use size-tiered compaction.
- L3+ use leveled compaction by key range.
- Concurrent compaction with configurable maximum concurrency limits.
- Compaction score is calculated using a hybrid formula `score = (levelSize / capacity) * 0.7 + (sstableCount / threshold) * 0.3` compaction is triggered when score > 1.0
- Cooldown period enforced between compactions to prevent resource thrashing.
- Compaction filters out redundant tombstones based on timestamp and overlapping range.
- A tombstone is dropped if it's older than the oldest active read and no longer needed in higher levels.

### SSTable Metadata
Each SSTable tracks the following main meta details:
- Min and Max keys for fast range filtering
- EntryCount (total number of valid records) used for recreating filters if need be
- Size (approximate byte size) used for when reopening levels
- Optional BloomFilter for accelerated key lookups
- Level (mainly tells us during reopening and compaction)

We only list the main meta data but there is more for internal use.

### SSTable Format
SSTables are prefix and range optimized immutable BTree's.

Structure
- **KLog** `.klog` Contains BTree with key metadata and pointers to values
- **VLog** `.vlog` Contains actual value data in append-only format

During lookups
- **Range check** Min/Max key range validation (skipped if outside bounds)
- **Bloom filter** Consulted first if enabled (configurable false positive rate)
- **BTree search** Key lookup in KLog BTree structure
- **Value retrieval** Actual values retrieved from VLog using block IDs from KLog entries
- **MVCC filtering** Only versions visible to read timestamp are returned

### Compaction Policy / Strategy
- **L1-L2** Size-tiered compaction for similar-sized SSTables
- **L3+** Leveled compaction based on key range overlaps
- **Concurrent execution** Configurable max concurrency with priority-based job scheduling
- **Cooldown mechanism** Prevents thrashing with configurable cooldown periods

#### Compaction Scoring Formula
```
score = (levelSize / capacity) * sizeWeight + (sstableCount / threshold) * countWeight
```

- **Default weight** sizeWeight=0.8, countWeight=0.2
- **Trigger threshold** Compaction starts when score > 1.0
- **Priority-based** Higher scores get priority in the compaction queue

#### Compaction Process
- **Job scheduling** Background process evaluates all levels every CompactorTickerInterval
- **Priority queue** Jobs sorted by compaction score (highest first)
- **Concurrent execution** Up to MaxCompactionConcurrency jobs run simultaneously
- **Atomic operations** Source SSTables marked as merging, target level updated atomically
- **Cleanup** Old SSTable files removed after successful compaction

### Concurrency Model
- Wildcat uses lock-free structures where possible (e.g., atomic value swaps for memtables, atomic lru, queues, and more)
- Read and write operations are designed to be non-blocking.
- WAL appends are retried with backoff and allow concurrent writes.
- Flusher and Compactor run as independent goroutines, handling flushing and compaction in the background.
- Block manager uses per-file concurrency-safe(multi writer-reader) access and is integrated with LRU for lifecycle management. It leverages direct system calls (pread/pwrite) for efficient, non-blocking disk I/O.
- Writers never block; readers always see consistent, committed snapshots.
- No uncommitted state ever surfaces due to internal synchronization and timestamp-based visibility guarantees.

### Isolation Levels
Wildcat supports ACID-compliant isolation
- **Snapshot Isolation** Transactions see a consistent snapshot as of their start timestamp
- **Read Committed** Readers observe only committed versions, never intermediate uncommitted writes
- **No dirty reads** Timestamp-based MVCC prevents reading uncommitted data
- **Repeatable reads** Within a transaction, reads are consistent to the transaction's timestamp

### Recoverability Guarantee Order
Recovery process consists of several steps
- **WAL scanning** All WAL files are processed in chronological order (sorted by timestamp)
- **Transaction consolidation** Multiple WAL entries for same transaction ID are merged to final state
- **State restoration** Final transaction states are applied to memtables and transaction lists
- **Incomplete transaction preservation** Uncommitted transactions remain accessible via GetTxn(id)

#### Durability Order
- **WAL** All writes recorded atomically with full transaction details (ReadSet, WriteSet, DeleteSet, commit status)
- **Memtable** Writes reflected in memory immediately upon commit
- **SSTables** Memtables flushed to SSTables asynchronously via background flusher

#### Recovery Guarantees
- **Complete state restoration** All committed transactions are fully recovered
- **Incomplete transaction access** Uncommitted transactions can be inspected, committed, or rolled back
- **Timestamp consistency** WAL replay maintains correct timestamp ordering
- **Atomic recovery** Only transactions with durable WAL entries are considered recoverable

### Block Manager
Wildcat's block manager provides low-level, high-performance file I/O with sophisticated features.

#### Core
- **Direct I/O** Uses pread/pwrite system calls for atomic, position-independent operations
- **Block chaining** Supports multi-block data with automatic chain management
- **Free block management** Atomic queue-based allocation with block reuse
- **CRC verification** All blocks include CRC32 checksums for data integrity
- **Concurrent access** Thread-safe operations with lock-free design where possible

#### Block Structure
- **Header validation** Magic number and version verification
- **Block metadata** Size, next block ID, and CRC checksums
- **Chain support** Automatic handling of data spanning multiple blocks
- **Free space tracking** Intelligent free block scanning and allocation

#### Sync Options
- **SyncNone** No disk synchronization (fastest, least durable)
- **SyncFull** Synchronous writes with fdatasync (safest, slower)
- **SyncPartial** Background synchronization at configurable intervals (balanced)

#### Recovery Features
- **Block validation** CRC verification on all block reads
- **Chain reconstruction** Automatic detection and handling of multi-block data
- **Free space recovery** Intelligent scanning to rebuild free block allocation table

### LRU Cache
Wildcat uses a sophisticated lock-free LRU cache for block manager handles.

#### Advanced Features
- **Lock-free design** Atomic operations with lazy eviction
- **Anti-thrashing** Smart eviction with access pattern analysis
- **Load factor awareness** Only evicts when near capacity (95%+)
- **Emergency recovery** Automatic detection and recovery from stuck states
- **Node reuse** Efficient memory management with node recycling

#### Eviction Algorithm
```bash
evictionScore = accessWeight * accessCount + timeWeight * age
```
- **Balanced approach** Considers both access frequency and recency
- **Configurable weights** BlockManagerLRUAccesWeight controls the balance
- **Gradual eviction** Evicts BlockManagerLRUEvictRatio portion when needed
- **Emergency fallback** Aggressive cleanup when cache becomes unresponsive

#### Performance Optimizations
- **Batched processing** Processes eviction queue in controlled batches
- **Progress tracking** Monitors cache operations to detect performance issues
- **Concurrent-safe** Multiple threads can safely access cached resources
- **Resource cleanup** Automatic resource management with eviction callbacks

### Lock-Free Queue
- **Michael & Scott algorithm** Industry-standard lock-free queue design
- **ABA problem prevention** Proper pointer management and consistency checks
- **Memory ordering** Atomic operations ensure proper synchronization
- **High throughput** Supports millions of operations per second under contention

### BTree
Wildcat's BTree provides the foundation for SSTable key storage with advanced features for range queries and bidirectional iteration.

#### Core
- **Immutable design** Once written, BTrees are never modified, ensuring consistency
- **Configurable order** SSTableBTreeOrder controls node size and tree depth
- **Metadata storage** Supports arbitrary metadata attachment for SSTable information
- **Block-based storage** Integrates seamlessly with the block manager for efficient I/O

#### Advanced Features
- **Bidirectional iteration** Full support for forward and reverse traversal
- **Range queries** Efficient RangeIterator with start/end key bounds
- **Prefix iteration** Optimized PrefixIterator for prefix-based searches
- **Seek operations** Direct positioning to any key with Seek, SeekToFirst, SeekToLast
- **BSON serialization** Automatic serialization/deserialization of nodes and values

#### Iterator Capabilities
```go
// Multiple iterator types with consistent interface
iterator, err := btree.Iterator(ascending)                      // Full tree iteration
rangeIter, err := btree.RangeIterator(start, end, ascending)    // Range-bounded
prefixIter, err := btree.PrefixIterator(prefix, ascending)      // Prefix-based

// All iterators support
iter.Next()           // Advance in configured direction
iter.Prev()           // Move opposite to configured direction
iter.Seek(key)        // Position at specific key
iter.SetDirection()   // Change iteration direction
iter.Valid()          // Check if positioned at valid entry
```

#### Performance Optimizations
- **Lazy loading** Nodes loaded on-demand from block storage
- **Efficient splitting** Automatic node splitting maintains balance
- **Leaf linking** Direct leaf-to-leaf pointers for faster iteration
- **Memory efficient** Minimal memory footprint with block-based storage

#### Node Structure
- **Internal nodes** Store keys and child pointers for navigation
- **Leaf nodes** Store actual key-value pairs with next/prev links
- **BSON encoding** Consistent serialization format across all node types

### SkipList
Wildcat's SkipList serves as the core data structure for memtables, providing concurrent MVCC access with lock-free operations.

#### MVCC Architecture
- **Version chains** Each key maintains a linked list of timestamped versions
- **Timestamp ordering** Physical nanosecond timestamps ensure global ordering
- **Lock-free access** Atomic operations enable concurrent reads and writes
- **Snapshot isolation** Readers see consistent snapshots at their read timestamp

#### Concurrency Features
- **Non-blocking reads** Readers never block, even during concurrent writes
- **Atomic writes** CAS operations ensure thread-safe modifications
- **Version visibility** Timestamp-based filtering for MVCC consistency
- **Memory ordering** Proper atomic synchronization prevents race conditions

#### Version Management
```go
type ValueVersion struct {
    Data      []byte           // Actual value data
    Timestamp int64            // Version timestamp
    Type      ValueVersionType // Write or Delete marker
    Next      *ValueVersion    // Link to previous version
}
```

#### Advanced Operations
- **Put operations** Atomic insertion with version chaining
- **Delete operations** Tombstone markers with timestamp tracking
- **Get operations** Version-aware lookups with timestamp filtering
- **Range scanning** Efficient iteration over key ranges with MVCC consistency

**Iterator Types**
```go
// Multiple specialized iterators
iterator := skiplist.NewIterator(startKey, readTimestamp)
rangeIter := skiplist.NewRangeIterator(start, end, readTimestamp)
prefixIter := skiplist.NewPrefixIterator(prefix, readTimestamp)

// All support bidirectional traversal with MVCC semantics
iter.Next()    // Forward iteration with timestamp filtering
iter.Prev()    // Backward iteration with timestamp filtering
iter.Peek()    // Non-destructive current value inspection
```

#### Performance Characteristics
- **O(log n)** average case for all operations
- **Lock-free design** High concurrency with minimal contention
- **Memory efficient** Optimized node structure with atomic pointers
- **Skip probability** Configurable level distribution (p=0.25)
- **Maximum levels** Bounded height (MaxLevel=16) for predictable performance

#### MVCC Semantics
- **Read consistency** Transactions see stable snapshots throughout their lifetime
- **Write isolation** Concurrent writes don't interfere with each other
- **Timestamp ordering** Later timestamps always override earlier ones
- **Tombstone handling** Delete operations create versioned tombstone markers
- **Garbage collection** Old versions naturally filtered by timestamp-based reads

#### Memory Layout
- **Node structure** Atomic forward pointers array + backward pointer
- **Level generation** Probabilistic level assignment with geometric distribution
- **Version chains** Per-key linked lists with atomic head pointers

### Motivation
My name is Alex Gaetano Padula, and I've spent the past several years fully immersed in one thing.. databases and storage engines. Not frameworks. Not trends. Just the raw, unforgiving internals of how data lives, moves, and survives.

My journey began not with textbooks or formal training, but with pure curiosity. After writing my first database, CursusDB, to solve a real-time distributed problem, I couldn't stop experimenting. I dove into everything I could get my hands on.

I didn't just want to use a database.
I wanted to understand and build every layer of one, from disk to wire.

I'm not a database or storage expert - I'm just someone who loves building and tinkering. I've written everything from the simplest key-value stores to complex real-time distributed systems, learning through trial and error, late-night debugging sessions, and countless experiments.

Each engine taught me something new.
Each mistake deepened my understanding.
Each redesign brought me closer to fresh insights and ideas.

And that's where Wildcat came in.

I wanted to build a storage layer that didn't force you to choose between performance, safety, and usability. A system that embraced concurrency, durability, and simplicity, not as tradeoffs, but as first principles. Wildcat is my answer to the single-writer bottleneck - an MVCC, multi-writer, LSM-tree storage engine built in Go with a fully non-blocking transactional model.

Wildcat isn't just code I wrote. It's the distillation of every storage engine I've experimented with, every system I've torn down to understand, and every lesson I've learned through hands-on building.

It's a culmination of countless experiments, sleepless nights, LOTS of COFFEE, and a relentless curiosity about how things actually work under the hood.

### Contributing
You can contribute, no problem!  Find a bug, optimization, refactor and submit a PR.  It will be reviewed.  You're only helping us all :)

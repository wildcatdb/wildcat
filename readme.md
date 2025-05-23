<div>
    <h1 align="left"><img width="128" src="artwork/wildcat-logo.png"></h1>
</div>

![License](https://img.shields.io/badge/license-MPL_2.0-blue)


Wildcat is a high-performance embedded key-value database (or storage engine) written in Go. It incorporates modern database design principles including LSM (Log-Structured Merge) tree architecture, MVCC (Multi-Version Concurrency Control), and lock-free data structures for its critical paths, along with automatic background operations to deliver excellent read/write performance with strong consistency guarantees.

## Table of Contents
- [Features](#features)
- [Basic Usage](#basic-usage)
- [Version and Compatibility](#version-and-compatibility)
- [Advanced Configuration](#advanced-configuration)
- [Transaction Management](#manual-transaction-management)
- [Iteration](#iterating-keys)
- [Architecture Overview](#architecture-overview)
- [Log Channel](#log-channel)
- [MVCC Model](#mvcc-model)
- [WAL and Durability](#wal-and-durability)
- [Concurrency Model](#concurrency-model)
- [Recoverability](#recoverability-guarantee-order)
- [Isolation Levels](#isolation-levels)
- [SSTable Format](#sstable-format)
- [SSTable Metadata](#sstable-metadata)
- [Compaction](#sstables-and-compaction)
- [Motivation](#motivation)
- [Contributing](#contributing)

## Features
- LSM (Log-Structured Merge) tree architecture optimized for high write throughput
- Minimal to no blocking concurrency for readers and writers
- WAL logging captures full transaction state for recovery and rehydration
- Version-aware skip list for fast in-memory MVCC access
- Atomic write path, safe for multithreaded use
- Scalable design with background flusher and compactor
- Durable and concurrent block storage, leveraging direct, offset-based file I/O (using `pread`/`pwrite`) for optimal performance and control
- Atomic LRU for active block manager handles
- Memtable lifecycle management and snapshot durability
- Configurable Sync options such as `None`, `Partial (with background interval)`, `Full`
- Snapshot-isolated MVCC with read timestamps
- Crash recovery restores all in-flight and committed transactions
- Automatic multi-threaded background compaction
- Full ACID transaction support
- Bidirectional iteration with MVCC-consistent visibility
- Sustains 100K+ txns/sec writes, and hundreds of thousands of reads/sec
- Optional Bloom filter per SSTable for fast key lookups
- Key value separation optimization (`.klog` for keys, `.vlog` for values, klog entries point to vlog entries)
- Tombstone-aware compaction with retention based on active transaction windows

## Version and Compatibility
- Go 1.24+
- Linux/macOS/Windows (64-bit)

## Basic Usage
Wildcat supports opening multiple `wildcat.DB` instances in parallel, each operating independently in separate directories.

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
    Directory:           "/path/to/database",     // Directory for database files
    WriteBufferSize:     32 * 1024 * 1024,        // 32MB memtable size
    SyncOption:          wildcat.SyncFull,        // Full sync for maximum durability
    SyncInterval:        128 * time.Millisecond,  // Only set when using SyncPartial, can be 0 otherwise
    LevelCount:          7,                       // Number of LSM levels
    LevelMultiplier:     10,                      // Size multiplier between levels
    BlockManagerLRUSize: 256,                     // Cache size for block managers
    BlockSetSize:        8 * 1024 * 1024,         // 8MB block set size, each klog block will have BlockSetSize of entries
    LogChannel:          make(chan string, 1000), // Channel for real time logging
    BloomFilter:         false,                   // Enable/disable sstable bloom filters
}
```

#### Configuration Options Explained
1. **Directory** The path where the database files will be stored
2. **WriteBufferSize** Size threshold for memtable before flushing to disk
3. **SyncOption** Controls durability vs performance tradeoff:
    - **SyncNone** Fastest, but no durability guarantees
    - **SyncPartial** Balances performance and durability
    - **SyncFull** Maximum durability, slower performance
4. **SyncInterval** Time between background sync operations
5. **LevelCount** Number of levels in the LSM tree
6. **LevelMultiplier** Size ratio between adjacent levels
7. **BlockManagerLRUSize** Number of block managers to cache
8. **BlockSetSize** Size of SSTable klog block sets
9. **LogChannel** Channel for real-time logging, useful for debugging and monitoring
10. **BloomFilter** Enable or disable bloom filters for SSTables to speed up key lookups

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
Wildcat supports MVCC-consistent, bidirectional iteration. You iterate over keys and their values in order, no duplicates, and consistent!
```go
txn := db.Begin()
iter := txn.NewIterator(nil, nil)

for {
    key, val, ts, ok := iter.Next()
    if !ok {
        break
    }
    fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, val, ts)
}

for {
    key, val, ts, ok := iter.Prev()
    if !ok {
        break
    }
    fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, val, ts)
}
```

Or using a transaction block.

```go
db.Update(func(txn *wildcat.Txn) error {
    iter := txn.NewIterator(nil, nil)
    for {
        key, val, ts, ok := iter.Next()
        if !ok {
            break
        }
        fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, val, ts)
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

### Prefix Iteration with Snapshot Isolation
```go
db.View(func(txn *wildcat.Txn) error {
    iter := txn.NewIterator(nil, []byte("user:"))

    for {
        key, val, ts, ok := iter.Next()
        if !ok {
            break
        }
        fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, val, ts)
    }

    return nil
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

## Log Channel
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

## Architecture Overview

### MVCC Model
- Each key stores a timestamped version chain. The timestamps used are physical nanosecond timestamps (derived `from time.Now().UnixNano())`, providing a simple yet effective global ordering for versions.
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

### SSTables and Compaction
- Immutable SSTables are organized into levels.
- L1–L2 use size-tiered compaction.
- L3+ use leveled compaction by key range.
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
SSTables are append-only and composed of
- **Block 0** BSON-encoded metadata block containing structural fields (Min, Max, EntryCount, etc.)
- **Block 1..N** Serialized BlockSet entries containing sorted key/value pairs

During lookups
- The Bloom filter is consulted first (if enabled)
- Then Min/Max key range is checked
- If eligible, the scan proceeds over BlockSet entries until the key is found
- Actual values are retrieved from the VLog by block id

### Concurrency Model
- Wildcat uses lock-free structures where possible (e.g., atomic value swaps for memtables, atomic lru, queues, and more)
- Read and write operations are designed to be non-blocking.
- WAL appends are retried with backoff and allow concurrent writes.
- Flusher and Compactor run as independent goroutines, handling flushing and compaction in the background.
- Block manager uses per-file concurrency-safe(multi writer-reader) access and is integrated with LRU for lifecycle management. It leverages direct system calls (pread/pwrite) for efficient, non-blocking disk I/O.
- Writers never block; readers always see consistent, committed snapshots.
- No uncommitted state ever surfaces due to internal synchronization and timestamp-based visibility guarantees.

### Isolation Levels
Wildcat supports the following transactional isolation levels:
- **Snapshot Isolation** Transactions see a consistent snapshot of the database as of their start timestamp.
- **Read Committed (implicit)** Readers only observe committed versions but do not see intermediate uncommitted writes.

### Recoverability Guarantee Order
- **WAL** All writes are first recorded to WAL atomically with full transaction details, including the transaction ID, ReadSet, WriteSet, and DeleteSet.
- **Memtable** Writes are reflected in the in-memory memtable immediately upon commit.
- **SSTables** Memtables are flushed to SSTables asynchronously via the flusher.

On crash
- Wildcat rehydrates all transactions from the WAL, including active but uncommitted transactions, and reinstates their state in memory.
- Recovered transactions can be accessed via `GetTxn(id)` for inspection, conflict resolution, or auditing.
- The WAL is replayed in timestamp order, ensuring that the in-memory structures reflect a correct, consistent view of the last known good state.
- Only transactions with durable WAL entries are considered recoverable.

This design guarantees durability, atomicity, and the ability to inspect past transactional state under failure recovery conditions.
- WAL is replayed fully before memtable/SSTables are accessed.
- Guarantees durability and atomicity for all committed transactions.

## Motivation
```
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
```

## Contributing
You can contribute, no problem!  Find a bug, optimization, refactor and submit a PR.  It will be reviewed.  You're only helping us all :)
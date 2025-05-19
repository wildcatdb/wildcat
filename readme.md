<div>
    <h1 align="left"><img width="128" src="artwork/wildcat-logo.png"></h1>
</div>

Wildcat is a high-performance embedded key-value database or you can also call it a storage engine written in Go. It incorporates modern database design principles including LSM tree architecture, MVCC (Multi-Version Concurrency Control), and automatic background operations to deliver excellent read/write performance with strong consistency guarantees.

## Features
- LSMT(log-structure-merged-tree) architecture optimized for high write throughput
- Minimal to no blocking concurrency for readers and writers
- Per-transaction WAL logging with recovery and rehydration
- Version-aware skip list for fast in-memory MVCC access
- Atomic write path, safe for multithreaded use
- Scalable design with background flusher and compactor
- Durable and concurrent block storage
- Atomic LRU for active block manager handles
- Memtable lifecycle management and snapshot durability
- Configurable Sync options, `None`, `Partial (w/ background interval)`, `Full`
- MVCC with snapshot isolation (with read timestamp)
- WALs ensure durability and crash recovery of state, including active transactions
- Automatic multi-threaded background compaction that maintains optimal performance over time
- Full ACID transaction support
- Bidirectional iteration for efficient data scanning
- Can sustain write throughput at 100K+ txns/sec
- Handles hundreds of thousands of concurrent read ops/sec with default settings
- Bloom filter per sstable for fast key lookups


## Basic Usage

### Opening a Database
```go
// Create default options
opts := &wildcat.Options{
Directory: "/path/to/database",
    // Use defaults for other settings
}

// Open or create the database
db, err := wildcat.Open(opts)
if err != nil {
    log.Fatalf("Failed to open database: %v", err)
}
defer db.Close()
```

### Simple Key-Value Operations
The easiest way to interact with Wildcat is through the Update method, which handles transactions automatically.
```go
// Write a value
err := db.Update(func(txn *wildcat.Txn) error {
    return txn.Put([]byte("hello"), []byte("world"))
})
if err != nil {
    log.Fatalf("Failed to write: %v", err)
}

// Read a value
var result []byte
err = db.Update(func(txn *wildcat.Txn) error {
    var err error
    result, err = txn.Get([]byte("hello"))
    return err
})

if err != nil {
    log.Fatalf("Failed to read: %v", err)
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
    log.Fatal(err)
}

value, err := txn.Get([]byte("key1"))
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

// Commit or rollback
err = txn.Commit()
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}
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
if err != nil {
    log.Fatalf("Failed to write batch: %v", err)
}
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

## Iterating Keys
Wildcat supports bidirectional, MVCC-consistent iteration within a transaction using `txn.NewIterator(startKey, prefix)`.
```go
// Begin a transaction
txn := db.Begin()

// Create an iterator from a given start key (or nil to begin from the start)
iter := txn.NewIterator(nil, nil)

// Forward iteration
fmt.Println("Forward scan:")
for {
    key, val, ts, ok := iter.Next()
    if !ok {
        break
    }
    fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, val, ts)
}

// Reverse iteration
fmt.Println("Reverse scan:")
for {
    key, val, ts, ok := iter.Prev()
    if !ok {
        break
    }
    fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, val, ts)
}
```

OR

```go
err := db.Update(func(txn *wildcat.Txn) error {
    iter := txn.NewIterator(nil, nil) // Full forward scan

    fmt.Println("Keys in snapshot:")
    for {
        key, value, ts, ok := iter.Next()
        if !ok {
            break
        }
        fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, value, ts)
    }

    return nil
})
if err != nil {
    log.Fatalf("Iteration failed: %v", err)
}
```

### Read-Only Transactions with View
Wildcat provides a `View` method specifically designed for read-only operations. This is more efficient than using `Update` when you only need to read data.

```go
// Read a value with View
var result []byte
err = db.View(func(txn *wildcat.Txn) error {
    var err error
    result, err = txn.Get([]byte("hello"))
    return err
})

if err != nil {
    log.Fatalf("Failed to read: %v", err)
} else {
    fmt.Println("Value:", string(result)) // Outputs: Value: world
}
```

// Iterate through keys in read-only mode
err := db.View(func(txn *wildcat.Txn) error {
    iter := txn.NewIterator(nil, nil)

    for {
        key, value, ts, ok := iter.Next()
        if !ok {
            break
        }
        fmt.Printf("Key=%s Value=%s Timestamp=%d\n", key, value, ts)
    }

    return nil
})
if err != nil {
    log.Fatalf("Iteration failed: %v", err)
}

## Advanced Configuration
Wildcat provides several configuration options for fine-tuning.
```go
opts := &wildcat.Options{
    Directory:           "/path/to/database",
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

### Configuration Options Explained
2. **Directory** The path where the database files will be stored
3. **WriteBufferSize** Size threshold for memtable before flushing to disk
4. **SyncOption** Controls durability vs performance tradeoff:
    - **SyncNone** Fastest, but no durability guarantees
    - **SyncPartial** Balances performance and durability
    - **SyncFull** Maximum durability, slower performance
5. **SyncInterval** Time between background sync operations
6. **LevelCount** Number of levels in the LSM tree
7. **LevelMultiplier** Size ratio between adjacent levels
8. **BlockManagerLRUSize** Number of block managers to cache
9. **BlockSetSize** Size of SSTable klog block sets
10. **LogChannel** Channel for real-time logging, useful for debugging and monitoring
11. **BloomFilter** Enable or disable bloom filters for SSTables to speed up key lookups

## Implementation Details
### Data Storage Architecture
Wildcat uses a multi-level Log-Structured Merge (LSM) tree architecture.

- **Memtable** In-memory skiplist for recent writes (L0)
- **Immutable Memtables** Memtables awaiting flush to disk
- **SSTables** On-disk sorted string tables organized into levels
- **Write-Ahead Log (WAL)** Ensures durability of in-memory data

### Compaction Strategy
Wildcat employs a hybrid compaction strategy:
- **Size-Tiered Compaction (L1–L2)** Merges similarly sized SSTables to reduce write amplification and flush pressure. This strategy groups SSTables of comparable size and merges them into the next level. Ideal for absorbing high write throughput efficiently.
- **Leveled Compaction (L3 and above)** Maintains disjoint key ranges within each level to optimize lookup performance. Overlapping SSTables from the lower level are merged into the appropriate ranges in the higher level, minimizing read amplification and ensuring predictable query cost.

**Compaction is triggered when**
1. A level size exceeds capacity
2. Number of sstables exceeds threshold (CompactionSizeThreshold)
3. Compaction score exceeds 1.0 `score = sizeScore * WeightSize + countScore * WeightCount` default:
    - CompactionScoreSizeWeight = 0.7
    - CompactionScoreCountWeight = 0.3
4. Cooldown period passed (CompactionCooldownPeriod)


### MVCC and Transactions
Wildcat uses a snapshot-isolated MVCC (Multi-Version Concurrency Control) model to provide full ACID-compliant transactions with zero reader/writer blocking and high concurrency.

Each transaction is assigned a unique, monotonic timestamp at creation time. This timestamp determines the visibility window of data for the entire duration of the transaction.

### MVCC Model
- Each key stores a chain of timestamped versions (latest → oldest).
- Reads only see the latest version ≤ transaction timestamp.
- Writes are buffered in a per-transaction WriteSet and flushed atomically on commit.
- Deletions are tracked via timestamped tombstones in a DeleteSet.

### Snapshot Isolation
- All reads during a transaction reflect a consistent snapshot of the database as of the transaction's start time.
- Writers can proceed concurrently without locking; they do not overwrite but create a new version.

### WAL Durability
- Transactions are logged to a write-ahead log (WAL) before commit, capturing the full WriteSet, DeleteSet, and ReadSet.
- WALs are replayed on crash to restore in-flight or recently committed transactions.

### Conflict Handling
Wildcat uses an optimistic concurrency model.
- Write-write conflicts are resolved by timestamp ordering - later transactions take precedence.
- No explicit read/write conflict detection.
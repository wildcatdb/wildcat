## OrinDB
OrinDB is a high-performance embedded key-value database written in Go. It incorporates modern database design principles including LSM tree architecture, MVCC (Multi-Version Concurrency Control), and automatic background operations to deliver excellent read/write performance with strong consistency guarantees.

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
- Configurable Sync options, None, Partial (background interval), Full
- MVCC with snapshot isolation (with read timestamp)
- WALs ensure durability and crash recovery of state, including active transactions
- Automatic multi-threaded background compaction that maintains optimal performance over time
- Full ACID transaction support
- Bidirectional iteration for efficient data scanning
- Can sustain write throughput at 100K+ txns/sec
- Handles hundreds of thousands of concurrent read ops/sec with default settings

## Todo
- Possible bloom filter for member queries

## Basic Usage

### Opening a Database
```go
// Create default options
opts := &orindb.Options{
Directory: "/path/to/database",
    // Use defaults for other settings
}

// Open or create the database
db, err := orindb.Open(opts)
if err != nil {
    log.Fatalf("Failed to open database: %v", err)
}
defer db.Close()
```

### Simple Key-Value Operations
The easiest way to interact with OrinDB is through the Update method, which handles transactions automatically.
```go
// Write a value
err := db.Update(func(txn *orindb.Txn) error {
    return txn.Put([]byte("hello"), []byte("world"))
})
if err != nil {
    log.Fatalf("Failed to write: %v", err)
}

// Read a value
var result []byte
err = db.Update(func(txn *orindb.Txn) error {
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
err := db.Update(func(txn *orindb.Txn) error {
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
OrinDB provides a log channel for real-time logging. You can set up a goroutine to listen for log messages.
```go
// Create a log channel
logChannel := make(chan string, 100) // Buffer size of 100 messages

// Set up options with the log channel
opts := &orindb.Options{
    Directory:       "/path/to/db",
    LogChannel:      logChannel,
    // Other options...
}

// Open the database
db, err := orindb.Open(opts)
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
        fmt.Println("OrinDB Log:", msg)

        // You could also write to a file, send to a logging service, etc.
        // log.Println(msg)
    }
}()

// Use..

wg.Wait() // Wait for the goroutine to finish

// When you're done, close the database
defer db.Close()
```

## Advanced Configuration
OrinDB provides several configuration options for fine-tuning.
```go
opts := &orindb.Options{
    Directory:           "/path/to/database",
    WriteBufferSize:     32 * 1024 * 1024,        // 32MB memtable size
    SyncOption:          orindb.SyncFull,         // Full sync for maximum durability
    SyncInterval:        128 * time.Millisecond,  // Only set when using SyncPartial, can be 0 otherwise
    LevelCount:          7,                       // Number of LSM levels
    LevelMultiplier:     10,                      // Size multiplier between levels
    BlockManagerLRUSize: 256,                     // Cache size for block managers
    BlockSetSize:        8 * 1024 * 1024,         // 8MB block set size, each klog block will have BlockSetSize of entries
    LogChannel:          make(chan string, 1000), // Channel for real time logging
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

## Implementation Details
### Data Storage Architecture
OrinDB uses a multi-level Log-Structured Merge (LSM) tree architecture.

- **Memtable** In-memory skiplist for recent writes (L0)
- **Immutable Memtables** Memtables awaiting flush to disk
- **SSTables** On-disk sorted string tables organized into levels
- **Write-Ahead Log (WAL)** Ensures durability of in-memory data

### Compaction Strategy
OrinDB employs a hybrid compaction strategy:
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
All operations in OrinDB use Multi-Version Concurrency Control.

- Each write has an associated timestamp
- Reads use a consistent snapshot timestamp
- Transactions provide ACID guarantees
- Conflicts are detected using read and write sets

## OrinDB
OrinDB is a high-performance embedded key-value database written in Go. It incorporates modern database design principles including LSM tree architecture, MVCC (Multi-Version Concurrency Control), and automatic compaction to deliver excellent read/write performance with strong consistency guarantees.

## Features
- LSM Tree Architecture optimized for high write throughput
- Full non-blocking concurrency for readers and writers
- Per-transaction WAL logging with recovery and rehydration
- Version-aware skip list for fast in-memory MVCC access
- Atomic write path, safe for multi-threaded use
- Scalable design with background flushers and compactors
- Durable and concurrent block storage
- Atomic LRU for active block manager handles
- Memtable lifecycle management and snapshot durability
- Configurable Sync options, None, Partial (background interval), Full
- MVCC with snapshot isolation (with read timestamp)
- WAL Ensures durability and crash recovery
- Automatic background compaction that maintains optimal performance over time
- Full ACID transaction support
- Bidirectional iteration for efficient data scanning

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

### Scanning Data
OrinDB provides rich iteration capabilities.
```go
// Iterate through all entries
err := db.Update(func(txn *orindb.Txn) error {
    return txn.ForEach(func(key, value []byte) bool {
        fmt.Printf("Key: %s, Value: %s\n", key, value)
        return true // Continue iteration
    })
})

// Range scan
err := db.Update(func(txn *orindb.Txn) error {
    startKey := []byte("key100")
    endKey := []byte("key200")

    return txn.Scan(startKey, endKey, func(key, value []byte) bool {
        fmt.Printf("Key: %s, Value: %s\n", key, value)
        return true // Continue scanning
    })
})
```

### Bidirectional Iteration
For more complex iteration needs, you can use the low-level iterator API.
```go
err := db.Update(func(txn *orindb.Txn) error {
    // Get an iterator starting at a specific key
    iter := txn.NewIterator([]byte("key500"))

    // Forward iteration
    for {
        key, value, ok := iter.Next()
        if !ok {
            break // No more entries
        }
        fmt.Printf("Forward - Key: %s, Value: %s\n", key, value.([]byte))
    }

    // Reset iterator and go backward
    iter = txn.NewIterator([]byte("key500"))

    for {
        key, value, ok := iter.Prev()
        if !ok {
            break // No more entries
        }
        fmt.Printf("Backward - Key: %s, Value: %s\n", key, value.([]byte))
    }

    return nil
})
if err != nil {
    log.Fatalf("Failed to iterate: %v", err)
}
```

## Advanced Configuration
OrinDB provides several configuration options for fine-tuning.
```go
opts := &orindb.Options{
    Directory:           "/path/to/database",
    WriteBufferSize:     32 * 1024 * 1024, // 32MB memtable size
    SyncOption:          orindb.SyncFull,  // Full sync for maximum durability
    SyncInterval:        500 * time.Millisecond,
    LevelCount:          7,                // Number of LSM levels
    LevelMultiplier:     10,               // Size multiplier between levels
    BlockManagerLRUSize: 256,              // Cache size for block managers
    BlockSetSize:        4 * 1024 * 1024,  // 4MB block set size, each klog block will have BlockSetSize of entries
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

## Implementation Details
### Data Storage Architecture
OrinDB uses a multi-level Log-Structured Merge (LSM) tree architecture.

- **Memtable** In-memory skiplist for recent writes
- **Immutable Memtables** Memtables awaiting flush to disk
- **SSTables** On-disk sorted string tables organized into levels
- **Write-Ahead Log (WAL)** Ensures durability of in-memory data

### Compaction Strategy
OrinDB employs a hybrid compaction strategy:
- **Size-Tiered Compaction** For lower levels (L0-L2), merges similarly sized SSTables
- **Leveled Compaction** For higher levels (L3+), maintains key ranges for efficient lookups

**Compaction is triggered when**
- A level exceeds its capacity threshold
- A level contains too many files

### MVCC and Transactions
All operations in OrinDB use Multi-Version Concurrency Control.

- Each write has an associated timestamp
- Reads use a consistent snapshot timestamp
- Transactions provide ACID guarantees
- Conflicts are detected using read and write sets

### Performance Considerations
For optimal performance, consider the following.
- **Choose appropriate WriteBufferSize** Larger values improve write throughput but increase memory usage
- **Tune SyncOption** Use SyncNone for maximum speed, SyncFull for maximum durability
- **Adjust LevelCount and LevelMultiplier** Impact space amplification and read performance
- **Use batch operations** Group multiple writes in a single transaction
- **Consider key design** Keys with similar prefixes may be stored closer together
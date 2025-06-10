<div>
    <h1 align="left"><img width="128" src="artwork/wildcat-logo.png"></h1>
</div>

![License](https://img.shields.io/badge/license-MPL_2.0-blue)

Wildcat is a high-performance embedded key-value database (or storage engine) written in Go with C interoptibility. It incorporates modern database design principles including LSM (Log-Structured Merge) tree architecture, MVCC (Multi-Version Concurrency Control), and lock-free data structures for its critical paths, along with automatic background operations to deliver excellent read/write performance with immediate consistency and durability.

## Features
- LSM (Log-Structured Merge) tree architecture optimized for write-heavy workloads
- Mostly lock-free MVCC with minimal blocking on critical paths
- WAL logging captures full transaction state for recovery and rehydration
- Version-aware skip list for fast in-memory MVCC access
- Thread-safe write operations with atomic coordination
- Scalable design with background flusher and compactor
- Concurrent block storage leveraging direct, offset-based file I/O (using `pread`/`pwrite`) for optimal performance
- Atomic LRU cache for active block manager handles
- Memtable lifecycle management
- SSTables stored as immutable BTrees
- Configurable durability levels `None` (fastest), `Partial` (balanced), `Full` (most durable)
- Snapshot-isolated MVCC with timestamp-based reads
- Crash recovery preserves committed transactions and maintains access to incomplete transactions
- Automatic multi-threaded background compaction with configurable concurrency
- ACID transaction support with configurable durability guarantees
- Range, prefix, and full iteration support with bidirectional traversal
- High transactional throughput per second with low latency due to lock-free and non-blocking design.
- Optional Bloom filters per SSTable for improved key lookup performance
- Key-value separation optimization (`.klog` for keys, `.vlog` for values)
- Tombstone-aware compaction with retention based on active transaction windows
- Transaction recovery preserves incomplete transactions for post-crash inspection and resolution if db configured with `RecoverUncommittedTxns`
- Keys and values stored as opaque byte sequences
- Single-node embedded storage engine with no network or replication overhead

## Discord Community
Join our Discord community to discuss development, design, ask questions, and get help with Wildcat.

[![Discord](https://img.shields.io/discord/1380406674216587294?label=Discord&logo=discord&color=EDB73B)](https://discord.gg/Rs5Z2e69ts)

## Table of Contents
- [Version and Compatibility](#version-and-compatibility)
- [Basic Usage](#basic-usage)
  - [Opening a Wildcat DB instance](#opening-a-wildcat-db-instance)
  - [Simple Key-Value Operations](#simple-key-value-operations)
  - [Manual Transaction Management](#manual-transaction-management)
  - [Read-Only Transactions with View](#read-only-transactions-with-view)
  - [Batch Operations](#batch-operations)
  - [Iterating Keys](#iterating-keys)
    - [Full Iterator (bidirectional)](#full-iterator-bidirectional)
    - [Range Iterator (bidirectional)](#range-iterator-bidirectional)
    - [Prefix Iterator (bidirectional)](#prefix-iterator-bidirectional)
  - [Transaction Recovery](#transaction-recovery)
  - [Log Channel](#log-channel)
  - [Database Statistics](#database-statistics)
  - [Force Flushing](#force-flushing)
  - [Escalate Sync](#escalate-sync)
  - [Advanced Configuration](#advanced-configuration)
- [Shared C Library](#shared-c-library)
- [Contributing](#contributing)

## Samples
You can find sample programs [here](https://github.com/wildcatdb/samples)

## Benchmarks
You can find the benchmarking tool [here](https://github.com/wildcatdb/bench)

## Wiki
You can read about Wildcat's architecture at the [wiki](https://github.com/wildcatdb/wildcat/wiki).

## Version and Compatibility
- Go 1.24+
- Linux/macOS/Windows (64-bit)

## Basic Usage
Wildcat supports opening multiple `*wildcat.DB` instances in parallel, each operating independently in separate directories.

### Downloading
```bash
go get github.com/wildcatdb/wildcat/v2
```

Specific major version i.e `v2.x.x` can be downloaded using
```bash
go get github.com/wildcatdb/wildcat/v2@v2.1.4
```

### Import
```go
import (
    "github.com/wildcatdb/wildcat/v2"
)
```

When importing different majors you can do
```go
// v1 (not recommended, use v2+)
import (
    "github.com/wildcatdb/wildcat"
)
```

```go
// v2
import (
    "github.com/wildcatdb/wildcat/v2"
)
```

```go
// v2+
import (
    "github.com/wildcatdb/wildcat/v32"
)
```

### Opening a Wildcat DB instance
The only required option is the database directory path.
```go
// Create default options
opts := &wildcat.Options{
    Directory: "/path/to/db",
    // You don't need to set all options only Directory is required!
}

// Open or create a new Wildcat DB instance
db, err := wildcat.Open(opts) // Returns *wildcat.DB
if err != nil {
    // Handle error
}
defer db.Close()
```

### Simple Key-Value Operations
The easiest way to interact with Wildcat is through the Update method, which handles transactions automatically.  This means it runs begin, commit, and rollback for you, allowing you to focus on the operations themselves.
```go
// Write a value
err := db.Update(func(txn *wildcat.Txn) error {
    return txn.Put([]byte("hello"), []byte("world")) // Put update's existing key's values.
})
if err != nil {
    // Handle error
}

// Read a value
var result []byte
err = db.View(func(txn *wildcat.Txn) error {
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
txn, err := db.Begin()
if err != nil {
    // Handle error
    return
}

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

> [!CAUTION]
> Batch operations on the Wildcat engine are slower completed inside an `Update`.  It's better to use Begin->Put flow for batch writes.
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

**OR**
```go
// Perform batch operations
for i := 0; i < 1000; i++ {
    // Begin a transaction
    txn, err := db.Begin()
    if err != nil {
        // Handle error
        return
    }

    key := []byte(fmt.Sprintf("key%d", i))
    value := []byte(fmt.Sprintf("value%d", i))

    if err := txn.Put(key, value); err != nil {
        txn.Rollback()
        // Handle error
        return
    }

    // Commit the transaction
    err = txn.Commit()
    if err != nil {
        // Handle error
        return
    }
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
┌──────────────────────────────────────────────────────────────┐
│ Wildcat DB Stats and Configuration                           │
├──────────────────────────────────────────────────────────────┤
│ Write Buffer Size          : 33554432                        │
│ Sync Option                : 2                               │
│ Level Count                : 6                               │
│ Bloom Filter Enabled       : false                           │
│ Max Compaction Concurrency : 2                               │
│ Compaction Cooldown        : 3s                              │
│ Compaction Batch Size      : 8                               │
│ Compaction Size Ratio      : 1.1                             │
│ Compaction Threshold       : 8                               │
│ Score Size Weight          : 0.8                             │
│ Score Count Weight         : 0.2                             │
│ Flusher Interval           : 1ms                             │
│ Compactor Interval         : 250ms                           │
│ Bloom FPR                  : 0.01                            │
│ WAL Retry                  : 10                              │
│ WAL Backoff                : 128µs                           │
│ SSTable B-Tree Order       : 16                              │
│ LRU Size                   : 1024                            │
│ LRU Evict Ratio            : 0.2                             │
│ LRU Access Weight          : 0.8                             │
│ File Version               : 2                               │
│ Magic Number               : 1464421444                      │
│ Directory                  : /tmp/wildcat_stats_example/     │
├──────────────────────────────────────────────────────────────┤
│ ID Generator State                                           │
├──────────────────────────────────────────────────────────────┤
│ Last SST ID                : 1                               │
│ Last WAL ID                : 1                               │
│ Last TXN ID                : 22                              │
├──────────────────────────────────────────────────────────────┤
│ Runtime Statistics                                           │
├──────────────────────────────────────────────────────────────┤
│ Active Memtable Size       : 15479                           │
│ Active Memtable Entries    : 126                             │
│ Active Transactions        : 0                               │
│ Oldest Read Timestamp      : 1749434695512449925             │
│ WAL Files                  : 0                               │
│ Total SSTables             : 1                               │
│ Total Entries              : 249                             │
└──────────────────────────────────────────────────────────────┘
```

This returns detailed information including
- Configuration settings and tuning parameters
- Active memtable size and entry count
- Transaction counts and oldest active read timestamp
- Level statistics and SSTable counts
- ID generator states and WAL file counts
- Compaction and flushing statistics

### Force Flushing
You can force a flush of current and immutable memtables in queue to disk using the `Flush` method.
```go
// Force all memtables to flush to SSTables
err := db.ForceFlush()
if err != nil {
    // Handle error
}
```

### Escalate Sync
If you have your sync option set to `SyncNone` and would like to control when the block manager syncs a WAL to disk, you can use the `*DB.Sync()` which syncs the current WAL to disk.
```go
// Escalate sync to ensure current WAL is written to disk
err := db.Sync()
if err != nil {
    // Handle error
}
```

### Advanced Configuration
Wildcat provides many configuration options for fine-tuning.

| Parameter | Example Value | Description |
|-----------|---------------|-------------|
| **Directory** | `"/path/to/database"` | The path where the database files will be stored |
| **WriteBufferSize** | `64 * 1024 * 1024` | Size threshold for memtable before flushing to disk |
| **SyncOption** | `wildcat.SyncFull` | Controls durability vs performance tradeoff |
| **SyncNone** | - | Fastest, but no durability guarantees |
| **SyncPartial** | - | Balances performance and durability |
| **SyncFull** | - | Maximum durability, slower performance |
| **SyncInterval** | `128 * time.Millisecond` | Time between background sync operations (only for SyncPartial) |
| **LevelCount** | `7` | Number of levels in the LSM tree |
| **LevelMultiplier** | `10` | Size ratio between adjacent levels |
| **BlockManagerLRUSize** | `1024` | Number of block managers to cache |
| **SSTableBTreeOrder** | `10` | Size of SSTable klog block sets |
| **LogChannel** | `make(chan string, 1000)` | Channel for real-time logging, useful for debugging and monitoring |
| **BloomFilter** | `false` | Enable or disable bloom filters for SSTables to speed up key lookups. Bloom filters use double hashing with FNV-1a and FNV hash functions. Is automatically sized based on expected items and desired false positive rate. |
| **MaxCompactionConcurrency** | `4` | Maximum number of concurrent compactions |
| **CompactionCooldownPeriod** | `5 * time.Second` | Cooldown period between compactions to prevent thrashing |
| **CompactionBatchSize** | `8` | Max number of SSTables to compact at once |
| **CompactionSizeRatio** | `1.1` | Level size ratio that triggers compaction |
| **CompactionSizeThreshold** | `8` | Number of files to trigger size-tiered compaction |
| **CompactionScoreSizeWeight** | `0.8` | Weight for size-based compaction scoring |
| **CompactionScoreCountWeight** | `0.2` | Weight for count-based compaction scoring |
| **CompactionSizeTieredSimilarityRatio** | `1.5` | Similarity ratio for size-tiered compaction. For grouping SSTables that are "roughly the same size" together for compaction. |
| **FlusherTickerInterval** | `1 * time.Millisecond` | Interval for flusher background process |
| **CompactorTickerInterval** | `250 * time.Millisecond` | Interval for compactor background process |
| **BloomFilterFPR** | `0.01` | False positive rate for Bloom filters |
| **WalAppendRetry** | `10` | Number of retries for WAL append operations |
| **WalAppendBackoff** | `128 * time.Microsecond` | Backoff duration for WAL append retries |
| **BlockManagerLRUEvictRatio** | `0.20` | Ratio for LRU eviction. Determines what percentage of the cache to evict when cleanup is needed. |
| **BlockManagerLRUAccesWeight** | `0.8` | Weight for LRU access eviction. Balances how much to prioritize access frequency vs. age when deciding what to evict. |
| **STDOutLogging** | `false` | If true, logs will be printed to stdout instead of the log channel. Log channel will be ignored if provided. |
| **MaxConcurrentTxns** | `65536` | Maximum number of concurrent transactions. This is the size of the ring buffer used for transaction management. |
| **TxnBeginRetry** | `10` | Number of retries for `Begin()` when the transaction buffer is full. |
| **TxnBeginBackoff** | `1 * time.Microsecond` | Initial backoff duration for `Begin()` retries when the transaction buffer is full. |
| **TxnBeginMaxBackoff** | `100 * time.Millisecond` | Maximum backoff duration for `Begin()` retries when the transaction buffer is full. |
| **RecoverUncommittedTxns** | `true` | If true, Wildcat will attempt to recover uncommitted transactions on startup. This allows you to inspect and potentially commit or rollback transactions that were in progress at the time of a crash. |

## Shared C Library
You will require the latest Go toolchain to build the shared C library for Wildcat. This allows you to use Wildcat as a C library in other languages.
```bash
go build -buildmode=c-shared -o libwildcat.so wildcat_c.go
```

### C API
For C example check `c/example.c` in the [repository](https://github.com/wildcatdb/wildcat/tree/master/c).

### Linux instructions
Once you've built your shared library, you can use the below commands.
```bash
sudo cp libwildcat.so /usr/local/lib/
sudo cp libwildcat.h /usr/local/include/
sudo ldconfig
```

Now you can include header
```c
#include <wildcat.h>
```

Then you can link against the library when compiling.  Below is an example compiling `example.c` in repository on Linux.
```bash
gcc -o wildcat_example c/example.c -L. -lwildcat -lpthread
```

#### Options
```c
typedef struct {
    char* directory;
    long write_buffer_size;
    int sync_option;
    long sync_interval_ns;
    int level_count;
    int level_multiplier;
    int block_manager_lru_size;
    double block_manager_lru_evict_ratio;
    double block_manager_lru_access_weight;
    int permission;
    int bloom_filter;
    int max_compaction_concurrency;
    long compaction_cooldown_ns;
    int compaction_batch_size;
    double compaction_size_ratio;
    int compaction_size_threshold;
    double compaction_score_size_weight;
    double compaction_score_count_weight;
    double compaction_size_tiered_similarity_ratio;
    long flusher_interval_ns;
    long compactor_interval_ns;
    double bloom_fpr;
    int wal_append_retry;
    long wal_append_backoff_ns;
    int sstable_btree_order;
    int stdout_logging;
    int max_compaction_concurrency;
    int txn_begin_retry;
    long txn_begin_backoff_ns;
    long txn_begin_max_backoff_ns;
    int recover_uncommitted_txns;
} wildcat_opts_t;
```

#### Sync Options
```c
typedef enum {
    SYNC_NONE = 0,
    SYNC_FULL,
    SYNC_PARTIAL
} sync_option_t;
```

#### C API Functions
```c
extern long unsigned int wildcat_open(wildcat_opts_t* opts);
extern void wildcat_close(long unsigned int handle);
extern long int wildcat_begin_txn(long unsigned int handle);
extern int wildcat_txn_put(long unsigned int handle, long int txnId, char* key, char* val);
extern char* wildcat_txn_get(long unsigned int handle, long int txnId, char* key);
extern int wildcat_txn_delete(long unsigned int handle, long int txnId, char* key);
extern int wildcat_txn_commit(long unsigned int handle, long int txnId);
extern int wildcat_txn_rollback(long unsigned int handle, long int txnId);
extern void wildcat_txn_free(long unsigned int handle, long int txnId);
extern long unsigned int wildcat_txn_new_iterator(long unsigned int handle, long int txnId, int asc);
extern long unsigned int wildcat_txn_new_range_iterator(long unsigned int handle, long int txnId, char* start, char* end, int asc);
extern long unsigned int wildcat_txn_new_prefix_iterator(long unsigned int handle, long int txnId, char* prefix, int asc);
extern char* wildcat_stats(long unsigned int handle);
extern int wildcat_force_flush(long unsigned int handle);
extern int wildcat_txn_iterate_next(long unsigned int id);
extern int wildcat_txn_iterate_prev(long unsigned int id);
extern int wildcat_txn_iter_valid(long unsigned int id);
extern char* wildcat_iterator_key(long unsigned int id);
extern char* wildcat_iterator_value(long unsigned int id);
extern void wildcat_iterator_free(long unsigned int id);
extern int wildcat_sync(long unsigned int handle);
```

### Contributing
You are open to contribute to Wildcat.  Find a bug, optimization, refactor and submit a PR.  It will be reviewed.  You're only helping us all.
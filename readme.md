## OrinDB
OrinDB is a lightweight, key-value LSM tree storage library.
It focuses on massive concurrency, transactional support, and high performance.

## Features
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

## Todo
- Merge iterator with optional point query for transactions
- Possible bloom filter for member queries
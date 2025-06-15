package wildcat

import (
	"errors"
	"fmt"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/tree"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Txn represents a transaction in a Wildcat DB instance
type Txn struct {
	Id        int64             // The transactions id, can be recovered
	ReadSet   map[string]int64  // Key -> Timestamp
	WriteSet  map[string][]byte // Key -> Value
	DeleteSet map[string]bool   // Key -> Deleted
	Timestamp int64             // The timestamp of the transaction
	Committed bool              // Whether the transaction is committed
	db        *DB               // Not exported for serialization
	mutex     sync.Mutex        // Not exported for serialization
}

// Begin starts a new transaction
func (db *DB) Begin() (*Txn, error) {
	txn := &Txn{
		db:        db,
		ReadSet:   make(map[string]int64),
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		Timestamp: db.txnTSGenerator.nextID(),
		Committed: false,
		mutex:     sync.Mutex{},
	}

	// Atomic retry loop with exponential backoff
	var lastErr error
	backoff := db.opts.TxnBeginBackoff

	for attempt := 0; attempt <= db.opts.TxnBeginRetry; attempt++ {
		var err error
		txn.Id, err = db.txnBuffer.Add(txn)
		if err == nil {
			// Successfully added to the transaction ring buffer
			return txn, nil
		}

		lastErr = fmt.Errorf("transaction ring buffer full on attempt %d", attempt+1)

		// Check if we've exhausted retries
		if attempt == db.opts.TxnBeginRetry {
			break
		}

		// Exponential backoff with jitter!!!!
		sleepTime := backoff * (1 << uint(attempt))
		if sleepTime > db.opts.TxnBeginMaxBackoff {
			sleepTime = db.opts.TxnBeginMaxBackoff
		}

		// Add jitter (± 25% randomization)
		jitter := time.Duration(rand.Int63n(int64(sleepTime / 4)))
		if rand.Intn(2) == 0 {
			sleepTime += jitter
		} else {
			sleepTime -= jitter
		}

		time.Sleep(sleepTime)
	}

	// All retries exhausted
	return nil, errors.New(fmt.Sprintf("failed to begin transaction after %d attempts: %v",
		db.opts.TxnBeginRetry+1, lastErr))
}

// GetTxn retrieves a transaction by ID.
// Can be used on system recovery.  You can recover an incomplete transaction.
func (db *DB) GetTxn(id int64) (*Txn, error) {
	txn, err := db.txnBuffer.Get(id)
	if txn == nil {
		return nil, fmt.Errorf("transaction with ID %d not found", id)
	}
	return txn.(*Txn), err
}

// Put adds key-value pair to database
func (txn *Txn) Put(key []byte, value []byte) error {

	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	if len(value) == 0 {
		return fmt.Errorf("value cannot be empty")
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add to write set
	txn.WriteSet[string(key)] = value
	delete(txn.DeleteSet, string(key)) // Remove from delete set if exists

	err := txn.appendWal()
	if err != nil {
		return err
	}

	return nil
}

// Delete removes a key from database
func (txn *Txn) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add to delete set
	txn.DeleteSet[string(key)] = true
	delete(txn.WriteSet, string(key)) // Remove from write set if exists

	err := txn.appendWal()
	if err != nil {
		return err
	}

	return nil
}

// Commit commits the transaction
func (txn *Txn) Commit() error {
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}

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
		atomic.AddInt64(&txn.db.memtable.Load().(*Memtable).size, int64(len(key)+len(value)))
	}

	// Apply deletes
	for key := range txn.DeleteSet {
		txn.db.memtable.Load().(*Memtable).skiplist.Delete([]byte(key), txn.Timestamp)

		// Decrement the size of the memtable
		atomic.AddInt64(&txn.db.memtable.Load().(*Memtable).size, -int64(len(key)))
	}

	txn.Committed = true

	err := txn.appendWal()
	if err != nil {
		return err
	}

	// Check if we need to enqueue the memtable for flush
	if atomic.LoadInt64(&txn.db.memtable.Load().(*Memtable).size) > txn.db.opts.WriteBufferSize {
		err = txn.db.flusher.queueMemtable()
		if err != nil {
			return fmt.Errorf("failed to queue memtable: %w", err)
		}
	}

	return nil
}

// Rollback rolls back the transaction
func (txn *Txn) Rollback() error {
	if txn == nil {
		return fmt.Errorf("transaction is nil")
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()
	defer txn.remove() // Ensure cleanup happens

	// Clear all pending changes
	txn.WriteSet = make(map[string][]byte)
	txn.DeleteSet = make(map[string]bool)
	txn.ReadSet = make(map[string]int64)

	txn.Committed = false

	err := txn.appendWal()
	if err != nil {
		return err
	}

	return nil
}

// Get retrieves a value by key
func (txn *Txn) Get(key []byte) ([]byte, error) {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Check write set first (transaction's own writes)
	if val, exists := txn.WriteSet[string(key)]; exists {
		return val, nil
	}

	// Check delete set (transaction's own deletes)
	if txn.DeleteSet[string(key)] {
		return nil, fmt.Errorf("key not found")
	}

	// Track the best value found so far and its timestamp
	var bestValue []byte = nil
	var bestTimestamp int64 = 0

	// Fetch from active memtable
	val, ts, ok := txn.db.memtable.Load().(*Memtable).skiplist.Get(key, txn.Timestamp)
	if ok && ts <= txn.Timestamp && ts > bestTimestamp {
		bestValue = val
		bestTimestamp = ts
	}

	// Check currently flushing memtable if any
	// We do this as depending on write buffer the flushing memory table can take a while to persist to an immutable btree (sstable)
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		val, ts, ok = fmem.skiplist.Get(key, txn.Timestamp)
		if ok && ts <= txn.Timestamp && ts > bestTimestamp {
			bestValue = val
			bestTimestamp = ts
		}
	}

	// Check immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		val, ts, ok = memt.skiplist.Get(key, txn.Timestamp)
		if ok && ts <= txn.Timestamp && ts > bestTimestamp {
			bestValue = val
			bestTimestamp = ts
		}
		return true // Continue searching
	})

	// Check levels for SSTables
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}

			// Check all SSTables in this level
			for _, sstable := range *sstables {
				val, ts = sstable.get(key, txn.Timestamp)

				if ts == 0 {
					continue // Key not found in this SSTable or timestamp not visible
				}

				// Only update if we found a better (more recent) version
				// that's still visible to our read timestamp
				if ts <= txn.Timestamp && ts > bestTimestamp {
					bestValue = val // This could be nil for deletions
					bestTimestamp = ts
				}
			}
		}
	}

	// Check if we found a value
	if bestTimestamp > 0 {
		if bestValue == nil {
			return nil, fmt.Errorf("key not found") // This was a deletion
		}
		return bestValue, nil
	}

	return nil, fmt.Errorf("key not found")
}

// Update performs an atomic update using a transaction
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}

	txn, err := db.Begin()
	if txn == nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer txn.remove()

	fnErr := fn(txn)
	if fnErr != nil {
		rollbackErr := txn.Rollback()
		if rollbackErr != nil {
			return fmt.Errorf("transaction failed: %v, rollback failed: %w", fnErr, rollbackErr)
		}
		return fnErr // Return the original error from the function
	}
	return txn.Commit()
}

// View performs a read-only transaction
func (db *DB) View(fn func(txn *Txn) error) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}

	txn, err := db.Begin()
	if txn == nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer txn.remove()

	fnErr := fn(txn)
	if fnErr != nil {
		return fnErr
	}

	// We don't commit - this is a read-only transaction
	return nil
}

// NewIterator creates a new bidirectional iterator
func (txn *Txn) NewIterator(asc bool) (*MergeIterator, error) {
	var items []*iterator

	// Active memtable
	active := txn.db.memtable.Load().(*Memtable)
	iter, err := active.skiplist.NewIterator(nil, txn.Timestamp)
	if err != nil {
		return nil, err
	}
	if !asc {
		iter.ToLast()
	}

	items = append(items, &iterator{
		underlyingIterator: iter,
	})

	// Currently flushing memtable if any
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		it, err := fmem.skiplist.NewIterator(nil, txn.Timestamp)
		if err == nil {
			if !asc {
				it.ToLast()
			}
			items = append(items, &iterator{
				underlyingIterator: it,
			})
		}
	}

	// Immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		it, err := memt.skiplist.NewIterator(nil, txn.Timestamp)
		if err != nil {
			return false // Error occurred, stop iteration
		}
		if !asc {
			it.ToLast()
		}

		items = append(items, &iterator{
			underlyingIterator: it,
		})
		return true
	})

	// SSTables from each level
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}
			for _, sst := range *sstables {
				klogPath := sst.kLogPath()
				klogBm, ok := txn.db.lru.Get(klogPath)
				if !ok {
					var err error
					klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY,
						txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
					if err != nil {
						continue
					}
					txn.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
						if bm, ok := value.(*blockmanager.BlockManager); ok {
							_ = bm.Close()
						}
					})
				} else if v, ok := klogBm.(*blockmanager.BlockManager); !ok || v == nil {
					continue
				}

				t, err := tree.Open(klogBm.(*blockmanager.BlockManager), sst.db.opts.SSTableBTreeOrder, sst)
				if err != nil {
					continue
				}
				treeIter, err := t.Iterator(asc)
				if err != nil {
					continue
				}
				items = append(items, &iterator{
					underlyingIterator: treeIter,
					sst:                sst,
				})
			}
		}
	}

	return NewMergeIterator(txn.db, items, txn.Timestamp, asc)
}

// NewRangeIterator creates a new range bidirectional iterator
func (txn *Txn) NewRangeIterator(startKey []byte, endKey []byte, asc bool) (*MergeIterator, error) {
	var items []*iterator

	// Active memtable
	active := txn.db.memtable.Load().(*Memtable)
	iter, err := active.skiplist.NewRangeIterator(startKey, endKey, txn.Timestamp)
	if err != nil {
		return nil, err
	}
	if !asc {
		iter.ToLast()
	}

	items = append(items, &iterator{
		underlyingIterator: iter,
	})

	// Currently flushing memtable if any
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		it, err := fmem.skiplist.NewRangeIterator(startKey, endKey, txn.Timestamp)
		if err == nil {
			if !asc {
				it.ToLast()
			}
			items = append(items, &iterator{
				underlyingIterator: it,
			})
		}
	}

	// Immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		it, err := memt.skiplist.NewRangeIterator(startKey, endKey, txn.Timestamp)
		if err != nil {
			return false // Error occurred, stop iteration
		}
		if !asc {
			it.ToLast()
		}

		items = append(items, &iterator{
			underlyingIterator: it,
		})
		return true
	})

	// SSTables from each level
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}
			for _, sst := range *sstables {
				klogPath := sst.kLogPath()
				klogBm, ok := txn.db.lru.Get(klogPath)
				if !ok {
					var err error
					klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY,
						txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
					if err != nil {
						continue
					}
					txn.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
						if bm, ok := value.(*blockmanager.BlockManager); ok {
							_ = bm.Close()
						}
					})
				} else if v, ok := klogBm.(*blockmanager.BlockManager); !ok || v == nil {
					continue
				}

				t, err := tree.Open(klogBm.(*blockmanager.BlockManager), sst.db.opts.SSTableBTreeOrder, sst)
				if err != nil {
					continue
				}
				treeIter, err := t.RangeIterator(startKey, endKey, asc)
				if err != nil {
					continue
				}
				items = append(items, &iterator{
					underlyingIterator: treeIter,
					sst:                sst,
				})
			}
		}
	}

	return NewMergeIterator(txn.db, items, txn.Timestamp, asc)
}

// NewPrefixIterator creates a new prefix bidirectional iterator
func (txn *Txn) NewPrefixIterator(prefix []byte, asc bool) (*MergeIterator, error) {
	var items []*iterator

	// Active memtable
	active := txn.db.memtable.Load().(*Memtable)
	iter, err := active.skiplist.NewPrefixIterator(prefix, txn.Timestamp)
	if err != nil {
		return nil, err
	}
	if !asc {
		iter.ToLast()
	}

	items = append(items, &iterator{
		underlyingIterator: iter,
	})

	// Currently flushing memtable if any
	fmem := txn.db.flusher.flushing.Load()
	if fmem != nil {
		it, err := fmem.skiplist.NewPrefixIterator(prefix, txn.Timestamp)
		if err == nil {
			if !asc {
				it.ToLast()
			}
			items = append(items, &iterator{
				underlyingIterator: it,
			})
		}
	}

	// Immutable memtables
	txn.db.flusher.immutable.ForEach(func(item interface{}) bool {
		memt := item.(*Memtable)
		it, err := memt.skiplist.NewPrefixIterator(prefix, txn.Timestamp)
		if err != nil {
			return false // Error occurred, stop iteration
		}
		if !asc {
			it.ToLast()
		}

		items = append(items, &iterator{
			underlyingIterator: it,
		})
		return true
	})

	// SSTables from each level
	levels := txn.db.levels.Load()
	if levels != nil {
		for _, level := range *levels {
			sstables := level.sstables.Load()
			if sstables == nil {
				continue
			}
			for _, sst := range *sstables {
				klogPath := sst.kLogPath()
				klogBm, ok := txn.db.lru.Get(klogPath)
				if !ok {
					var err error
					klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY,
						txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
					if err != nil {
						continue
					}
					txn.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
						if bm, ok := value.(*blockmanager.BlockManager); ok {
							_ = bm.Close()
						}
					})
				} else if v, ok := klogBm.(*blockmanager.BlockManager); !ok || v == nil {
					continue
				}

				t, err := tree.Open(klogBm.(*blockmanager.BlockManager), sst.db.opts.SSTableBTreeOrder, sst)
				if err != nil {
					continue
				}
				treeIter, err := t.PrefixIterator(prefix, asc)
				if err != nil {
					continue
				}
				items = append(items, &iterator{
					underlyingIterator: treeIter,
					sst:                sst,
				})
			}
		}
	}

	return NewMergeIterator(txn.db, items, txn.Timestamp, asc)
}

// remove removes the transaction from the database
func (txn *Txn) remove() {
	if txn == nil || txn.db == nil {
		return
	}

	// Clear the transaction data
	txn.ReadSet = make(map[string]int64)
	txn.WriteSet = make(map[string][]byte)
	txn.DeleteSet = make(map[string]bool)
	txn.Committed = false

	_ = txn.db.txnBuffer.Remove(txn.Id)
}

// appendWal appends the transaction state to a Write-Ahead Log (WAL)
func (txn *Txn) appendWal() error {
	data, err := txn.serializeTransaction()
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= txn.db.opts.WalAppendRetry; attempt++ {

		walPath := txn.db.memtable.Load().(*Memtable).wal.path
		wal, ok := txn.db.lru.Get(walPath)
		if !ok {

			walBm, err := blockmanager.Open(walPath, os.O_WRONLY|os.O_APPEND,
				txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
			if err != nil {
				lastErr = fmt.Errorf("failed to open WAL block manager: %w", err)
				if attempt == txn.db.opts.WalAppendRetry {
					return lastErr
				}
				time.Sleep(txn.db.opts.WalAppendBackoff)
				continue
			}

			txn.db.lru.Put(walPath, walBm, func(key, value interface{}) {
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})

			wal = walBm
		}

		_, err = wal.(*blockmanager.BlockManager).Append(data)
		if err == nil {
			// Success EXIT!!
			return nil
		}

		lastErr = err

		needsReopen := errors.Is(err, syscall.EBADF) ||
			strings.Contains(err.Error(), "bad file descriptor")

		if needsReopen {
			txn.db.lru.Delete(walPath)

			walBm, err := blockmanager.Open(walPath, os.O_WRONLY|os.O_APPEND,
				txn.db.opts.Permission, blockmanager.SyncOption(txn.db.opts.SyncOption))
			if err != nil {
				lastErr = fmt.Errorf("failed to reopen WAL block manager: %w", err)
				if attempt == txn.db.opts.WalAppendRetry {
					return lastErr
				}
				time.Sleep(txn.db.opts.WalAppendBackoff)
				continue
			}

			txn.db.lru.Put(walPath, walBm, func(key, value interface{}) {
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})

			_, err = walBm.Append(data)
			if err == nil {
				// Success after reopen EXIT!!
				return nil
			}

			lastErr = fmt.Errorf("failed to append transaction to WAL after reopen: %w", err)
		} else {
			lastErr = fmt.Errorf("failed to append transaction to WAL: %w", err)
		}

		if attempt == txn.db.opts.WalAppendRetry {
			return lastErr
		}

		time.Sleep(txn.db.opts.WalAppendBackoff)
	}

	return lastErr
}

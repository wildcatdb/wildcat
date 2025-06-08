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
	"unsafe"
)

// TxnRingBuffer implements a lock-free ring buffer for transactions
type TxnRingBuffer struct {
	buffer   []*TxnSlot // Ring buffer of *TxnSlot pointers
	mask     uint64     // Size mask (size must be power of 2)
	head     uint64     // Next position to write
	tail     uint64     // Next position to read/search from
	capacity uint64     // Buffer capacity
}

// TxnSlot represents a slot in the transaction ring buffer
type TxnSlot struct {
	txn     unsafe.Pointer // *Txn pointer
	version uint64         // Version for ABA prevention
	active  uint64         // 1 if active, 0 if free
}

// Txn represents a transaction in Wildcat
type Txn struct {
	Id        int64             // The transactions id, can be recovered
	ReadSet   map[string]int64  // Key -> Timestamp
	WriteSet  map[string][]byte // Key -> Value
	DeleteSet map[string]bool   // Key -> Deleted
	Timestamp int64             // The timestamp of the transaction
	Committed bool              // Whether the transaction is committed
	db        *DB               // Not exported for serialization
	mutex     sync.Mutex        // Not exported for serialization
	slotPos   uint64            // Position in ring buffer
}

// newTxnRingBuffer creates a new TxnRingBuffer with the specified capacity for transactions
func newTxnRingBuffer(capacity uint64) *TxnRingBuffer {

	// Ensure capacity is power of 2
	if capacity&(capacity-1) != 0 {
		// Round up to next power of 2
		capacity = 1 << (64 - uint64(countLeadingZeros(capacity-1)))
	}

	// Properly initialize the buffer slice
	buffer := make([]*TxnSlot, capacity)
	for i := uint64(0); i < capacity; i++ {
		buffer[i] = &TxnSlot{}
	}

	return &TxnRingBuffer{
		buffer:   buffer,
		mask:     capacity - 1,
		capacity: capacity,
		head:     0,
		tail:     0,
	}
}

// add adds a transaction to the ring buffer
func (rb *TxnRingBuffer) add(txn *Txn) bool {
	// Safety check
	if rb == nil || rb.buffer == nil || len(rb.buffer) == 0 || txn == nil {
		return false
	}

	// Try to find a free slot
	for attempts := uint64(0); attempts < rb.capacity*4; attempts++ {
		// First, try to advance tail to free up inactive slots
		rb.advanceTail()

		head := atomic.LoadUint64(&rb.head)
		tail := atomic.LoadUint64(&rb.tail)

		// Check if buffer is full
		if head-tail >= rb.capacity {
			return false // Buffer is full
		}

		// Try each slot starting from tail (to reuse freed slots first)
		for offset := uint64(0); offset < rb.capacity; offset++ {
			pos := (tail + offset) & rb.mask

			// Safety bounds check
			if pos >= uint64(len(rb.buffer)) {
				continue
			}

			slot := rb.buffer[pos]
			if slot == nil {
				continue
			}

			// Check if slot is free and try to claim it
			if atomic.CompareAndSwapUint64(&slot.active, 0, 1) {

				// Successfully claimed the slot
				atomic.AddUint64(&slot.version, 1)

				atomic.StorePointer(&slot.txn, unsafe.Pointer(txn))

				// Store slot position in transaction for fast removal
				txn.slotPos = pos

				// Update head to be at least past this position
				for {
					currentHead := atomic.LoadUint64(&rb.head)
					newHead := pos + 1
					if newHead <= currentHead {
						break // Head is already past this position
					}
					if atomic.CompareAndSwapUint64(&rb.head, currentHead, newHead) {
						break // Successfully updated head
					}
					// Retry if CAS failed
				}
				return true
			}
		}

		// No free slots found, try to advance head and retry
		if !atomic.CompareAndSwapUint64(&rb.head, head, head+1) {
			// Someone else advanced head, retry immediately
			continue
		}
	}

	return false // Couldn't find a free slot after many attempts
}

// advanceTail tries to advance the tail pointer past inactive slots
func (rb *TxnRingBuffer) advanceTail() {
	maxAdvances := rb.capacity / 4 // Limit how much we advance to prevent infinite loops**
	if maxAdvances == 0 {
		maxAdvances = 1 // Ensure we advance at least once for small buffers
	}

	for advances := uint64(0); advances < maxAdvances; advances++ {
		tail := atomic.LoadUint64(&rb.tail)
		head := atomic.LoadUint64(&rb.head)

		// Don't advance past head
		if tail >= head {
			break
		}

		pos := tail & rb.mask

		// Safety bounds check
		if pos >= uint64(len(rb.buffer)) {
			break
		}

		slot := rb.buffer[pos]
		if slot == nil {
			break
		}

		// If this slot is inactive, we can advance tail
		if atomic.LoadUint64(&slot.active) == 0 {
			if atomic.CompareAndSwapUint64(&rb.tail, tail, tail+1) {
				continue // Successfully advanced, try next
			} else {
				break // Someone else changed tail, stop trying
			}
		} else {
			break // Found an active slot, can't advance further
		}
	}
}

// remove removes a transaction from the ring buffer
func (rb *TxnRingBuffer) remove(txn *Txn) bool {
	if rb == nil || rb.buffer == nil || txn == nil {
		return false
	}

	if txn.slotPos >= rb.capacity {
		return false
	}

	slot := rb.buffer[txn.slotPos]
	if slot == nil {
		return false
	}

	// Check if this is still our transaction
	storedTxn := (*Txn)(atomic.LoadPointer(&slot.txn))
	if storedTxn != txn {
		return false
	}

	// Clear the slot atomically - order matters here
	atomic.StorePointer(&slot.txn, nil)
	atomic.AddUint64(&slot.version, 1)  // Increment version on removal
	atomic.StoreUint64(&slot.active, 0) // Mark as inactive last to ensure cleanup

	return true
}

// findByID finds a transaction by ID
func (rb *TxnRingBuffer) findByID(id int64) *Txn {
	if rb == nil || rb.buffer == nil {
		return nil
	}

	for i := uint64(0); i < rb.capacity; i++ {
		slot := rb.buffer[i]
		if slot != nil && atomic.LoadUint64(&slot.active) == 1 {
			txn := (*Txn)(atomic.LoadPointer(&slot.txn))
			if txn != nil && txn.Id == id {
				return txn
			}
		}
	}
	return nil
}

// forEach iterates over all active transactions
func (rb *TxnRingBuffer) forEach(fn func(*Txn) bool) {
	if rb == nil || rb.buffer == nil {
		return
	}

	for i := uint64(0); i < rb.capacity; i++ {
		slot := rb.buffer[i]
		if slot != nil && atomic.LoadUint64(&slot.active) == 1 {
			txn := (*Txn)(atomic.LoadPointer(&slot.txn))
			if txn != nil {
				if !fn(txn) {
					break
				}
			}
		}
	}
}

// count returns the approximate number of active transactions
func (rb *TxnRingBuffer) count() uint64 {
	if rb == nil || rb.buffer == nil {
		return 0
	}

	count := uint64(0)
	for i := uint64(0); i < rb.capacity; i++ {
		slot := rb.buffer[i]
		if slot != nil && atomic.LoadUint64(&slot.active) == 1 {
			count++
		}
	}
	return count
}

// Begin starts a new transaction
func (db *DB) Begin() (*Txn, error) {
	txn := &Txn{
		Id:        db.txnIdGenerator.nextID(),
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
		if db.txnRing.add(txn) {
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
	txn := db.txnRing.findByID(id)
	if txn == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return txn, nil
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
	defer txn.remove() // Ensure cleanup happens

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
		// Enqueue the memtable for flush and swap
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

	// We always update oldestActiveRead to latest read which could be the oldest if that makes sense :)
	// On background compactions we don't just want to blindly merge sstables that could be part of active reads transactions.
	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

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

	defer txn.remove() // Ensure transaction is cleaned up

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

	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

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

	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

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

	atomic.StoreInt64(&txn.db.oldestActiveRead, txn.Timestamp)

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

	// Remove from ring buffer
	txn.db.txnRing.remove(txn)
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

			// Open the WAL file
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

			// Add to LRU cache
			txn.db.lru.Put(walPath, walBm, func(key, value interface{}) {
				// Close the block manager when evicted from LRU
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})

			// Use the newly opened WAL
			wal = walBm
		}

		_, err = wal.(*blockmanager.BlockManager).Append(data)
		if err == nil {
			// Success EXIT!!
			return nil
		}

		lastErr = err

		// Special handling for bad file descriptor errors
		needsReopen := errors.Is(err, syscall.EBADF) ||
			strings.Contains(err.Error(), "bad file descriptor")

		if needsReopen {
			// Remove the bad file descriptor from cache first
			txn.db.lru.Delete(walPath)

			// Reopen the WAL file
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

			// Add to LRU cache
			txn.db.lru.Put(walPath, walBm, func(key, value interface{}) {
				if bm, ok := value.(*blockmanager.BlockManager); ok {
					_ = bm.Close()
				}
			})

			// Try to append again immediately with the new descriptor
			_, err = walBm.Append(data)
			if err == nil {
				// Success after reopen EXIT!!
				return nil
			}

			lastErr = fmt.Errorf("failed to append transaction to WAL after reopen: %w", err)
		} else {
			lastErr = fmt.Errorf("failed to append transaction to WAL: %w", err)
		}

		// Check if we've used all our retries
		if attempt == txn.db.opts.WalAppendRetry {
			return lastErr
		}

		// Wait before next retry
		time.Sleep(txn.db.opts.WalAppendBackoff)
	}

	return lastErr
}

// countLeadingZeros counts the number of leading zeros in an uint64 value.
func countLeadingZeros(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x <= 0x00000000FFFFFFFF {
		n += 32
		x <<= 32
	}
	if x <= 0x0000FFFFFFFFFFFF {
		n += 16
		x <<= 16
	}
	if x <= 0x00FFFFFFFFFFFFFF {
		n += 8
		x <<= 8
	}
	if x <= 0x0FFFFFFFFFFFFFFF {
		n += 4
		x <<= 4
	}
	if x <= 0x3FFFFFFFFFFFFFFF {
		n += 2
		x <<= 2
	}
	if x <= 0x7FFFFFFFFFFFFFFF {
		n += 1
	}
	return n
}

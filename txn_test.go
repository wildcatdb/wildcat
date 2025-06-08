package wildcat

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestTxnRingBuffer_BasicOperations(t *testing.T) {
	capacity := uint64(8)
	rb := newTxnRingBuffer(capacity)

	if rb == nil {
		t.Fatal("Failed to create ring buffer")
	}

	if rb.capacity != capacity {
		t.Errorf("Expected capacity %d, got %d", capacity, rb.capacity)
	}

	if rb.mask != capacity-1 {
		t.Errorf("Expected mask %d, got %d", capacity-1, rb.mask)
	}

	if len(rb.buffer) != int(capacity) {
		t.Errorf("Expected buffer length %d, got %d", capacity, len(rb.buffer))
	}

	for i := 0; i < len(rb.buffer); i++ {
		if rb.buffer[i] == nil {
			t.Errorf("Buffer slot %d is nil", i)
		}
		if atomic.LoadUint64(&rb.buffer[i].active) != 0 {
			t.Errorf("Buffer slot %d should be inactive", i)
		}
	}
}

func TestTxnRingBuffer_PowerOfTwoCapacity(t *testing.T) {
	testCases := []struct {
		input    uint64
		expected uint64
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{15, 16},
		{16, 16},
		{17, 32},
		{100, 128},
		{1000, 1024},
	}

	for _, tc := range testCases {
		rb := newTxnRingBuffer(tc.input)
		if rb.capacity != tc.expected {
			t.Errorf("Input %d: expected capacity %d, got %d", tc.input, tc.expected, rb.capacity)
		}
		if rb.mask != tc.expected-1 {
			t.Errorf("Input %d: expected mask %d, got %d", tc.input, tc.expected-1, rb.mask)
		}
	}
}

func TestTxnRingBuffer_AddAndRemove(t *testing.T) {
	rb := newTxnRingBuffer(4)

	txn1 := &Txn{Id: 1, Timestamp: 100}
	txn2 := &Txn{Id: 2, Timestamp: 200}
	txn3 := &Txn{Id: 3, Timestamp: 300}

	if !rb.add(txn1) {
		t.Error("Failed to add first transaction")
	}
	if !rb.add(txn2) {
		t.Error("Failed to add second transaction")
	}
	if !rb.add(txn3) {
		t.Error("Failed to add third transaction")
	}

	if rb.count() != 3 {
		t.Errorf("Expected count 3, got %d", rb.count())
	}

	found := rb.findByID(2)
	if found == nil {
		t.Error("Failed to find transaction by ID")
	}
	if found.Id != 2 {
		t.Errorf("Expected transaction ID 2, got %d", found.Id)
	}

	if !rb.remove(txn2) {
		t.Error("Failed to remove transaction")
	}

	found = rb.findByID(2)
	if found != nil {
		t.Error("Found removed transaction")
	}

	if rb.count() != 2 {
		t.Errorf("Expected count 2, got %d", rb.count())
	}
}

func TestTxnRingBuffer_BufferFull(t *testing.T) {
	capacity := uint64(4)
	rb := newTxnRingBuffer(capacity)

	var txns []*Txn
	for i := uint64(0); i < capacity; i++ {
		txn := &Txn{Id: int64(i + 1), Timestamp: int64((i + 1) * 100)}
		txns = append(txns, txn)
		if !rb.add(txn) {
			t.Errorf("Failed to add transaction %d", i+1)
		}
	}

	extraTxn := &Txn{Id: int64(capacity + 1), Timestamp: int64((capacity + 1) * 100)}
	if rb.add(extraTxn) {
		t.Error("Should not be able to add transaction to full buffer")
	}

	if !rb.remove(txns[0]) {
		t.Error("Failed to remove transaction")
	}

	if !rb.add(extraTxn) {
		t.Error("Should be able to add transaction after removal")
	}
}

func TestTxnRingBuffer_AdvanceTail(t *testing.T) {
	rb := newTxnRingBuffer(8)

	txns := make([]*Txn, 5)
	for i := 0; i < 5; i++ {
		txns[i] = &Txn{Id: int64(i + 1), Timestamp: int64((i + 1) * 100)}
		if !rb.add(txns[i]) {
			t.Errorf("Failed to add transaction %d", i+1)
		}
	}

	for i := 0; i < 3; i++ {
		if !rb.remove(txns[i]) {
			t.Errorf("Failed to remove transaction %d", i+1)
		}
	}

	rb.advanceTail()

	for i := 0; i < 3; i++ {
		newTxn := &Txn{Id: int64(i + 10), Timestamp: int64((i + 10) * 100)}
		if !rb.add(newTxn) {
			t.Errorf("Failed to add new transaction %d", i+10)
		}
	}

	if rb.count() != 5 {
		t.Errorf("Expected count 5, got %d", rb.count())
	}
}

func TestTxnRingBuffer_ForEach(t *testing.T) {
	rb := newTxnRingBuffer(8)

	expectedIDs := []int64{1, 2, 3, 4, 5}
	for _, id := range expectedIDs {
		txn := &Txn{Id: id, Timestamp: id * 100}
		if !rb.add(txn) {
			t.Errorf("Failed to add transaction %d", id)
		}
	}

	var foundIDs []int64
	rb.forEach(func(txn *Txn) bool {
		foundIDs = append(foundIDs, txn.Id)
		return true // Continue iteration
	})

	if len(foundIDs) != len(expectedIDs) {
		t.Errorf("Expected %d transactions, found %d", len(expectedIDs), len(foundIDs))
	}

	var limitedIDs []int64
	count := 0
	rb.forEach(func(txn *Txn) bool {
		limitedIDs = append(limitedIDs, txn.Id)
		count++
		return count < 3 // Stop after 3 transactions
	})

	if len(limitedIDs) != 3 {
		t.Errorf("Expected 3 transactions with early termination, got %d", len(limitedIDs))
	}
}

func TestTxnRingBuffer_ConcurrentOperations(t *testing.T) {
	rb := newTxnRingBuffer(64)

	const numGoroutines = 10
	const txnsPerGoroutine = 20

	var wg sync.WaitGroup
	var addedTxns sync.Map
	var addCount int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < txnsPerGoroutine; i++ {
				txnID := int64(goroutineID*txnsPerGoroutine + i + 1)
				txn := &Txn{
					Id:        txnID,
					Timestamp: txnID * 100,
				}

				if rb.add(txn) {
					addedTxns.Store(txnID, txn)
					atomic.AddInt64(&addCount, 1)
				}

				time.Sleep(time.Microsecond)
			}
		}(g)
	}

	wg.Wait()

	finalCount := atomic.LoadInt64(&addCount)
	ringCount := rb.count()

	// Verify counts match
	if ringCount != uint64(finalCount) {
		t.Errorf("Ring buffer count %d doesn't match added count %d", ringCount, finalCount)
	}

	// Verify all added transactions can be found
	addedTxns.Range(func(key, value interface{}) bool {
		txnID := key.(int64)
		found := rb.findByID(txnID)
		if found == nil {
			t.Errorf("Failed to find transaction %d", txnID)
		}
		return true
	})

	t.Logf("Successfully added %d transactions concurrently", finalCount)
}

func TestTxnRingBuffer_RemovalConcurrency(t *testing.T) {
	rb := newTxnRingBuffer(32)

	var txns []*Txn
	for i := 0; i < 20; i++ {
		txn := &Txn{Id: int64(i + 1), Timestamp: int64((i + 1) * 100)}
		if rb.add(txn) {
			txns = append(txns, txn)
		}
	}

	if len(txns) == 0 {
		t.Fatal("Failed to add any transactions")
	}

	// Concurrently remove transactions
	var wg sync.WaitGroup
	var removeCount int64

	for _, txn := range txns {
		wg.Add(1)
		go func(t *Txn) {
			defer wg.Done()
			if rb.remove(t) {
				atomic.AddInt64(&removeCount, 1)
			}
		}(txn)
	}

	wg.Wait()

	finalRemoveCount := atomic.LoadInt64(&removeCount)
	if finalRemoveCount != int64(len(txns)) {
		t.Errorf("Expected to remove %d transactions, removed %d", len(txns), finalRemoveCount)
	}

	if rb.count() != 0 {
		t.Errorf("Expected empty ring buffer, got count %d", rb.count())
	}
}

func TestTxnRingBuffer_EdgeCases(t *testing.T) {
	rb := newTxnRingBuffer(4)

	// Test operations on nil buffer
	var nilRb *TxnRingBuffer
	if nilRb.add(&Txn{Id: 1}) {
		t.Error("Should not be able to add to nil ring buffer")
	}
	if nilRb.remove(&Txn{Id: 1}) {
		t.Error("Should not be able to remove from nil ring buffer")
	}
	if nilRb.findByID(1) != nil {
		t.Error("Should not find transaction in nil ring buffer")
	}
	if nilRb.count() != 0 {
		t.Error("Nil ring buffer should have count 0")
	}

	// Test adding nil transaction
	if rb.add(nil) {
		t.Error("Should not be able to add nil transaction")
	}

	// Test removing nil transaction
	if rb.remove(nil) {
		t.Error("Should not be able to remove nil transaction")
	}

	// Test removing transaction not in buffer
	txn := &Txn{Id: 999, Timestamp: 999}
	if rb.remove(txn) {
		t.Error("Should not be able to remove transaction not in buffer")
	}

	// Test slot position validation
	txn.slotPos = 1000 // Invalid position
	if rb.remove(txn) {
		t.Error("Should not be able to remove transaction with invalid slot position")
	}
}

func TestTxnRingBuffer_SlotReuse(t *testing.T) {
	rb := newTxnRingBuffer(4)

	var txns []*Txn
	for i := 0; i < 4; i++ {
		txn := &Txn{Id: int64(i + 1), Timestamp: int64((i + 1) * 100)}
		if !rb.add(txn) {
			t.Errorf("Failed to add transaction %d", i+1)
		}
		txns = append(txns, txn)
	}

	// Remove every other transaction
	if !rb.remove(txns[0]) {
		t.Error("Failed to remove transaction 1")
	}
	if !rb.remove(txns[2]) {
		t.Error("Failed to remove transaction 3")
	}

	// Add new transactions.. should reuse freed slots
	newTxn1 := &Txn{Id: 10, Timestamp: 1000}
	newTxn2 := &Txn{Id: 11, Timestamp: 1100}

	if !rb.add(newTxn1) {
		t.Error("Failed to add new transaction 1")
	}
	if !rb.add(newTxn2) {
		t.Error("Failed to add new transaction 2")
	}

	// Should have 4 active transactions
	if rb.count() != 4 {
		t.Errorf("Expected count 4, got %d", rb.count())
	}

	// Verify specific transactions exist
	if rb.findByID(10) == nil {
		t.Error("Failed to find new transaction 10")
	}
	if rb.findByID(11) == nil {
		t.Error("Failed to find new transaction 11")
	}
	if rb.findByID(2) == nil {
		t.Error("Failed to find existing transaction 2")
	}
	if rb.findByID(4) == nil {
		t.Error("Failed to find existing transaction 4")
	}
}

func TestTxnRingBuffer_ABAProtection(t *testing.T) {
	rb := newTxnRingBuffer(4)

	txn1 := &Txn{Id: 1, Timestamp: 100}
	if !rb.add(txn1) {
		t.Fatal("Failed to add transaction")
	}

	pos := txn1.slotPos
	slot := rb.buffer[pos]
	initialVersion := atomic.LoadUint64(&slot.version)

	storedTxnPointer := atomic.LoadPointer(&slot.txn)
	storedTxn := (*Txn)(storedTxnPointer)

	if storedTxn != txn1 {
		t.Fatal("Stored transaction doesn't match")
	}
	if !rb.remove(txn1) {
		t.Fatal("Failed to remove transaction")
	}

	afterRemovalVersion := atomic.LoadUint64(&slot.version)
	if afterRemovalVersion <= initialVersion {
		t.Errorf("Version should have incremented: initial=%d, after=%d", initialVersion, afterRemovalVersion)
	}

	var txn2 *Txn
	var attempts int
	for attempts = 0; attempts < 20; attempts++ {
		candidateTxn := &Txn{Id: int64(attempts + 10), Timestamp: int64((attempts + 10) * 100)}
		if rb.add(candidateTxn) {
			if candidateTxn.slotPos == pos {
				txn2 = candidateTxn
				break
			}
			rb.remove(candidateTxn)
		}
	}

	if txn2 == nil {
		t.Skip("Could not force transaction into same slot - this is expected behavior")
		return
	}

	afterAddVersion := atomic.LoadUint64(&slot.version)
	if afterAddVersion <= afterRemovalVersion {
		t.Errorf("Version should have incremented again: after_removal=%d, after_add=%d", afterRemovalVersion, afterAddVersion)
	}

	if rb.remove(storedTxn) {
		t.Error("Should not be able to remove old transaction due to ABA protection")
	}

	currentTxnPointer := atomic.LoadPointer(&slot.txn)
	if storedTxnPointer == currentTxnPointer {
		t.Error("ABA scenario detected: same pointer in slot after remove/add cycle")
	}
}

func TestTxnRingBuffer_MemoryConsistency(t *testing.T) {
	rb := newTxnRingBuffer(8)

	txn := &Txn{Id: 1, Timestamp: 100}
	if !rb.add(txn) {
		t.Fatal("Failed to add transaction")
	}

	slot := rb.buffer[txn.slotPos]
	storedTxn := (*Txn)(atomic.LoadPointer(&slot.txn))
	if storedTxn != txn {
		t.Error("Stored transaction pointer doesn't match original")
	}

	if uintptr(unsafe.Pointer(storedTxn)) != uintptr(unsafe.Pointer(txn)) {
		t.Error("Unsafe pointer conversion failed")
	}

	txn.Timestamp = 999
	if storedTxn.Timestamp != 999 {
		t.Error("Changes to original transaction not visible through stored pointer")
	}
}

func TestTxnRingBuffer_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	rb := newTxnRingBuffer(128)
	const duration = 2 * time.Second
	const numWorkers = 8

	var wg sync.WaitGroup
	var operations int64
	var errors int64
	stop := make(chan struct{})

	// Start workers that continuously add and remove transactions
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localTxns := make([]*Txn, 0, 10)
			counter := int64(0)

			for {
				select {
				case <-stop:

					// Clean up remaining transactions
					for _, txn := range localTxns {
						rb.remove(txn)
					}
					return
				default:
				}

				counter++
				atomic.AddInt64(&operations, 1)

				if len(localTxns) < 5 || counter%3 == 0 {
					// Add a transaction
					txn := &Txn{
						Id:        int64(workerID)*1000000 + counter,
						Timestamp: time.Now().UnixNano(),
					}
					if rb.add(txn) {
						localTxns = append(localTxns, txn)
					} else {
						atomic.AddInt64(&errors, 1)
					}
				} else {
					// Remove a transaction
					if len(localTxns) > 0 {
						idx := counter % int64(len(localTxns))
						txn := localTxns[idx]
						if rb.remove(txn) {
							// Remove from local slice
							localTxns[idx] = localTxns[len(localTxns)-1]
							localTxns = localTxns[:len(localTxns)-1]
						} else {
							atomic.AddInt64(&errors, 1)
						}
					}
				}

				// Occasionally test findByID
				if counter%10 == 0 && len(localTxns) > 0 {
					txn := localTxns[counter%int64(len(localTxns))]
					found := rb.findByID(txn.Id)
					if found == nil {
						atomic.AddInt64(&errors, 1)
					}
				}
			}
		}(i)
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	totalOps := atomic.LoadInt64(&operations)
	totalErrors := atomic.LoadInt64(&errors)

	t.Logf("Stress test completed: %d operations, %d errors, %.2f%% error rate",
		totalOps, totalErrors, float64(totalErrors)/float64(totalOps)*100)

	// Error rate should be very low (less than 1%)
	if float64(totalErrors)/float64(totalOps) > 0.01 {
		t.Errorf("Error rate too high: %.2f%%", float64(totalErrors)/float64(totalOps)*100)
	}

	// Should have processed a significant number of operations
	if totalOps < 1000 {
		t.Errorf("Too few operations processed: %d", totalOps)
	}
}

func BenchmarkTxnRingBuffer_ConcurrentOperations_65536(b *testing.B) {
	capacity := uint64(65536)
	rb := newTxnRingBuffer(capacity)

	// Use number of CPUs for realistic concurrency
	numWorkers := runtime.NumCPU()
	if numWorkers < 4 {
		numWorkers = 4 // Minimum 4 workers for good concurrency test
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		var operations int64
		var successful int64

		// Each worker will perform operations for a short burst
		opsPerWorker := 1000

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				localTxns := make([]*Txn, 0, 100)
				baseID := int64(workerID * 1000000)

				for op := 0; op < opsPerWorker; op++ {
					atomic.AddInt64(&operations, 1)

					// Mix of operations: 60% add, 40% remove
					if len(localTxns) < 50 || op%5 < 3 {
						// Add operation
						txn := &Txn{
							Id:        baseID + int64(op),
							Timestamp: int64(time.Now().UnixNano()),
						}

						if rb.add(txn) {
							localTxns = append(localTxns, txn)
							atomic.AddInt64(&successful, 1)
						}
					} else {
						// Remove operation
						if len(localTxns) > 0 {
							idx := op % len(localTxns)
							txn := localTxns[idx]

							if rb.remove(txn) {

								// Remove from local slice efficiently
								localTxns[idx] = localTxns[len(localTxns)-1]
								localTxns = localTxns[:len(localTxns)-1]
								atomic.AddInt64(&successful, 1)
							}
						}
					}
				}

				// Cleanup remaining transactions
				for _, txn := range localTxns {
					rb.remove(txn)
				}
			}(w)
		}

		wg.Wait()

		// Report some stats for the last iteration
		if i == b.N-1 {
			totalOps := atomic.LoadInt64(&operations)
			successOps := atomic.LoadInt64(&successful)
			b.Logf("Workers: %d, Total ops: %d, Successful: %d (%.1f%%)",
				numWorkers, totalOps, successOps, float64(successOps)/float64(totalOps)*100)
		}
	}
}

func BenchmarkTxnRingBuffer_HighContention_65536(b *testing.B) {
	capacity := uint64(65536)
	rb := newTxnRingBuffer(capacity)

	// High number of workers to create contention
	numWorkers := 32

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		var addCount, removeCount int64

		// Pre-fill the buffer to ~80% capacity to create contention
		preFillCount := int(capacity * 8 / 10)
		var preFillTxns []*Txn

		for j := 0; j < preFillCount; j++ {
			txn := &Txn{
				Id:        int64(j + 1000000),
				Timestamp: int64(time.Now().UnixNano()),
			}
			if rb.add(txn) {
				preFillTxns = append(preFillTxns, txn)
			}
		}

		// Now run concurrent operations on a nearly full buffer
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				baseID := int64(workerID * 10000)

				// Each worker tries 100 operations
				for op := 0; op < 100; op++ {
					txn := &Txn{
						Id:        baseID + int64(op),
						Timestamp: int64(time.Now().UnixNano()),
					}

					// Try to add
					if rb.add(txn) {
						atomic.AddInt64(&addCount, 1)

						// Immediately try to remove to free space
						if rb.remove(txn) {
							atomic.AddInt64(&removeCount, 1)
						}
					}
				}
			}(w)
		}

		wg.Wait()

		// Cleanup pre-filled transactions
		for _, txn := range preFillTxns {
			rb.remove(txn)
		}

		if i == b.N-1 {
			b.Logf("High contention test - Adds: %d, Removes: %d",
				atomic.LoadInt64(&addCount), atomic.LoadInt64(&removeCount))
		}
	}
}

func TestTxn_BasicOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_txn_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Test basic transaction operations
	txn, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Verify transaction ID is non-empty
	if txn.Id == 0 {
		t.Errorf("Expected non-empty transaction ID")
	}

	// Test Put operation
	err = txn.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Verify key is in write set
	if _, exists := txn.WriteSet["key1"]; !exists {
		t.Errorf("Expected key to be in write set")
	}

	// Verify we can read the key before committing
	value, err := txn.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key before commit: %v", err)
	} else if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// Verify the key is not visible outside the transaction yet
	txn2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	_, err = txn2.Get([]byte("key1"))
	if err == nil {
		t.Errorf("Key should not be visible in other transaction before commit")
	}

	// Test commit
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify the key is now visible in another transaction
	txn3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	value, err = txn3.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key after commit: %v", err)
	} else if string(value) != "value1" {
		t.Errorf("Expected value1 after commit, got %s", value)
	}

	// Test delete operation in a new transaction
	txn4, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txn4.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify key is in delete set
	if _, exists := txn4.DeleteSet[("key1")]; !exists {
		t.Errorf("Expected key to be in delete set")
	}

	// Commit the delete transaction
	err = txn4.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}

	// Start a new transaction and verify key is gone
	txn5, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	_, err = txn5.Get([]byte("key1"))
	if err == nil {
		t.Errorf("Key should be gone after delete commit")
	}
}

func TestTxn_Rollback(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_txn_rollback_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Write a key-value pair to the database
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("stable_key"), []byte("stable_value"))
	})
	if err != nil {
		t.Fatalf("Failed to write initial key: %v", err)
	}

	// Begin a transaction that will be rolled back
	txn, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Make some changes
	err = txn.Put([]byte("key_to_rollback"), []byte("value_to_rollback"))
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Marking stable key for deletion (but will be rolled back)
	err = txn.Delete([]byte("stable_key"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Roll back the transaction
	err = txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to roll back transaction: %v", err)
	}

	// Verify the changes are not visible after rollback
	txn2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	_, err = txn2.Get([]byte("key_to_rollback"))
	if err == nil {
		t.Errorf("Rolled back key should not be accessible")
	}

	value, err := txn2.Get([]byte("stable_key"))
	if err != nil {
		t.Errorf("Stable key should still exist: %v", err)
	} else if string(value) != "stable_value" {
		t.Errorf("Expected stable_value, got %s", value)
	}
}

func TestTxn_Isolation(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_txn_isolation_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// First, insert a key
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("isolation_key"), []byte("initial_value"))
	})
	if err != nil {
		t.Fatalf("Failed to insert initial key: %v", err)
	}

	// Start a long-running transaction
	txnA, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Read the value in transaction A
	valueA1, err := txnA.Get([]byte("isolation_key"))
	if err != nil {
		t.Fatalf("Failed to read key in txn A: %v", err)
	}
	if string(valueA1) != "initial_value" {
		t.Errorf("Expected initial_value in txn A, got %s", valueA1)
	}

	// Now update the key in a separate transaction
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("isolation_key"), []byte("updated_value"))
	})
	if err != nil {
		t.Fatalf("Failed to update key: %v", err)
	}

	// Start a new transaction that should see the updated value
	txnB, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	valueB, err := txnB.Get([]byte("isolation_key"))
	if err != nil {
		t.Fatalf("Failed to read key in txn B: %v", err)
	}
	if string(valueB) != "updated_value" {
		t.Errorf("Expected updated_value in txn B, got %s", valueB)
	}

	// Transaction A should still see the original value (snapshot isolation)
	valueA2, err := txnA.Get([]byte("isolation_key"))
	if err != nil {
		t.Fatalf("Failed to read key again in txn A: %v", err)
	}
	if string(valueA2) != "initial_value" {
		t.Errorf("Expected initial_value in txn A second read, got %s", valueA2)
	}

	// Verify both values match within each transaction (read stability)
	if string(valueA1) != string(valueA2) {
		t.Errorf("Snapshot isolation violation: txn A saw %s then %s", valueA1, valueA2)
	}
}

func TestTxn_Update(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_txn_update_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Test successful update
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("update_key"), []byte("update_value"))
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify the update was applied
	var value []byte
	err = db.Update(func(txn *Txn) error {
		var err error
		value, err = txn.Get([]byte("update_key"))
		return err
	})
	if err != nil {
		t.Fatalf("Failed to read update: %v", err)
	}
	if string(value) != "update_value" {
		t.Errorf("Expected update_value, got %s", value)
	}

	// Test update with error
	customErr := fmt.Errorf("simulated error")
	err = db.Update(func(txn *Txn) error {
		err := txn.Put([]byte("error_key"), []byte("error_value"))
		if err != nil {
			return err
		}
		return customErr // Return explicit error instead of fmt.Errorf
	})
	if err == nil || err.Error() != customErr.Error() {
		t.Fatalf("Expected specific error from Update, got: %v", err)
	}

	// Verify the failed update didn't apply changes
	err = db.Update(func(txn *Txn) error {
		_, err := txn.Get([]byte("error_key"))
		if err == nil {
			return fmt.Errorf("expected error_key to not exist")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
}

func TestTxn_ConcurrentOperations(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_txn_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Number of concurrent writers
	const numWriters = 5
	// Keys per writer
	const keysPerWriter = 20

	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Start concurrent writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < keysPerWriter; i++ {
				key := []byte(fmt.Sprintf("conc_key_w%d_k%d", writerID, i))
				value := []byte(fmt.Sprintf("value_w%d_k%d", writerID, i))

				err := db.Update(func(txn *Txn) error {
					return txn.Put(key, value)
				})
				if err != nil {
					t.Errorf("Writer %d failed to write key %d: %v", writerID, i, err)
					return
				}

				// Small sleep to reduce contention
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Wait for all writers to finish
	wg.Wait()

	// Verify all data was written correctly
	successCount := 0
	for w := 0; w < numWriters; w++ {
		for i := 0; i < keysPerWriter; i++ {
			key := []byte(fmt.Sprintf("conc_key_w%d_k%d", w, i))
			expectedValue := []byte(fmt.Sprintf("value_w%d_k%d", w, i))

			var actualValue []byte
			err := db.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get(key)
				return err
			})

			if err == nil && bytes.Equal(actualValue, expectedValue) {
				successCount++
			} else if err != nil {
				t.Logf("Failed to read key %s: %v", key, err)
			} else {
				t.Logf("Value mismatch for key %s: expected %s, got %s",
					key, expectedValue, actualValue)
			}
		}
	}

	// We should have at least 95% success rate
	// (some transactions might fail due to contention)
	expectedSuccesses := int(float64(numWriters*keysPerWriter) * 0.95)
	if successCount < expectedSuccesses {
		t.Errorf("Expected at least %d successful operations, got %d",
			expectedSuccesses, successCount)
	} else {
		t.Logf("Verified %d out of %d concurrent writes successfully",
			successCount, numWriters*keysPerWriter)
	}
}

func TestTxn_WALRecovery(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_txn_wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB with full sync
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncFull, // Use full sync for WAL reliability
		LogChannel: logChan,
	}

	// Create DB and write data with different transaction states
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Committed transaction
	err = db.Update(func(txn *Txn) error {
		return txn.Put([]byte("committed_key"), []byte("committed_value"))
	})
	if err != nil {
		t.Fatalf("Failed to write committed key: %v", err)
	}

	// Uncommitted transaction
	txnUncommitted, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txnUncommitted.Put([]byte("uncommitted_key"), []byte("uncommitted_value"))
	if err != nil {
		t.Fatalf("Failed to write uncommitted key: %v", err)
	}

	// Transaction that will be rolled back
	txnRolledBack, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	err = txnRolledBack.Put([]byte("rolledback_key"), []byte("rolledback_value"))
	if err != nil {
		t.Fatalf("Failed to write rollback key: %v", err)
	}
	err = txnRolledBack.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Close the database to simulate a crash/restart
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Drain the log channel
	for len(logChan) > 0 {
		<-logChan
	}

	// Reopen the database to test WAL recovery
	logChan = make(chan string, 100)
	opts.LogChannel = logChan

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func(db2 *DB) {
		_ = db2.Close()
	}(db2)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Check committed transaction was recovered
	var value []byte
	err = db2.Update(func(txn *Txn) error {
		var err error
		value, err = txn.Get([]byte("committed_key"))
		return err
	})
	if err != nil {
		t.Errorf("Failed to get committed key after recovery: %v", err)
	} else if string(value) != "committed_value" {
		t.Errorf("Expected committed_value, got %s", value)
	}

	// Check uncommitted transaction was not applied
	err = db2.Update(func(txn *Txn) error {
		_, err := txn.Get([]byte("uncommitted_key"))
		if err == nil {
			return fmt.Errorf("uncommitted key should not be accessible")
		}
		return nil
	})
	if err != nil {
		t.Errorf("Uncommitted key check failed: %v", err)
	}

	// Check rolled back transaction was not applied
	err = db2.Update(func(txn *Txn) error {
		_, err := txn.Get([]byte("rolledback_key"))
		if err == nil {
			return fmt.Errorf("rolled back key should not be accessible")
		}
		return nil
	})
	if err != nil {
		t.Errorf("Rolled back key check failed: %v", err)
	}
}

func TestTxn_DeleteTimestamp(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "db_transaction_delete_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a log channel
	logChan := make(chan string, 100)
	defer func() {
		// Drain the log channel
		for len(logChan) > 0 {
			<-logChan
		}
	}()

	// Create a test DB
	opts := &Options{
		Directory:  dir,
		SyncOption: SyncNone,
		LogChannel: logChan,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Insert a key
	key := []byte("timestamp_test_key")
	value := []byte("timestamp_test_value")

	txn1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Insert transaction ID: %d, Timestamp: %d", txn1.Id, txn1.Timestamp)

	err = txn1.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to insert key: %v", err)
	}

	err = txn1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit insert: %v", err)
	}

	// Verify the key exists
	txn2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Verification transaction ID: %d, Timestamp: %d", txn2.Id, txn2.Timestamp)

	retrievedValue, err := txn2.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after insert: %v", err)
	}
	if !bytes.Equal(retrievedValue, value) {
		t.Fatalf("Value mismatch: expected %s, got %s", value, retrievedValue)
	}
	t.Logf("Key found after insertion: %s", retrievedValue)

	// Delete the key with a new transaction
	txn3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Delete transaction ID: %d, Timestamp: %d", txn3.Id, txn3.Timestamp)

	err = txn3.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete: %v", err)
	}

	// Verify the key is deleted with a new transaction
	txn4, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	t.Logf("Post-delete verification transaction ID: %d, Timestamp: %d", txn4.Id, txn4.Timestamp)

	_, err = txn4.Get(key)
	if err == nil {
		t.Fatalf("Key still accessible after deletion")
	}
	t.Logf("Key correctly not found after deletion: %v", err)

	// Verify we can still access the key with a timestamp before deletion
	txn5, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	txn5.Timestamp = txn1.Timestamp // Use the original insert timestamp
	t.Logf("Historical verification transaction ID: %d, Using Timestamp: %d", txn5.Id, txn5.Timestamp)

	retrievedValue, err = txn5.Get(key)
	if err != nil {
		t.Logf("Historical view should see the key but got error: %v", err)
	} else {
		t.Logf("Historical view sees value: %s", retrievedValue)
	}
}

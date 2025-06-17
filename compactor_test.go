package wildcat

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompactor_KeyRangeComparison(t *testing.T) {
	// Create mock up of SSTables with different key ranges
	tables := []*SSTable{
		{
			Id:   1,
			Min:  []byte("apple"),
			Max:  []byte("banana"),
			Size: 1000,
		},
		{
			Id:   2,
			Min:  []byte("cat"),
			Max:  []byte("zebra"), // Larger range
			Size: 1000,
		},
		{
			Id:   3,
			Min:  []byte("dog"),
			Max:  []byte("elephant"), // Smaller range
			Size: 1000,
		},
		{
			Id:   4,
			Min:  []byte("apple"),
			Max:  []byte("apple"), // Same key (smallest range)
			Size: 1000,
		},
	}

	t.Log("Testing bytes.Compare behavior:")
	for _, table := range tables {
		comparison := bytes.Compare(table.Max, table.Min)
		t.Logf("Table %d: Min=%s, Max=%s, bytes.Compare(Max,Min)=%d",
			table.Id, string(table.Min), string(table.Max), comparison)
	}

	// Simulate the compactor's selection logic
	var selectedTable *SSTable
	var smallestRange int

	for i, table := range tables {
		keyRangeSize := bytes.Compare(table.Max, table.Min)

		if i == 0 || keyRangeSize < smallestRange {
			smallestRange = keyRangeSize
			selectedTable = table
		}

		if selectedTable != nil {
			t.Logf("Table %d: keyRangeSize=%d, current smallest=%d, selected=%d",
				table.Id, keyRangeSize, smallestRange, selectedTable.Id)
		}
	}

	if selectedTable == nil {
		t.Fatalf("No table selected, this should not happen")

	}

	t.Logf("Final selection: Table %d with range size %d", selectedTable.Id, smallestRange)

	selectedTable = nil
	var actualSmallestRange int

	for i, table := range tables {

		// Calculate actual lexicographic distance
		rangeSize := len(table.Max) - len(table.Min)
		if bytes.Equal(table.Max, table.Min) {
			rangeSize = 0 // Same key = no range
		}

		if i == 0 || rangeSize < actualSmallestRange {
			actualSmallestRange = rangeSize
			selectedTable = table
		}

		if selectedTable != nil {
			t.Logf("Table %d: actual range size=%d, current smallest=%d, selected=%d",
				table.Id, rangeSize, actualSmallestRange, selectedTable.Id)
		}
	}

	if selectedTable == nil {
		t.Fatalf("No table selected, this should not happen")

	}

	t.Logf("Better selection: Table %d with actual range size %d", selectedTable.Id, actualSmallestRange)
}

func TestCompactor_SizeTieredRatio(t *testing.T) {

	// Create SSTables with different size relationships
	tables := []*SSTable{
		{Id: 1, Size: 100}, // Base size
		{Id: 2, Size: 120}, // 1.2x - should be included
		{Id: 3, Size: 149}, // 1.49x - should be included
		{Id: 4, Size: 151}, // 1.51x - should be excluded
		{Id: 5, Size: 200}, // 2.0x - should be excluded
	}

	// Simulate the size-tiered selection logic
	baseSize := tables[0].Size
	var similarSized []*SSTable
	maxBatchSize := 10

	similarSized = append(similarSized, tables[0])

	for i := 1; i < len(tables); i++ {
		ratio := float64(tables[i].Size) / float64(baseSize)
		included := ratio <= 1.5 && len(similarSized) < maxBatchSize

		if included {
			similarSized = append(similarSized, tables[i])
		}

		t.Logf("Table %d: size=%d, ratio=%.2f, included=%v",
			tables[i].Id, tables[i].Size, ratio, included)
	}

	t.Logf("Selected %d tables for size-tiered compaction", len(similarSized))

	ratios := []float64{1.2, 1.5, 2.0}
	for _, testRatio := range ratios {
		var selected []*SSTable
		selected = append(selected, tables[0])

		for i := 1; i < len(tables); i++ {
			ratio := float64(tables[i].Size) / float64(baseSize)
			if ratio <= testRatio {
				selected = append(selected, tables[i])
			}
		}

		t.Logf("With ratio %.1f: selected %d tables", testRatio, len(selected))
	}
}

func TestCompactor_ScoreCalculation(t *testing.T) {

	writeBufferSize := int64(64 * 1024 * 1024) // 64MB
	capacity := writeBufferSize * 10           // 640MB for level 1
	sizeThreshold := 8
	sizeWeight := 0.8
	countWeight := 0.2

	testCases := []struct {
		name         string
		currentSize  int64
		sstableCount int
	}{
		{"Empty level", 0, 0},
		{"Half full, few files", capacity / 2, 3},
		{"Full size, normal files", capacity, 5},
		{"Overfull size", capacity * 2, 6},
		{"Normal size, too many files", capacity / 2, 12},
		{"Both triggers", capacity * 2, 15},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sizeScore := float64(tc.currentSize) / float64(capacity)
			countScore := float64(tc.sstableCount) / float64(sizeThreshold)

			totalScore := sizeScore*sizeWeight + countScore*countWeight
			shouldCompact := totalScore > 1.0

			t.Logf("Size: %d/%d (%.2f), Count: %d/%d (%.2f)",
				tc.currentSize, capacity, sizeScore,
				tc.sstableCount, sizeThreshold, countScore)
			t.Logf("Total score: %.2f, Should compact: %v", totalScore, shouldCompact)

			if tc.currentSize > capacity && tc.sstableCount > sizeThreshold && !shouldCompact {
				t.Errorf("Expected compaction when both size and count exceed thresholds")
			}
		})
	}
}

func TestCompactor_Basic(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_compactor_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	t.Log("Inserting data to trigger flushing...")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Every 10 keys, insert a larger value to trigger flush
		if i%10 == 0 {
			largeValue := make([]byte, opts.WriteBufferSize)
			for j := range largeValue {
				largeValue[j] = byte(j % 256)
			}

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(fmt.Sprintf("large_key_%d", i)), largeValue)
			})
			if err != nil {
				t.Fatalf("Failed to insert large value: %v", err)
			}

			// Allow time for flush operations
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for background operations
	time.Sleep(500 * time.Millisecond)

	// Verify SSTables were created in level 1
	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	if sstables == nil || len(*sstables) == 0 {
		t.Fatalf("Expected at least one SSTable in level 1, found none")
	}

	t.Logf("Found %d SSTables in level 1", len(*sstables))

	// Verify we can read the data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d", i)

		var actualValue []byte
		err = db.View(func(txn *Txn) error {
			var err error
			actualValue, err = txn.Get([]byte(key))
			return err
		})

		if err != nil {
			t.Errorf("Failed to read key %s: %v", key, err)
		} else if string(actualValue) != expectedValue {
			t.Errorf("For key %s expected value %s, got %s", key, expectedValue, actualValue)
		}
	}

	// Trigger a compaction by forcing a specific compaction if we have enough tables
	if len(*sstables) >= 2 {
		t.Log("Manually triggering compaction...")

		// Take the first 2 SSTables for compaction and mark them as merging
		tablesToCompact := (*sstables)[:2]

		// Reserve the tables for merging
		for _, table := range tablesToCompact {
			atomic.StoreInt32(&table.isMerging, 1)
		}

		err = db.compactor.compactSSTables(tablesToCompact, 0, 1)
		if err != nil {
			// Release the tables if compaction failed
			for _, table := range tablesToCompact {
				atomic.StoreInt32(&table.isMerging, 0)
			}
			t.Fatalf("Manual compaction failed: %v", err)
		}

		// Wait for compaction to complete
		time.Sleep(300 * time.Millisecond)

		// Verify compaction result - check level 2
		level2 := (*levels)[1]
		level2Tables := level2.sstables.Load()

		if level2Tables == nil || len(*level2Tables) == 0 {
			t.Errorf("Expected at least one SSTable in level 2 after compaction")
		} else {
			t.Logf("Found %d SSTables in level 2 after compaction", len(*level2Tables))
		}
	} else {
		t.Log("Not enough SSTables for manual compaction test")
	}
}

func TestCompactor_LeveledCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_leveled_compaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024, // Small buffer to force flushing
		LevelCount:      4,        // Fewer levels for testing
		LevelMultiplier: 2,        // Smaller multiplier for testing
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

	// Helper function to list SSTables in a level
	countSSTables := func(levelNum int) int {
		levels := db.levels.Load()
		if levels == nil || levelNum >= len(*levels) {
			return 0
		}

		level := (*levels)[levelNum]
		sstables := level.sstables.Load()
		if sstables == nil {
			return 0
		}
		return len(*sstables)
	}

	// Insert data in sorted chunks to create multiple SSTables
	// This helps ensure predictable SSTable boundaries
	batches := 5
	keysPerBatch := 20

	for batch := 0; batch < batches; batch++ {
		// Each batch has a different key prefix to help create distinct SSTables
		prefix := fmt.Sprintf("batch%d_", batch)

		for i := 0; i < keysPerBatch; i++ {
			key := fmt.Sprintf("%skey%03d", prefix, i)
			value := fmt.Sprintf("value%03d", i)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Force a flush after each batch
		largeValue := make([]byte, opts.WriteBufferSize)
		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(fmt.Sprintf("large_key_batch%d", batch)), largeValue)
		})
		if err != nil {
			t.Fatalf("Failed to insert large value: %v", err)
		}

		// Allow time for flush operations
		time.Sleep(200 * time.Millisecond)

		t.Logf("After batch %d: L1=%d, L2=%d, L3=%d",
			batch, countSSTables(0), countSSTables(1), countSSTables(2))
	}

	// Now force compactions by setting level sizes past their thresholds
	levels := db.levels.Load()
	if levels != nil && len(*levels) >= 2 {
		level1 := (*levels)[0]
		l1Tables := level1.sstables.Load()

		if l1Tables != nil && len(*l1Tables) >= 2 {
			// Manually force the level size to trigger compaction
			atomic.StoreInt64(&level1.currentSize, int64(level1.capacity*2))

			// Manually trigger compaction scoring
			db.compactor.scoreLock.Lock()
			db.compactor.lastCompaction = time.Now().Add(-2 * db.opts.CompactionCooldownPeriod)
			db.compactor.scoreLock.Unlock()

			// Check and schedule compactions
			db.compactor.checkAndScheduleCompactions()

			// Execute any pending compactions
			db.compactor.executeCompactions()

			// Allow time for compaction to complete
			time.Sleep(500 * time.Millisecond)

			// Log the state after forced compaction
			t.Logf("After forced compaction: L1=%d, L2=%d, L3=%d",
				countSSTables(0), countSSTables(1), countSSTables(2))
		}
	}

	// Verify all keys are still readable
	for batch := 0; batch < batches; batch++ {
		prefix := fmt.Sprintf("batch%d_", batch)

		for i := 0; i < keysPerBatch; i++ {
			key := fmt.Sprintf("%skey%03d", prefix, i)
			expectedValue := fmt.Sprintf("value%03d", i)

			var actualValue []byte
			err = db.Update(func(txn *Txn) error {
				var err error
				actualValue, err = txn.Get([]byte(key))
				return err
			})

			if err != nil {
				t.Errorf("Failed to read key %s after compaction: %v", key, err)
			} else if string(actualValue) != expectedValue {
				t.Errorf("For key %s expected value %s, got %s after compaction",
					key, expectedValue, actualValue)
			}
		}
	}
}

func TestCompactor_SizeTieredCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_size_tiered_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncNone,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create multiple SSTables with similar sizes in L1
	for j := 0; j < db.opts.CompactionSizeThreshold+1; j++ {

		// Each iteration creates one SSTable
		valueSize := 50 // Keep values similar in size

		// Write enough key-value pairs to fill an SSTable
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("st_batch%d_key%03d", j, i)
			value := make([]byte, valueSize)
			// Fill with a recognizable pattern
			for k := range value {
				value[k] = byte((i + j) % 256)
			}

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), value)
			})
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Force a flush
		err = db.ForceFlush()
		if err != nil {
			t.Fatalf("Failed to force flush: %v", err)
		}

		// Log current state
		t.Logf("Created SSTable %d/%d", j+1, db.opts.CompactionSizeThreshold+1)
	}

	// Verify L1 has enough SSTables
	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	if sstables == nil {
		t.Fatalf("Level 1 SSTables not initialized")
	}

	if len(*sstables) < db.opts.CompactionSizeThreshold {
		t.Fatalf("Expected at least %d SSTables in level 1, found %d",
			db.opts.CompactionSizeThreshold, len(*sstables))
	}

	t.Logf("Found %d SSTables in level 1, threshold is %d",
		len(*sstables), db.opts.CompactionSizeThreshold)

	// Force a size-tiered compaction by manually scheduling it
	if len(*sstables) >= db.opts.CompactionSizeThreshold {
		// Sort tables by size to simulate size-tiered selection
		sortedTables := make([]*SSTable, len(*sstables))
		copy(sortedTables, *sstables)

		sort.Slice(sortedTables, func(i, j int) bool {
			return sortedTables[i].Size < sortedTables[j].Size
		})

		// Select tables for compaction (at least 2)
		numToCompact := min(db.opts.CompactionBatchSize, len(sortedTables))
		if numToCompact < 2 {
			numToCompact = 2
		}

		// Take the smallest tables for compaction
		tablesToCompact := sortedTables[:numToCompact]

		// Mark tables as merging
		for _, table := range tablesToCompact {
			atomic.StoreInt32(&table.isMerging, 1)
		}

		t.Logf("Manually triggering size-tiered compaction with %d tables...", numToCompact)

		// Perform compaction
		err = db.compactor.compactSSTables(tablesToCompact, 0, 1)
		if err != nil {
			// Release tables if compaction failed
			for _, table := range tablesToCompact {
				atomic.StoreInt32(&table.isMerging, 0)
			}
			t.Fatalf("Manual size-tiered compaction failed: %v", err)
		}

		// Verify level 2 has a new SSTable from compaction
		level2 := (*levels)[1]
		level2Tables := level2.sstables.Load()

		if level2Tables == nil || len(*level2Tables) == 0 {
			t.Errorf("Expected at least one SSTable in level 2 after size-tiered compaction")
		} else {
			t.Logf("Found %d SSTables in level 2 after size-tiered compaction", len(*level2Tables))
		}
	}

	// Verify all keys are still readable (sample a few)
	for j := 0; j < db.opts.CompactionSizeThreshold+1; j += 2 {
		for i := 0; i < 50; i += 10 {
			key := fmt.Sprintf("st_batch%d_key%03d", j, i)

			var value []byte
			err = db.View(func(txn *Txn) error {
				var err error
				value, err = txn.Get([]byte(key))
				return err
			})

			if err != nil {
				t.Errorf("Failed to read key %s after compaction: %v", key, err)
				continue
			}

			// Verify value has correct pattern
			for k := range value {
				expected := byte((i + j) % 256)
				if value[k] != expected {
					t.Errorf("For key %s, value corruption at byte %d: expected %d, got %d",
						key, k, expected, value[k])
					break
				}
			}
		}
	}
}

func TestCompactor_CompactionQueue(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_compaction_queue_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
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

	// Insert enough data to create SSTables
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("qkey%03d", i)
		value := fmt.Sprintf("qvalue%03d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Force a flush occasionally
		if i%50 == 0 {
			largeValue := make([]byte, opts.WriteBufferSize)
			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(fmt.Sprintf("qlarge_%d", i)), largeValue)
			})
			if err != nil {
				t.Fatalf("Failed to insert large value: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for background operations
	time.Sleep(300 * time.Millisecond)

	// Get level 1 SSTables
	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()

	if sstables == nil {
		t.Fatalf("Level 1 SSTables not initialized")
	}

	if len(*sstables) < 2 {
		t.Fatalf("Expected at least 2 SSTables in level 1, found %d", len(*sstables))
	}

	// Create and queue several mock compaction jobs with different priorities
	db.compactor.scoreLock.Lock()

	// Clear any existing jobs
	db.compactor.compactionQueue = make([]*compactorJob, 0)

	// Add jobs with different priorities
	jobs := []struct {
		level    int
		priority float64
		tables   []*SSTable
	}{
		{1, 0.5, (*sstables)[:1]},
		{1, 2.0, (*sstables)[:2]}, // Highest priority
		{2, 0.8, (*sstables)[:1]},
		{1, 1.5, (*sstables)[:1]},
	}

	for _, job := range jobs {
		db.compactor.compactionQueue = append(db.compactor.compactionQueue, &compactorJob{
			levelIdx:   job.level,
			priority:   job.priority,
			ssTables:   job.tables,
			targetIdx:  job.level + 1,
			inProgress: false,
		})
	}

	// Sort the queue by priority (highest first)
	sort.Slice(db.compactor.compactionQueue, func(i, j int) bool {
		return db.compactor.compactionQueue[i].priority > db.compactor.compactionQueue[j].priority
	})

	db.compactor.scoreLock.Unlock()

	// Verify the job queue order
	db.compactor.scoreLock.Lock()
	if len(db.compactor.compactionQueue) != len(jobs) {
		t.Errorf("Expected %d jobs in queue, found %d", len(jobs), len(db.compactor.compactionQueue))
	} else {
		// The highest priority should be first
		highestPriority := db.compactor.compactionQueue[0].priority
		if highestPriority != 2.0 {
			t.Errorf("Expected highest priority job (2.0) to be first, got %f", highestPriority)
		}

		// Check the full ordering
		expectedPriorities := []float64{2.0, 1.5, 0.8, 0.5}
		actualPriorities := make([]float64, len(db.compactor.compactionQueue))
		for i, job := range db.compactor.compactionQueue {
			actualPriorities[i] = job.priority
		}

		t.Logf("Compaction queue priorities: %v (expected order: %v)",
			actualPriorities, expectedPriorities)

		// Check if first two jobs are in correct order
		if len(actualPriorities) >= 2 && actualPriorities[0] < actualPriorities[1] {
			t.Errorf("Job priorities not properly ordered: %v", actualPriorities)
		}
	}
	db.compactor.scoreLock.Unlock()

	// Now execute one compaction job
	db.compactor.executeCompactions()

	// Wait for it to complete
	time.Sleep(200 * time.Millisecond)

	// Verify a job was processed
	db.compactor.scoreLock.Lock()
	newQueueLength := len(db.compactor.compactionQueue)
	db.compactor.scoreLock.Unlock()

	// Ideally the queue should be smaller, but it depends on whether the job completed
	// during our test window - we're just verifying the mechanism works
	t.Logf("Compaction queue had %d jobs, now has %d jobs", len(jobs), newQueueLength)
}

func TestCompactor_ConcurrentCompactions(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_concurrent_compaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
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

	// Override max concurrency for testing
	db.compactor.maxConcurrency = 3

	// Prepare by creating multiple SSTables across levels
	// We'll create separate batches with different key prefixes for different levels
	keyPrefixes := []string{"l1_", "l2_", "l3_"}

	for _, prefix := range keyPrefixes {
		// Each prefix creates its own batch of SSTables
		for batch := 0; batch < 3; batch++ {
			batchPrefix := fmt.Sprintf("%sbatch%d_", prefix, batch)

			// Insert data
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("%skey%03d", batchPrefix, i)
				value := fmt.Sprintf("%svalue%03d", batchPrefix, i)

				err = db.Update(func(txn *Txn) error {
					return txn.Put([]byte(key), []byte(value))
				})
				if err != nil {
					t.Fatalf("Failed to insert data: %v", err)
				}
			}

			// Force a flush
			largeValue := make([]byte, opts.WriteBufferSize)
			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(fmt.Sprintf("%slarge", batchPrefix)), largeValue)
			})
			if err != nil {
				t.Fatalf("Failed to insert large value: %v", err)
			}

			// Allow flush to complete
			time.Sleep(100 * time.Millisecond)
		}

		// After creating SSTables with one prefix, force some compactions to move
		// data to the next level
		if prefix == "l1_" {
			// Force compaction of level 1 -> level 2
			levels := db.levels.Load()
			if levels != nil && len(*levels) >= 2 {
				level1 := (*levels)[0]
				sstables := level1.sstables.Load()
				if sstables != nil && len(*sstables) >= 2 {
					err = db.compactor.compactSSTables((*sstables)[:2], 1, 2)
					if err != nil {
						t.Logf("Level 1->2 compaction failed: %v", err)
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		} else if prefix == "l2_" {
			// Force compaction of level 2 -> level 3
			levels := db.levels.Load()
			if levels != nil && len(*levels) >= 3 {
				level2 := (*levels)[1]
				sstables := level2.sstables.Load()
				if sstables != nil && len(*sstables) >= 2 {
					err = db.compactor.compactSSTables((*sstables)[:2], 2, 3)
					if err != nil {
						t.Logf("Level 2->3 compaction failed: %v", err)
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
		}
	}

	// Helper function to safely access compaction queue
	safeQueueAccess := func(accessFunc func()) {
		db.compactor.scoreLock.Lock()
		defer db.compactor.scoreLock.Unlock()
		accessFunc()
	}

	// Queue multiple compaction jobs manually
	var initialQueueSize int
	safeQueueAccess(func() {
		db.compactor.activeJobs = 0
		db.compactor.compactionQueue = make([]*compactorJob, 0)

		levels := db.levels.Load()
		if levels != nil {

			// For each level that has SSTables, create a job
			for levelIdx := 0; levelIdx < len(*levels)-1; levelIdx++ {
				level := (*levels)[levelIdx]
				sstables := level.sstables.Load()

				if sstables != nil && len(*sstables) >= 2 {
					// Create a job for this level
					db.compactor.compactionQueue = append(db.compactor.compactionQueue, &compactorJob{
						levelIdx:   levelIdx + 1,
						priority:   float64(levelIdx + 1), // Higher levels have higher priority
						ssTables:   (*sstables)[:2],       // Use first two tables
						targetIdx:  levelIdx + 2,
						inProgress: false,
					})
				}
			}
		}
		initialQueueSize = len(db.compactor.compactionQueue)
	})

	t.Logf("Queued %d compaction jobs for concurrent execution", initialQueueSize)

	// Execute compactions concurrently
	for i := 0; i < db.compactor.maxConcurrency; i++ {
		db.compactor.executeCompactions()
		time.Sleep(50 * time.Millisecond) // Short delay to let job marking happen
	}

	// Check that we have the expected number of active jobs
	activeJobs := atomic.LoadInt32(&db.compactor.activeJobs)
	expectedActive := min(int32(initialQueueSize), int32(db.compactor.maxConcurrency))

	t.Logf("Active compaction jobs: %d (expected around %d based on queue size and concurrency)",
		activeJobs, expectedActive)

	// Allow compactions to complete
	time.Sleep(500 * time.Millisecond)

	// Verify active jobs counter decremented properly
	finalActiveJobs := atomic.LoadInt32(&db.compactor.activeJobs)
	if finalActiveJobs > expectedActive {
		t.Errorf("Active jobs counter not decremented properly: %d", finalActiveJobs)
	} else {
		t.Logf("Compactions completed, final active jobs: %d", finalActiveJobs)
	}

	// Verify we can still read data from all levels
	for _, prefix := range keyPrefixes {
		for batch := 0; batch < 3; batch++ {
			batchPrefix := fmt.Sprintf("%sbatch%d_", prefix, batch)

			// Sample a few keys from each batch
			for i := 0; i < 50; i += 10 {
				key := fmt.Sprintf("%skey%03d", batchPrefix, i)
				expectedValue := fmt.Sprintf("%svalue%03d", batchPrefix, i)

				var actualValue []byte
				err = db.Update(func(txn *Txn) error {
					var err error
					actualValue, err = txn.Get([]byte(key))
					if err != nil {
						return err
					}
					return nil
				})

				if err != nil {
					t.Logf("Warning: Could not read key %s after compaction: %v", key, err)
				} else if string(actualValue) != expectedValue {
					t.Errorf("For key %s expected value %s, got %s", key, expectedValue, actualValue)
				}
			}
		}
	}
}

func TestCompactor_LastLevelPartitioning_Detection(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_last_level_detection_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
		LevelCount:      4,
		LevelMultiplier: 2,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	// Test shouldPartitionLastLevel logic
	lastLevelIdx := len(*levels) - 1
	lastLevel := (*levels)[lastLevelIdx]

	// Initially should not need partitioning
	if db.compactor.shouldPartitionLastLevel(lastLevel) {
		t.Errorf("Empty last level should not need partitioning")
	}

	originalCapacity := lastLevel.capacity
	atomic.StoreInt64(&lastLevel.currentSize, int64(originalCapacity+1000))

	if !db.compactor.shouldPartitionLastLevel(lastLevel) {
		t.Errorf("Last level exceeding capacity should need partitioning")
	}

	atomic.StoreInt64(&lastLevel.currentSize, 0)

	t.Logf("Last level detection test passed - capacity: %d", originalCapacity)
}

func TestCompactor_LastLevelPartitioning_KeyRangeGrouping(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_key_range_grouping_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create mock SSTables with different key ranges
	testTables := []*SSTable{
		{
			Id:   1,
			Min:  []byte("apple"),
			Max:  []byte("banana"),
			Size: 1000,
		},
		{
			Id:   2,
			Min:  []byte("banana"), // Overlaps with previous
			Max:  []byte("cherry"),
			Size: 1000,
		},
		{
			Id:   3,
			Min:  []byte("dog"), // No overlap - new group
			Max:  []byte("elephant"),
			Size: 1000,
		},
		{
			Id:   4,
			Min:  []byte("elephant"), // Overlaps with previous
			Max:  []byte("fox"),
			Size: 1000,
		},
		{
			Id:   5,
			Min:  []byte("zebra"), // No overlap - new group
			Max:  []byte("zulu"),
			Size: 1000,
		},
	}

	// Test key range grouping
	groups := db.compactor.groupSSTablesByKeyRange(testTables)

	expectedGroups := 3 // apple-cherry, dog-fox, zebra-zulu
	if len(groups) != expectedGroups {
		t.Errorf("Expected %d groups, got %d", expectedGroups, len(groups))
	}

	// Verify first group has overlapping tables
	if len(groups) > 0 {
		firstGroup := groups[0]
		if len(firstGroup) != 2 {
			t.Errorf("First group should have 2 tables, got %d", len(firstGroup))
		}

		if firstGroup[0].Id != 1 || firstGroup[1].Id != 2 {
			t.Errorf("First group should contain tables 1 and 2, got %d and %d",
				firstGroup[0].Id, firstGroup[1].Id)
		}
	}

	// Verify second group
	if len(groups) > 1 {
		secondGroup := groups[1]
		if len(secondGroup) != 2 {
			t.Errorf("Second group should have 2 tables, got %d", len(secondGroup))
		}

		if secondGroup[0].Id != 3 || secondGroup[1].Id != 4 {
			t.Errorf("Second group should contain tables 3 and 4, got %d and %d",
				secondGroup[0].Id, secondGroup[1].Id)
		}
	}

	// Verify third group
	if len(groups) > 2 {
		thirdGroup := groups[2]
		if len(thirdGroup) != 1 {
			t.Errorf("Third group should have 1 table, got %d", len(thirdGroup))
		}

		if thirdGroup[0].Id != 5 {
			t.Errorf("Third group should contain table 5, got %d", thirdGroup[0].Id)
		}
	}

	t.Logf("Key range grouping test passed - created %d groups as expected", len(groups))
}

func TestCompactor_LastLevelPartitioning_Distribution(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_distribution_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:                            dir,
		SyncOption:                           SyncFull,
		LogChannel:                           nil,
		WriteBufferSize:                      4 * 1024,
		CompactionPartitionDistributionRatio: 0.7, // 70% to L-1, 30% to L-2
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create mock SSTable groups with known sizes
	groups := [][]*SSTable{
		{
			{Id: 1, Size: 1000}, // Group 1: 1000 bytes
		},
		{
			{Id: 2, Size: 2000}, // Group 2: 2000 bytes
		},
		{
			{Id: 3, Size: 3000}, // Group 3: 3000 bytes
		},
		{
			{Id: 4, Size: 4000}, // Group 4: 4000 bytes
		},
	}
	// Total: 10000 bytes
	// 70% = 7000 bytes should go to L-1
	// 30% = 3000 bytes should go to L-2

	level1Tables, level2Tables := db.compactor.distributeTableGroups(groups)

	// Calculate actual distribution
	var level1Size, level2Size int64
	for _, table := range level1Tables {
		level1Size += table.Size
	}
	for _, table := range level2Tables {
		level2Size += table.Size
	}

	totalSize := level1Size + level2Size
	level1Ratio := float64(level1Size) / float64(totalSize)

	t.Logf("Distribution results:")
	t.Logf("  L-1: %d bytes (%d tables) - %.1f%%", level1Size, len(level1Tables), level1Ratio*100)
	t.Logf("  L-2: %d bytes (%d tables) - %.1f%%", level2Size, len(level2Tables), (1-level1Ratio)*100)

	// Verify distribution is approximately correct (within reasonable bounds)
	expectedRatio := 0.7
	tolerance := 0.2 // Allow 20% tolerance due to group-based distribution

	if level1Ratio < expectedRatio-tolerance || level1Ratio > expectedRatio+tolerance {
		t.Errorf("L-1 ratio %.2f is outside expected range %.2f ± %.2f",
			level1Ratio, expectedRatio, tolerance)
	}

	// Verify all tables are accounted for
	totalTables := len(level1Tables) + len(level2Tables)
	expectedTables := 4
	if totalTables != expectedTables {
		t.Errorf("Expected %d total tables, got %d", expectedTables, totalTables)
	}
}

func TestCompactor_LastLevelPartitioning_FullWorkflow(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_full_partitioning_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:                            dir,
		SyncOption:                           SyncFull,
		LogChannel:                           nil,
		WriteBufferSize:                      4 * 1024,
		LevelCount:                           4,
		LevelMultiplier:                      2,
		CompactionPartitionRatio:             0.6,                  // Move 60% of data
		CompactionPartitionDistributionRatio: 0.7,                  // 70% to L-1, 30% to L-2
		CompactionCooldownPeriod:             1 * time.Millisecond, // Short for testing
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	countSSTables := func() (int, int, int, int) {
		levels := db.levels.Load()
		if levels == nil {
			return 0, 0, 0, 0
		}

		counts := make([]int, 4)
		for i := 0; i < 4 && i < len(*levels); i++ {
			level := (*levels)[i]
			sstables := level.sstables.Load()
			if sstables != nil {
				counts[i] = len(*sstables)
			}
		}
		return counts[0], counts[1], counts[2], counts[3]
	}

	// Create data in multiple levels by inserting and forcing compactions
	t.Log("Step 1: Creating data across levels...")

	// Insert data to create SSTables in L1
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("batch%d_key%03d", batch, i)
			value := fmt.Sprintf("value%03d_batch%d", i, batch)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Force flush
		err = db.ForceFlush()
		if err != nil {
			t.Fatalf("Failed to force flush: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	l1, l2, l3, l4 := countSSTables()
	t.Logf("After initial data: L1=%d, L2=%d, L3=%d, L4=%d", l1, l2, l3, l4)

	// Force compactions to move data to deeper levels
	t.Log("Step 2: Moving data to deeper levels...")

	levels := db.levels.Load()
	if levels != nil && len(*levels) >= 4 {

		// Force L1 -> L2 compaction
		level1 := (*levels)[0]
		l1tables := level1.sstables.Load()
		if l1tables != nil && len(*l1tables) >= 2 {
			err = db.compactor.compactSSTables((*l1tables)[:2], 1, 2)
			if err != nil {
				t.Logf("L1->L2 compaction failed: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Force L2 -> L3 compaction
		level2 := (*levels)[1]
		l2tables := level2.sstables.Load()
		if l2tables != nil && len(*l2tables) >= 2 {
			err = db.compactor.compactSSTables((*l2tables)[:2], 2, 3)
			if err != nil {
				t.Logf("L2->L3 compaction failed: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Force L3 -> L4 compaction to populate last level
		level3 := (*levels)[2]
		l3tables := level3.sstables.Load()
		if l3tables != nil && len(*l3tables) >= 2 {
			err = db.compactor.compactSSTables((*l3tables)[:2], 3, 4)
			if err != nil {
				t.Logf("L3->L4 compaction failed: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	l1, l2, l3, l4 = countSSTables()
	t.Logf("After compactions: L1=%d, L2=%d, L3=%d, L4=%d", l1, l2, l3, l4)

	// Manually trigger last level capacity exceeded
	t.Log("Step 3: Triggering last level partitioning...")

	levels = db.levels.Load()
	if levels != nil && len(*levels) >= 4 {
		lastLevel := (*levels)[3] // L4

		// Simulate last level exceeding capacity..
		originalSize := atomic.LoadInt64(&lastLevel.currentSize)
		atomic.StoreInt64(&lastLevel.currentSize, int64(lastLevel.capacity)*2)

		t.Logf("Set last level size to %d (capacity: %d)",
			atomic.LoadInt64(&lastLevel.currentSize), lastLevel.capacity)

		// Check if partitioning is detected
		if !db.compactor.shouldPartitionLastLevel(lastLevel) {
			t.Errorf("Last level should need partitioning but doesn't")
		}

		// Reset compaction cooldown to allow immediate scheduling
		db.compactor.scoreLock.Lock()
		db.compactor.lastCompaction = time.Now().Add(-2 * db.opts.CompactionCooldownPeriod)
		db.compactor.scoreLock.Unlock()

		// Trigger scheduling
		db.compactor.checkAndScheduleCompactions()

		// Check if partitioning job was scheduled
		db.compactor.scoreLock.Lock()
		queueLength := len(db.compactor.compactionQueue)
		var partitioningJobFound bool
		for _, job := range db.compactor.compactionQueue {
			if job.targetIdx == -1 {
				partitioningJobFound = true
				t.Logf("Found partitioning job with %d SSTables, priority %.1f",
					len(job.ssTables), job.priority)
				break
			}
		}
		db.compactor.scoreLock.Unlock()

		if queueLength == 0 {
			t.Logf("No compaction jobs scheduled (this may be normal if no SSTables available)")
		} else if !partitioningJobFound {
			t.Errorf("Partitioning job not found in compaction queue")
		}

		// Execute any scheduled compactions
		if queueLength > 0 {
			t.Log("Executing partitioning job...")
			db.compactor.executeCompactions()
			time.Sleep(200 * time.Millisecond)

			// Check final state
			l1, l2, l3, l4 = countSSTables()
			t.Logf("After partitioning: L1=%d, L2=%d, L3=%d, L4=%d", l1, l2, l3, l4)
		}

		// Restore original size
		atomic.StoreInt64(&lastLevel.currentSize, originalSize)
	}

	// Verify data integrity
	t.Log("Step 4: Verifying data integrity...")

	sampleKeys := []string{
		"batch0_key000", "batch1_key010", "batch2_key020",
		"batch3_key030", "batch4_key040",
	}

	for _, key := range sampleKeys {
		var value []byte
		err = db.View(func(txn *Txn) error {
			var err error
			value, err = txn.Get([]byte(key))
			return err
		})

		if err != nil {
			t.Errorf("Failed to read key %s after partitioning: %v", key, err)
		} else {
			t.Logf("Successfully read key %s: %s", key, string(value))
		}
	}
}

func TestCompactor_LastLevelPartitioning_Scheduling(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_partitioning_scheduling_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:                dir,
		SyncOption:               SyncFull,
		LogChannel:               nil,
		WriteBufferSize:          4 * 1024,
		LevelCount:               4,
		CompactionCooldownPeriod: 1 * time.Millisecond,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Create mock SSTables for the last level
	levels := db.levels.Load()
	if levels == nil || len(*levels) < 3 {
		t.Fatalf("Need at least 3 levels for partitioning test")
	}

	lastLevelIdx := len(*levels) - 1
	lastLevel := (*levels)[lastLevelIdx]

	// Create mock SSTables
	mockTables := []*SSTable{
		{
			Id:        1,
			Min:       []byte("key001"),
			Max:       []byte("key100"),
			Size:      1000,
			Timestamp: time.Now().UnixNano() - 1000000, // Older
			isMerging: 0,
		},
		{
			Id:        2,
			Min:       []byte("key101"),
			Max:       []byte("key200"),
			Size:      1500,
			Timestamp: time.Now().UnixNano() - 500000, // Newer
			isMerging: 0,
		},
		{
			Id:        3,
			Min:       []byte("key201"),
			Max:       []byte("key300"),
			Size:      2000,
			Timestamp: time.Now().UnixNano() - 2000000, // Oldest
			isMerging: 0,
		},
	}

	// Set up the last level with mock tables..
	lastLevel.sstables.Store(&mockTables)
	totalSize := int64(1000 + 1500 + 2000)
	atomic.StoreInt64(&lastLevel.currentSize, totalSize*2) // Exceed capacity

	t.Logf("Set up last level with %d SSTables, total size %d", len(mockTables), totalSize*2)

	// Test scheduling
	db.compactor.scoreLock.Lock()
	db.compactor.lastCompaction = time.Now().Add(-2 * db.opts.CompactionCooldownPeriod)
	db.compactor.compactionQueue = make([]*compactorJob, 0) // Clear queue
	db.compactor.scoreLock.Unlock()

	// Schedule partitioning
	db.compactor.scheduleLastLevelPartitioning(lastLevel, lastLevelIdx)

	// Verify job was scheduled
	db.compactor.scoreLock.Lock()
	queueLength := len(db.compactor.compactionQueue)
	var partitionJob *compactorJob
	if queueLength > 0 {
		partitionJob = db.compactor.compactionQueue[0]
	}
	db.compactor.scoreLock.Unlock()

	if queueLength == 0 {
		t.Fatalf("No partitioning job was scheduled")
	}

	if partitionJob == nil {
		t.Fatalf("Partitioning job is nil, expected a valid job")

	}

	if partitionJob.targetIdx != -1 {
		t.Errorf("Expected targetLevel -1 for partitioning job, got %d", partitionJob.targetIdx)
	}

	if partitionJob.priority != 10.0 {
		t.Errorf("Expected priority 10.0 for partitioning job, got %.1f", partitionJob.priority)
	}

	if len(partitionJob.ssTables) == 0 {
		t.Errorf("Partitioning job has no SSTables")
	}

	t.Logf("Partitioning job scheduled successfully:")
	t.Logf("  Level: %d", partitionJob.levelIdx)
	t.Logf("  Priority: %.1f", partitionJob.priority)
	t.Logf("  SSTables: %d", len(partitionJob.ssTables))
	t.Logf("  Target Level: %d", partitionJob.targetIdx)

	// Verify SSTables are sorted by timestamp (oldest first)
	for i := 0; i < len(partitionJob.ssTables)-1; i++ {
		current := partitionJob.ssTables[i]
		next := partitionJob.ssTables[i+1]
		if current.Timestamp > next.Timestamp {
			t.Errorf("SSTables not sorted by timestamp: %d > %d",
				current.Timestamp, next.Timestamp)
		}
	}

	// Clean up -- reset merging flags
	for _, table := range partitionJob.ssTables {
		atomic.StoreInt32(&table.isMerging, 0)
	}
}

func TestCompactor_LastLevelPartitioning_EdgeCases(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_partitioning_edge_cases_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		LogChannel:      nil,
		WriteBufferSize: 4 * 1024,
		LevelCount:      4,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *DB) {
		_ = db.Close()
	}(db)

	// Empty last level
	t.Run("EmptyLastLevel", func(t *testing.T) {
		levels := db.levels.Load()
		lastLevel := (*levels)[len(*levels)-1]

		if db.compactor.shouldPartitionLastLevel(lastLevel) {
			t.Errorf("Empty last level should not need partitioning")
		}
	})

	// Last level with insufficient SSTables
	t.Run("InsufficientSSTables", func(t *testing.T) {
		levels := db.levels.Load()
		lastLevelIdx := len(*levels) - 1
		lastLevel := (*levels)[lastLevelIdx]

		// Set capacity exceeded but no SSTables
		atomic.StoreInt64(&lastLevel.currentSize, int64(lastLevel.capacity*2))
		lastLevel.sstables.Store(&[]*SSTable{})

		db.compactor.scheduleLastLevelPartitioning(lastLevel, lastLevelIdx)

		// Should not schedule anything
		db.compactor.scoreLock.Lock()
		queueLength := len(db.compactor.compactionQueue)
		db.compactor.scoreLock.Unlock()

		// Reset
		atomic.StoreInt64(&lastLevel.currentSize, 0)

		t.Logf("Queue length with no SSTables: %d", queueLength)
	})

	// All SSTables already being merged
	t.Run("AllTablesBeingMerged", func(t *testing.T) {
		levels := db.levels.Load()
		lastLevelIdx := len(*levels) - 1
		lastLevel := (*levels)[lastLevelIdx]

		// Create SSTables that are all being merged
		busyTables := []*SSTable{
			{Id: 1, Size: 1000, isMerging: 1}, // Already merging
			{Id: 2, Size: 1000, isMerging: 1}, // Already merging
		}

		lastLevel.sstables.Store(&busyTables)
		atomic.StoreInt64(&lastLevel.currentSize, int64(lastLevel.capacity*2))

		originalQueueLength := 0
		db.compactor.scoreLock.Lock()
		originalQueueLength = len(db.compactor.compactionQueue)
		db.compactor.scoreLock.Unlock()

		db.compactor.scheduleLastLevelPartitioning(lastLevel, lastLevelIdx)

		db.compactor.scoreLock.Lock()
		newQueueLength := len(db.compactor.compactionQueue)
		db.compactor.scoreLock.Unlock()

		if newQueueLength > originalQueueLength {
			t.Errorf("Should not schedule partitioning when all tables are being merged")
		}

		// Reset
		for _, table := range busyTables {
			atomic.StoreInt32(&table.isMerging, 0)
		}
		atomic.StoreInt64(&lastLevel.currentSize, 0)

		t.Logf("Queue length with busy tables: %d -> %d", originalQueueLength, newQueueLength)
	})

	// Very small partition ratio
	t.Run("SmallPartitionRatio", func(t *testing.T) {
		// Temporarily change partition ratio
		originalRatio := db.opts.CompactionPartitionRatio
		db.opts.CompactionPartitionRatio = 0.01 // Only 1%

		tables := []*SSTable{
			{Id: 1, Size: 10000, Timestamp: 1000},
		}

		groups := [][]*SSTable{tables}
		level1Tables, level2Tables := db.compactor.distributeTableGroups(groups)

		totalTables := len(level1Tables) + len(level2Tables)
		if totalTables != 1 {
			t.Errorf("Expected 1 total table, got %d", totalTables)
		}

		// Restore original ratio
		db.opts.CompactionPartitionRatio = originalRatio

		t.Logf("Small partition ratio test: L1=%d, L2=%d tables",
			len(level1Tables), len(level2Tables))
	})

	// Single overlapping group
	t.Run("SingleOverlappingGroup", func(t *testing.T) {
		overlappingTables := []*SSTable{
			{Id: 1, Min: []byte("a"), Max: []byte("m"), Size: 1000},
			{Id: 2, Min: []byte("f"), Max: []byte("r"), Size: 1000}, // Overlaps
			{Id: 3, Min: []byte("k"), Max: []byte("z"), Size: 1000}, // Overlaps
		}

		groups := db.compactor.groupSSTablesByKeyRange(overlappingTables)

		if len(groups) != 1 {
			t.Errorf("Expected 1 group for overlapping tables, got %d", len(groups))
		}

		if len(groups) > 0 && len(groups[0]) != 3 {
			t.Errorf("Expected 3 tables in single group, got %d", len(groups[0]))
		}

		t.Logf("Single overlapping group test: %d groups, first group has %d tables",
			len(groups), len(groups[0]))
	})
}

func TestCompactor_SSTableMergingFlag(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_merging_flag_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncNone,
		WriteBufferSize: 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("flag_key_%d", i)
		value := fmt.Sprintf("flag_value_%d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	levels := db.levels.Load()
	if levels == nil || len(*levels) == 0 {
		t.Fatalf("No levels found")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()
	if sstables == nil || len(*sstables) < 2 {
		t.Fatalf("Need at least 2 SSTables for merging test, found %d", len(*sstables))
	}

	table1 := (*sstables)[0]
	table2 := (*sstables)[1]

	if atomic.LoadInt32(&table1.isMerging) != 0 {
		t.Error("SSTable should not be merging initially")
	}

	if !atomic.CompareAndSwapInt32(&table1.isMerging, 0, 1) {
		t.Error("Should be able to set merging flag")
	}

	if atomic.CompareAndSwapInt32(&table1.isMerging, 0, 1) {
		t.Error("Should not be able to set merging flag twice")
	}

	atomic.StoreInt32(&table1.isMerging, 0)

	var successCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if atomic.CompareAndSwapInt32(&table2.isMerging, 0, 1) {
				successCount.Add(1)
				time.Sleep(10 * time.Millisecond)
				atomic.StoreInt32(&table2.isMerging, 0)
			}
		}()
	}

	wg.Wait()

	if successCount.Load() != 1 {
		t.Errorf("Expected only 1 successful flag setting, got %d", successCount.Load())
	}
}

func TestCompactor_CompactionScoreEdgeCases(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_score_edge_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := &Options{
		Directory:                  dir,
		SyncOption:                 SyncNone,
		WriteBufferSize:            1024,
		CompactionScoreSizeWeight:  0.8,
		CompactionScoreCountWeight: 0.2,
		CompactionSizeThreshold:    4,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0]

	atomic.StoreInt64(&level1.currentSize, int64(level1.capacity))

	mockTables := make([]*SSTable, opts.CompactionSizeThreshold)
	for i := 0; i < opts.CompactionSizeThreshold; i++ {
		mockTables[i] = &SSTable{
			Id:   int64(i + 1),
			Size: 100,
		}
	}
	level1.sstables.Store(&mockTables)

	sizeScore := float64(level1.capacity) / float64(level1.capacity)                            // = 1.0
	countScore := float64(opts.CompactionSizeThreshold) / float64(opts.CompactionSizeThreshold) // = 1.0
	totalScore := sizeScore*opts.CompactionScoreSizeWeight + countScore*opts.CompactionScoreCountWeight

	t.Logf("Threshold test - Size score: %.2f, Count score: %.2f, Total: %.2f",
		sizeScore, countScore, totalScore)

	if totalScore <= 1.0 {
		t.Log("Score at threshold correctly does not trigger compaction")
	}

	atomic.StoreInt64(&level1.currentSize, int64(level1.capacity)+1)

	sizeScore = float64(level1.capacity+1) / float64(level1.capacity)
	totalScore = sizeScore*opts.CompactionScoreSizeWeight + countScore*opts.CompactionScoreCountWeight

	t.Logf("Over threshold test - Size score: %.2f, Count score: %.2f, Total: %.2f",
		sizeScore, countScore, totalScore)

	if totalScore <= 1.0 {
		t.Error("Score just over threshold should trigger compaction")
	}

	opts.CompactionScoreSizeWeight = 0.0
	opts.CompactionScoreCountWeight = 1.0

	atomic.StoreInt64(&level1.currentSize, int64(level1.capacity)*10)                                  // Very large size
	sizeScore = float64(level1.capacity*10) / float64(level1.capacity)                                 // = 10.0
	countScore = float64(opts.CompactionSizeThreshold) / float64(opts.CompactionSizeThreshold)         // = 1.0
	totalScore = sizeScore*opts.CompactionScoreSizeWeight + countScore*opts.CompactionScoreCountWeight // = 0*10 + 1*1 = 1.0

	t.Logf("Zero size weight test - Size score: %.2f, Count score: %.2f, Total: %.2f",
		sizeScore, countScore, totalScore)

	if totalScore > 1.0 {
		t.Error("With zero size weight, large size should not trigger compaction")
	}
}

func TestCompactor_WaitForActiveReads(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_active_reads_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := &Options{
		Directory:                          dir,
		SyncOption:                         SyncNone,
		WriteBufferSize:                    1024,
		CompactionActiveSSTReadWaitBackoff: 1 * time.Millisecond,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("read_test_key_%d", i)
		value := fmt.Sprintf("read_test_value_%d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	levels := db.levels.Load()
	if levels == nil || len(*levels) == 0 {
		t.Fatalf("No levels found")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		t.Fatalf("No SSTables found")
	}

	testTable := (*sstables)[0]

	atomic.StoreInt32(&testTable.isBeingRead, 1)

	waitComplete := make(chan bool, 1)
	go func() {
		db.compactor.waitForActiveReads([]*SSTable{testTable})
		waitComplete <- true
	}()

	select {
	case <-waitComplete:
		t.Error("waitForActiveReads should be blocked while read is active")
	case <-time.After(10 * time.Millisecond):
		t.Log("waitForActiveReads correctly waits for active reads")
	}

	atomic.StoreInt32(&testTable.isBeingRead, 0)

	select {
	case <-waitComplete:
		t.Log("waitForActiveReads correctly completed after read finished")
	case <-time.After(100 * time.Millisecond):
		t.Error("waitForActiveReads should complete after read flag is cleared")
	}
}

func TestCompactor_FileCleanup(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_cleanup_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncFull,
		WriteBufferSize: 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("cleanup_key_%d", i)
		value := fmt.Sprintf("cleanup_value_%d", i)

		err = db.Update(func(txn *Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	err = db.ForceFlush()
	if err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	l1Dir := filepath.Join(dir, "l1")
	filesBefore, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("Failed to read l1 directory: %v", err)
	}

	originalFileCount := len(filesBefore)
	t.Logf("Files before compaction: %d", originalFileCount)

	var originalFiles []string
	for _, file := range filesBefore {
		if filepath.Ext(file.Name()) == ".klog" || filepath.Ext(file.Name()) == ".vlog" {
			originalFiles = append(originalFiles, file.Name())
		}
	}

	levels := db.levels.Load()
	if levels != nil && len(*levels) >= 2 {
		level1 := (*levels)[0]
		sstables := level1.sstables.Load()

		if sstables != nil && len(*sstables) >= 2 {
			tablesToCompact := (*sstables)[:2]

			for _, table := range tablesToCompact {
				atomic.StoreInt32(&table.isMerging, 1)
			}

			err = db.compactor.compactSSTables(tablesToCompact, 0, 1)
			if err != nil {
				t.Fatalf("Compaction failed: %v", err)
			}

			time.Sleep(200 * time.Millisecond)

			filesAfter, err := os.ReadDir(l1Dir)
			if err != nil {
				t.Fatalf("Failed to read l1 directory after compaction: %v", err)
			}

			var filesAfterNames []string
			for _, file := range filesAfter {
				if filepath.Ext(file.Name()) == ".klog" || filepath.Ext(file.Name()) == ".vlog" {
					filesAfterNames = append(filesAfterNames, file.Name())
				}
			}

			t.Logf("Files after compaction: %d", len(filesAfterNames))

			var stillExist []string
			for _, originalFile := range originalFiles {
				for _, afterFile := range filesAfterNames {
					if originalFile == afterFile {
						stillExist = append(stillExist, originalFile)
						break
					}
				}
			}

			if len(stillExist) == len(originalFiles) {
				t.Error("Expected some original files to be cleaned up after compaction")
			} else {
				t.Logf("Successfully cleaned up %d files", len(originalFiles)-len(stillExist))
			}

			var value []byte
			err = db.View(func(txn *Txn) error {
				var err error
				value, err = txn.Get([]byte("cleanup_key_50"))
				return err
			})

			if err != nil {
				t.Errorf("Failed to read data after compaction and cleanup: %v", err)
			} else if string(value) != "cleanup_value_50" {
				t.Errorf("Data corruption after cleanup: expected cleanup_value_50, got %s", string(value))
			}
		}
	}
}

func TestCompactor_OverlappingKeyRanges(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_overlapping_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := &Options{
		Directory:       dir,
		SyncOption:      SyncNone,
		WriteBufferSize: 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ranges := []struct {
		prefix string
		start  int
		end    int
	}{
		{"a", 0, 50},  // a000-a050
		{"a", 25, 75}, // a025-a075 (overlaps with first)
		{"b", 0, 30},  // b000-b030 (separate range)
	}

	for i, r := range ranges {
		for j := r.start; j < r.end; j++ {
			key := fmt.Sprintf("%s%03d", r.prefix, j)
			value := fmt.Sprintf("value_%d_%d", i, j)

			err = db.Update(func(txn *Txn) error {
				return txn.Put([]byte(key), []byte(value))
			})
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		err = db.ForceFlush()
		if err != nil {
			t.Fatalf("Failed to force flush: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	levels := db.levels.Load()
	if levels == nil {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0]
	sstables := level1.sstables.Load()
	if sstables == nil || len(*sstables) < 2 {
		t.Fatalf("Need at least 2 SSTables for overlap test, found %d", len(*sstables))
	}

	for i, sst := range *sstables {
		t.Logf("SSTable %d: min=%s, max=%s", i, string(sst.Min), string(sst.Max))
	}

	if len(*sstables) >= 3 {

		selectedTable := (*sstables)[0]

		atomic.StoreInt32(&selectedTable.isMerging, 1)
		defer atomic.StoreInt32(&selectedTable.isMerging, 0)

		// Manually perform leveled compaction
		err = db.compactor.compactSSTables([]*SSTable{selectedTable}, 0, 1)
		if err != nil {
			t.Errorf("Leveled compaction with overlapping ranges failed: %v", err)
		}

		// Verify data integrity after compaction
		testKeys := []string{"a025", "a030", "a040"} // Keys in overlapping region
		for _, key := range testKeys {
			var value []byte
			err = db.View(func(txn *Txn) error {
				var err error
				value, err = txn.Get([]byte(key))
				return err
			})

			if err != nil {
				t.Errorf("Failed to read key %s after compaction: %v", key, err)
			} else {
				t.Logf("Successfully read key %s: %s", key, string(value))
			}
		}
	}
}

func TestCompactor_CooldownPeriod(t *testing.T) {
	dir, err := os.MkdirTemp("", "db_cooldown_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := &Options{
		Directory:                  dir,
		SyncOption:                 SyncNone,
		WriteBufferSize:            1024,
		CompactionCooldownPeriod:   100 * time.Millisecond,
		CompactionSizeThreshold:    2,
		CompactionScoreSizeWeight:  0.8,
		CompactionScoreCountWeight: 0.2,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	levels := db.levels.Load()
	if levels == nil || len(*levels) == 0 {
		t.Fatalf("Levels not initialized")
	}

	level1 := (*levels)[0]

	mockTables := []*SSTable{
		{Id: 1, Size: 1000, isMerging: 0, Min: []byte("a"), Max: []byte("m")},
		{Id: 2, Size: 1000, isMerging: 0, Min: []byte("n"), Max: []byte("z")},
		{Id: 3, Size: 1000, isMerging: 0, Min: []byte("aa"), Max: []byte("bb")},
	}
	level1.sstables.Store(&mockTables)

	// Set size well above capacity to ensure high score
	atomic.StoreInt64(&level1.currentSize, int64(level1.capacity)*3)

	// Calculate expected score to verify it would trigger compaction
	sizeScore := float64(level1.capacity*3) / float64(level1.capacity)                                  // = 3.0
	countScore := float64(len(mockTables)) / float64(opts.CompactionSizeThreshold)                      // = 1.5
	totalScore := sizeScore*opts.CompactionScoreSizeWeight + countScore*opts.CompactionScoreCountWeight // = 3*0.8 + 1.5*0.2 = 2.7

	t.Logf("Setup score: size=%.2f, count=%.2f, total=%.2f (should trigger)", sizeScore, countScore, totalScore)

	if totalScore <= 1.0 {
		t.Fatalf("Test setup failed - score %.2f should be > 1.0 to trigger compaction", totalScore)
	}

	// Set last compaction to now (within cooldown period)
	db.compactor.scoreLock.Lock()
	db.compactor.lastCompaction = time.Now()
	db.compactor.compactionQueue = make([]*compactorJob, 0) // Clear queue
	initialQueueLength := len(db.compactor.compactionQueue)
	db.compactor.scoreLock.Unlock()

	// Try to schedule compaction immediately (should be blocked by cooldown)
	db.compactor.checkAndScheduleCompactions()

	db.compactor.scoreLock.Lock()
	queueLengthDuringCooldown := len(db.compactor.compactionQueue)
	db.compactor.scoreLock.Unlock()

	if queueLengthDuringCooldown > initialQueueLength {
		t.Error("Compaction should be blocked during cooldown period")
	}

	// Wait for cooldown to expire
	time.Sleep(110 * time.Millisecond)

	// Reset last compaction to definitely be outside cooldown
	db.compactor.scoreLock.Lock()
	db.compactor.lastCompaction = time.Now().Add(-2 * opts.CompactionCooldownPeriod)
	db.compactor.scoreLock.Unlock()

	// Try to schedule compaction after cooldown
	db.compactor.checkAndScheduleCompactions()

	db.compactor.scoreLock.Lock()
	queueLengthAfterCooldown := len(db.compactor.compactionQueue)
	db.compactor.scoreLock.Unlock()

	t.Logf("Queue lengths: initial=%d, during_cooldown=%d, after_cooldown=%d",
		initialQueueLength, queueLengthDuringCooldown, queueLengthAfterCooldown)

	if queueLengthAfterCooldown <= queueLengthDuringCooldown {
		t.Error("Compaction should be allowed after cooldown period")
	}

	db.compactor.scoreLock.Lock()
	for _, job := range db.compactor.compactionQueue {
		for _, table := range job.ssTables {
			atomic.StoreInt32(&table.isMerging, 0)
		}
	}
	db.compactor.scoreLock.Unlock()
}

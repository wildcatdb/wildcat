package wildcat

import (
	"bytes"
	"fmt"
	"os"
	"sort"
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
		err = db.Update(func(txn *Txn) error {
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

	// Trigger a compaction by forcing a specific compaction
	level := (*levels)[0]
	sstablesToCompact := *level.sstables.Load()
	if len(sstablesToCompact) >= 2 {
		t.Log("Manually triggering compaction...")

		// Take the first 2 SSTables for compaction
		tablesToCompact := sstablesToCompact[:2]

		err = db.compactor.compactSSTables(tablesToCompact, 1, 2)
		if err != nil {
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
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(dir)

	// Create multiple SSTables with similar sizes in L1
	// Size-tiered compaction looks for similarly sized tables
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

		// Take the smallest tables for compaction
		tablesToCompact := sortedTables[:numToCompact]

		t.Logf("Manually triggering size-tiered compaction with %d tables...", numToCompact)

		// Perform compaction
		err = db.compactor.compactSSTables(tablesToCompact, 1, 2)
		if err != nil {
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
			err = db.Update(func(txn *Txn) error {
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
			level:       job.level,
			priority:    job.priority,
			ssTables:    job.tables,
			targetLevel: job.level + 1,
			inProgress:  false,
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
						level:       levelIdx + 1,
						priority:    float64(levelIdx + 1), // Higher levels have higher priority
						ssTables:    (*sstables)[:2],       // Use first two tables
						targetLevel: levelIdx + 2,
						inProgress:  false,
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

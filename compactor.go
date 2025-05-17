package orindb

import (
	"bytes"
	"fmt"
	"log"
	"orindb/blockmanager"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

// SSTCompactionIterator allows iteration over an SSTable
type SSTCompactionIterator struct {
	sstable    *SSTable
	klogIter   *blockmanager.Iterator
	blockset   *BlockSet
	blockIndex int
	eof        bool
}

// NewCompactor creates a new compactor
func NewCompactor(db *DB, maxConcurrency int) *Compactor {
	if maxConcurrency <= 0 {
		maxConcurrency = MaxCompactionConcurrency
	}

	return &Compactor{
		db:              db,
		compactionQueue: make([]*CompactionJob, 0),
		maxConcurrency:  maxConcurrency,
		lastCompaction:  time.Now(),
	}
}

// backgroundCompactor is the main compaction routine
func (db *DB) backgroundCompactor() {
	defer db.wg.Done()
	cm := NewCompactor(db, MaxCompactionConcurrency)
	ticker := time.NewTicker(time.Millisecond * 24)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			log.Println("Compactor stopped")
			return
		case <-ticker.C:
			// Check and schedule compactions
			cm.checkAndScheduleCompactions()

			// Execute pending compactions if under concurrency limit
			cm.executeCompactions()
		}
	}
}

// checkAndScheduleCompactions evaluates all levels for needed compactions
func (cm *Compactor) checkAndScheduleCompactions() {
	cm.scoreLock.Lock()
	defer cm.scoreLock.Unlock()

	// Only check for new compactions after cooldown period
	if time.Since(cm.lastCompaction) < CompactionCooldownPeriod {
		return
	}

	levels := cm.db.levels.Load()
	if levels == nil {
		return
	}

	// Evaluate each level for compaction
	for i, level := range *levels {
		// Skip last level
		if i == len(*levels)-1 {
			continue
		}

		sstables := level.sstables.Load()
		if sstables == nil || len(*sstables) == 0 {
			continue
		}

		// Calculate compaction score
		sizeScore := float64(atomic.LoadInt64(&level.currentSize)) / float64(level.capacity)
		countScore := float64(len(*sstables)) / float64(CompactionSizeThreshold)

		// Weight the scores
		score := sizeScore*CompactionScoreSizeWeight + countScore*CompactionScoreCountWeight

		// Schedule compaction if score exceeds threshold
		if score > 1.0 {
			// For lower levels (0-2), use size-tiered compaction
			if i < 2 {
				cm.scheduleSizeTieredCompaction(level, i, score)
			} else {
				// For higher levels, use leveled compaction
				cm.scheduleLeveledCompaction(level, i, score)
			}

			cm.lastCompaction = time.Now()
		}
	}

	// Sort compaction queue by priority
	sort.Slice(cm.compactionQueue, func(i, j int) bool {
		return cm.compactionQueue[i].Priority > cm.compactionQueue[j].Priority
	})
}

// scheduleSizeTieredCompaction schedules a size-tiered compaction
func (cm *Compactor) scheduleSizeTieredCompaction(level *Level, levelNum int, score float64) {
	sstables := level.sstables.Load()
	if len(*sstables) < CompactionSizeThreshold {
		return
	}

	// Sort SSTables by size for size-tiered compaction
	sortedTables := make([]*SSTable, len(*sstables))
	copy(sortedTables, *sstables)

	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Size < sortedTables[j].Size
	})

	// Find similar-sized SSTables
	var selectedTables []*SSTable

	// Select up to CompactionBatchSize tables with similar size
	for i := 0; i < len(sortedTables); {
		size := sortedTables[i].Size
		similarSized := []*SSTable{sortedTables[i]}

		j := i + 1
		for j < len(sortedTables) && float64(sortedTables[j].Size)/float64(size) <= 1.5 && len(similarSized) < CompactionBatchSize {
			similarSized = append(similarSized, sortedTables[j])
			j++
		}

		// If we found enough similar-sized tables, select them
		if len(similarSized) >= 2 {
			selectedTables = similarSized
			break
		}

		i = j
	}

	// If we couldn't find similar-sized tables, just take the smallest ones
	if len(selectedTables) < 2 && len(sortedTables) >= 2 {
		selectedTables = sortedTables[:min(CompactionBatchSize, len(sortedTables))]
	}

	if len(selectedTables) >= 2 {
		cm.compactionQueue = append(cm.compactionQueue, &CompactionJob{
			Level:       levelNum,
			Priority:    score,
			SSTables:    selectedTables,
			TargetLevel: levelNum + 1,
			InProgress:  false,
		})
	}
}

// scheduleLeveledCompaction schedules a leveled compaction
func (cm *Compactor) scheduleLeveledCompaction(level *Level, levelNum int, score float64) {
	sstables := level.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		return
	}

	// For leveled compaction, pick the oldest SSTable
	// (often this would be picking by smallest key range, but we'll use oldest for simplicity)
	oldestTable := (*sstables)[0]
	for _, table := range *sstables {
		if table.Id < oldestTable.Id {
			oldestTable = table
		}
	}

	// Find overlapping SSTables in the next level
	nextLevelNum := levelNum + 1
	if nextLevelNum >= len(*cm.db.levels.Load()) {
		return
	}

	nextLevel := (*cm.db.levels.Load())[nextLevelNum]
	nextLevelTables := nextLevel.sstables.Load()
	if nextLevelTables == nil {
		// No tables in next level, just move the table down
		cm.compactionQueue = append(cm.compactionQueue, &CompactionJob{
			Level:       levelNum,
			Priority:    score,
			SSTables:    []*SSTable{oldestTable},
			TargetLevel: nextLevelNum,
			InProgress:  false,
		})
		return
	}

	// Find overlapping tables in next level
	var overlappingTables []*SSTable
	for _, table := range *nextLevelTables {
		if bytes.Compare(table.Max, oldestTable.Min) >= 0 && bytes.Compare(table.Min, oldestTable.Max) <= 0 {
			overlappingTables = append(overlappingTables, table)
		}
	}

	// Create compaction job with selected table and overlapping tables
	selectedTables := []*SSTable{oldestTable}
	selectedTables = append(selectedTables, overlappingTables...)

	cm.compactionQueue = append(cm.compactionQueue, &CompactionJob{
		Level:       levelNum,
		Priority:    score,
		SSTables:    selectedTables,
		TargetLevel: nextLevelNum,
		InProgress:  false,
	})
}

// executeCompactions processes pending compaction jobs
func (cm *Compactor) executeCompactions() {
	// Skip if we've reached max concurrency
	if atomic.LoadInt32(&cm.activeJobs) >= int32(cm.maxConcurrency) {
		return
	}

	// Find the highest priority non-in-progress job
	var selectedJob *CompactionJob
	var selectedIdx int

	for i, job := range cm.compactionQueue {
		if !job.InProgress {
			selectedJob = job
			selectedIdx = i
			break
		}
	}

	if selectedJob == nil {
		return
	}

	// Mark the job as in progress
	selectedJob.InProgress = true
	atomic.AddInt32(&cm.activeJobs, 1)

	// Execute the compaction in a goroutine
	go func(job *CompactionJob, idx int) {
		defer func() {
			atomic.AddInt32(&cm.activeJobs, -1)

			// Remove job from queue when done
			cm.scoreLock.Lock()
			defer cm.scoreLock.Unlock()

			// Only remove if it's still in the queue at the same position
			if idx < len(cm.compactionQueue) && cm.compactionQueue[idx] == job {
				cm.compactionQueue = append(cm.compactionQueue[:idx], cm.compactionQueue[idx+1:]...)
			}
		}()

		// Execute the actual compaction
		err := cm.db.compactSSTables(job.SSTables, job.Level+1, job.TargetLevel)
		if err != nil {
			log.Printf("Compaction failed: %v", err)
			return
		}
	}(selectedJob, selectedIdx)
}

// compactSSTables performs the actual compaction of SSTables
func (db *DB) compactSSTables(sstables []*SSTable, sourceLevel, targetLevel int) error {
	if len(sstables) == 0 {
		return nil
	}

	log.Printf("Starting compaction: %d SSTables from level %d to level %d",
		len(sstables), sourceLevel, targetLevel)

	// Create a new SSTable for the target level
	newSSTable := &SSTable{
		Id:    db.idGenerator.nextID(),
		db:    db,
		Level: targetLevel,
	}

	// Find min and max keys across all input tables
	newSSTable.Min = sstables[0].Min
	newSSTable.Max = sstables[0].Max

	for _, table := range sstables {
		if bytes.Compare(table.Min, newSSTable.Min) < 0 {
			newSSTable.Min = table.Min
		}
		if bytes.Compare(table.Max, newSSTable.Max) > 0 {
			newSSTable.Max = table.Max
		}

		// Mark table as being merged to prevent concurrent access
		atomic.StoreInt32(&table.isMerging, 1)
	}

	// Create new SSTable files for the compacted result
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, 0666,
		blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, 0666,
		blockmanager.SyncOption(db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// Merge the SSTables
	err = db.mergeSSTables(sstables, klogBm, vlogBm, newSSTable)
	if err != nil {
		// Clean up on error
		os.Remove(klogPath)
		os.Remove(vlogPath)
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	// Add the new SSTable to the target level
	targetLevelPtr := (*db.levels.Load())[targetLevel]
	currSSTables := targetLevelPtr.sstables.Load()

	var newSSTables []*SSTable
	if currSSTables != nil {
		newSSTables = make([]*SSTable, len(*currSSTables)+1)
		copy(newSSTables, *currSSTables)
		newSSTables[len(*currSSTables)] = newSSTable
	} else {
		newSSTables = []*SSTable{newSSTable}
	}

	targetLevelPtr.sstables.Store(&newSSTables)

	// Update the level size
	atomic.AddInt64(&targetLevelPtr.currentSize, newSSTable.Size)

	// Remove the original SSTables from the source level
	if sourceLevel != targetLevel {
		sourceLevelPtr := (*db.levels.Load())[sourceLevel]
		currentSSTables := sourceLevelPtr.sstables.Load()

		if currentSSTables != nil {
			// Create a map for fast lookup of SSTables to remove
			toRemove := make(map[int64]bool)
			for _, table := range sstables {
				toRemove[table.Id] = true
			}

			// Filter out the SSTables that were merged
			remainingSSTables := make([]*SSTable, 0, len(*currentSSTables))
			for _, table := range *currentSSTables {
				if !toRemove[table.Id] {
					remainingSSTables = append(remainingSSTables, table)
				} else {
					// Update level size
					atomic.AddInt64(&sourceLevelPtr.currentSize, -table.Size)
				}
			}

			sourceLevelPtr.sstables.Store(&remainingSSTables)
		}
	}

	// Add KLog and VLog managers to LRU cache
	db.lru.Put(klogPath, klogBm)
	db.lru.Put(vlogPath, vlogBm)

	// Clean up the old SSTable files
	for _, table := range sstables {
		oldKlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, sourceLevel,
			string(os.PathSeparator), SSTablePrefix, table.Id, KLogExtension)
		oldVlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", db.opts.Directory, LevelPrefix, sourceLevel,
			string(os.PathSeparator), SSTablePrefix, table.Id, VLogExtension)

		// Wait a bit before deleting to ensure no ongoing reads
		time.Sleep(100 * time.Millisecond)

		// Remove from LRU first
		if bm, ok := db.lru.Get(oldKlogPath); ok {
			if bm, ok := bm.(*blockmanager.BlockManager); ok {
				bm.Close()
			}
			db.lru.Delete(oldKlogPath)
		}

		if bm, ok := db.lru.Get(oldVlogPath); ok {
			if bm, ok := bm.(*blockmanager.BlockManager); ok {
				bm.Close()
			}
			db.lru.Delete(oldVlogPath)
		}

		// Delete the files
		os.Remove(oldKlogPath)
		os.Remove(oldVlogPath)
	}

	log.Printf("Completed compaction: %d SSTables from level %d to level %d, new table size: %d",
		len(sstables), sourceLevel, targetLevel, newSSTable.Size)

	return nil
}

// mergeSSTables merges multiple SSTables into a new SSTable
func (db *DB) mergeSSTables(sstables []*SSTable, klogBm, vlogBm *blockmanager.BlockManager, newSSTable *SSTable) error {
	// Write metadata as first block
	sstableData, err := newSSTable.serializeSSTable()
	if err != nil {
		return fmt.Errorf("failed to serialize SSTable: %w", err)
	}

	_, err = klogBm.Append(sstableData)
	if err != nil {
		return fmt.Errorf("failed to write KLog: %w", err)
	}

	// Create a merged iterator over all input SSTables
	iterators := make([]*SSTCompactionIterator, len(sstables))
	for i, table := range sstables {
		iterators[i] = newSSTCompactionIterator(table)
	}

	mergeIter := newMergeCompactionIterator(iterators)

	// Create a block set for the merged data
	blockset := &BlockSet{
		Entries: make([]*KLogEntry, 0),
		Size:    0,
	}

	entryCount := 0
	var totalSize int64 = 0

	// Iterate through all entries and merge them
	for {
		key, value, ts, valid := mergeIter.Next()
		if !valid {
			break
		}

		// Write the value to VLog
		id, err := vlogBm.Append(value.([]byte))
		if err != nil {
			return fmt.Errorf("failed to write VLog: %w", err)
		}

		klogEntry := &KLogEntry{
			Key:          key,
			Timestamp:    ts,
			ValueBlockID: id,
		}

		blockset.Entries = append(blockset.Entries, klogEntry)
		entrySize := int64(len(key) + len(value.([]byte)))
		blockset.Size += entrySize
		totalSize += entrySize
		entryCount++

		// Flush the block set if it reaches the threshold
		if blockset.Size >= db.opts.BlockSetSize {
			blocksetData, err := blockset.serializeBlockSet()
			if err != nil {
				return fmt.Errorf("failed to serialize BlockSet: %w", err)
			}

			_, err = klogBm.Append(blocksetData)
			if err != nil {
				return fmt.Errorf("failed to write BlockSet to KLog: %w", err)
			}

			blockset.Entries = make([]*KLogEntry, 0)
			blockset.Size = 0
		}
	}

	// Write any remaining entries
	if len(blockset.Entries) > 0 {
		blocksetData, err := blockset.serializeBlockSet()
		if err != nil {
			return fmt.Errorf("failed to serialize BlockSet: %w", err)
		}

		_, err = klogBm.Append(blocksetData)
		if err != nil {
			return fmt.Errorf("failed to write BlockSet to KLog: %w", err)
		}
	}

	// Update the SSTable metadata
	newSSTable.Size = totalSize
	newSSTable.EntryCount = entryCount

	return nil
}

// newSSTCompactionIterator creates a new iterator over an SSTable
func newSSTCompactionIterator(sst *SSTable) *SSTCompactionIterator {
	// Get the KLog block manager
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", sst.db.opts.Directory, LevelPrefix, sst.Level,
		string(os.PathSeparator), SSTablePrefix, sst.Id, KLogExtension)

	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return &SSTCompactionIterator{eof: true}
		}
		sst.db.lru.Put(klogPath, klogBm)
	}

	iter := &SSTCompactionIterator{
		sstable:    sst,
		klogIter:   klogBm.Iterator(),
		blockIndex: -1,
		eof:        false,
	}

	// Skip the first block (metadata)
	_, _, err = iter.klogIter.Next()
	if err != nil {
		iter.eof = true
		return iter
	}

	// Load the first block set
	err = iter.loadNextBlockSet()
	if err != nil {
		iter.eof = true
	}

	return iter
}

// loadNextBlockSet loads the next block set from the KLog
func (iter *SSTCompactionIterator) loadNextBlockSet() error {
	data, _, err := iter.klogIter.Next()
	if err != nil {
		return err
	}

	var blockset BlockSet
	err = blockset.deserializeBlockSet(data)
	if err != nil {
		return err
	}

	iter.blockset = &blockset
	iter.blockIndex = 0

	return nil
}

// Next returns the next key-value pair from the SSTable
func (iter *SSTCompactionIterator) Next() ([]byte, interface{}, int64, bool) {
	if iter.eof || iter.blockset == nil {
		return nil, nil, 0, false
	}

	// If we've reached the end of the current block set, load the next one
	if iter.blockIndex >= len(iter.blockset.Entries) {
		err := iter.loadNextBlockSet()
		if err != nil {
			iter.eof = true
			return nil, nil, 0, false
		}
	}

	// Get the current entry
	entry := iter.blockset.Entries[iter.blockIndex]
	iter.blockIndex++

	// Get the VLog block manager
	vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", iter.sstable.db.opts.Directory, LevelPrefix, iter.sstable.Level,
		string(os.PathSeparator), SSTablePrefix, iter.sstable.Id, VLogExtension)

	var vlogBm *blockmanager.BlockManager
	var err error

	if v, ok := iter.sstable.db.lru.Get(vlogPath); ok {
		vlogBm = v.(*blockmanager.BlockManager)
	} else {
		vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, 0666,
			blockmanager.SyncOption(iter.sstable.db.opts.SyncOption))
		if err != nil {
			iter.eof = true
			return nil, nil, 0, false
		}
		iter.sstable.db.lru.Put(vlogPath, vlogBm)
	}

	// Read the value from VLog
	value, _, err := vlogBm.Read(entry.ValueBlockID)
	if err != nil {
		iter.eof = true
		return nil, nil, 0, false
	}

	return entry.Key, value, entry.Timestamp, true
}

// MergeCompactionIterator merges multiple SSTable iterators
type MergeCompactionIterator struct {
	iters   []*SSTCompactionIterator
	current []*KeyValueEntry
}

// KeyValueEntry represents a key-value entry with timestamp
type KeyValueEntry struct {
	Key       []byte
	Value     interface{}
	Timestamp int64
}

// newMergeCompactionIterator creates a new merge iterator
func newMergeCompactionIterator(iters []*SSTCompactionIterator) *MergeCompactionIterator {
	m := &MergeCompactionIterator{
		iters:   iters,
		current: make([]*KeyValueEntry, len(iters)),
	}

	// Initialize the current entries
	for i, iter := range iters {
		key, value, ts, valid := iter.Next()
		if valid {
			m.current[i] = &KeyValueEntry{
				Key:       key,
				Value:     value,
				Timestamp: ts,
			}
		}
	}

	return m
}

// Next returns the next key-value pair in sorted order
func (m *MergeCompactionIterator) Next() ([]byte, interface{}, int64, bool) {
	// Find the smallest key
	var smallestIdx = -1
	var smallestKey []byte
	var latestTS int64

	for i, entry := range m.current {
		if entry == nil {
			continue
		}

		if smallestIdx == -1 || bytes.Compare(entry.Key, smallestKey) < 0 {
			smallestIdx = i
			smallestKey = entry.Key
			latestTS = entry.Timestamp
		} else if bytes.Equal(entry.Key, smallestKey) && entry.Timestamp > latestTS {
			// If keys are equal, take the one with the latest timestamp
			smallestIdx = i
			latestTS = entry.Timestamp
		}
	}

	if smallestIdx == -1 {
		return nil, nil, 0, false // No more entries
	}

	// Get the current smallest entry
	result := m.current[smallestIdx]

	// Advance the iterator that provided this entry
	key, value, ts, valid := m.iters[smallestIdx].Next()
	if valid {
		m.current[smallestIdx] = &KeyValueEntry{
			Key:       key,
			Value:     value,
			Timestamp: ts,
		}
	} else {
		m.current[smallestIdx] = nil
	}

	// For keys that match but have lower timestamps, skip them
	for i, entry := range m.current {
		if entry != nil && bytes.Equal(entry.Key, result.Key) && entry.Timestamp < result.Timestamp {
			// Advance this iterator
			key, value, ts, valid = m.iters[i].Next()
			if valid {
				m.current[i] = &KeyValueEntry{
					Key:       key,
					Value:     value,
					Timestamp: ts,
				}
			} else {
				m.current[i] = nil
			}
		}
	}

	return result.Key, result.Value, result.Timestamp, true
}

// shouldCompact determines if compaction is needed
func (db *DB) shouldCompact() bool {
	levels := db.levels.Load()
	if levels == nil {
		return false
	}

	// Check if any level has reached its capacity
	for i, level := range *levels {
		if i == len(*levels)-1 {
			continue // Skip the last level
		}

		sstables := level.sstables.Load()
		if sstables == nil {
			continue
		}

		// Size-based criteria - if level is above capacity * ratio
		if atomic.LoadInt64(&level.currentSize) > int64(float64(level.capacity)*CompactionSizeRatio) {
			return true
		}

		// Count-based criteria - if level has too many files
		if len(*sstables) >= CompactionSizeThreshold {
			return true
		}
	}

	return false
}

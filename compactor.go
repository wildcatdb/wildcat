// Package orindb
//
// (C) Copyright OrinDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package orindb

import (
	"bytes"
	"fmt"
	"orindb/blockmanager"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// compactorJob represents a scheduled compaction
type compactorJob struct {
	level       int
	priority    float64
	ssTables    []*SSTable
	targetLevel int
	inProgress  bool
}

// Compactor is responsible for managing compaction jobs
type Compactor struct {
	db              *DB
	compactionQueue []*compactorJob
	activeJobs      int32
	maxConcurrency  int
	lastCompaction  time.Time
	scoreLock       sync.Mutex
}

// sstCompactionIterator iterates over an SSTable for compaction
type sstCompactionIterator struct {
	sstable    *SSTable
	klogIter   *blockmanager.Iterator
	blockset   *BlockSet
	blockIndex int
	eof        bool
}

// mergeCompactionIterator merges multiple SSTable iterators
type mergeCompactionIterator struct {
	iters   []*sstCompactionIterator
	current []*compactionEntry
}

// compactionEntry represents a key-value entry with timestamp
type compactionEntry struct {
	key       []byte
	value     interface{}
	timestamp int64
}

// newCompactor creates a new compactor
func newCompactor(db *DB, maxConcurrency int) *Compactor {
	if maxConcurrency <= 0 {
		maxConcurrency = MaxCompactionConcurrency
	}

	return &Compactor{
		db:              db,
		compactionQueue: make([]*compactorJob, 0),
		maxConcurrency:  maxConcurrency,
		lastCompaction:  time.Now(),
	}
}

// backgroundProcess runs the compaction process in the background
func (compactor *Compactor) backgroundProcess() {
	defer compactor.db.wg.Done()
	ticker := time.NewTicker(CompactorTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-compactor.db.closeCh:
			return
		case <-ticker.C:
			// Check and schedule compactions
			compactor.checkAndScheduleCompactions()

			// Execute pending compactions if under concurrency limit
			compactor.executeCompactions()
		}
	}
}

// checkAndScheduleCompactions evaluates all levels for needed compactions
func (compactor *Compactor) checkAndScheduleCompactions() {
	compactor.scoreLock.Lock()
	defer compactor.scoreLock.Unlock()

	// Only check for new compactions after cooldown period
	if time.Since(compactor.lastCompaction) < CompactionCooldownPeriod {
		return
	}

	levels := compactor.db.levels.Load()
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
				compactor.scheduleSizeTieredCompaction(level, i, score)
			} else {
				// For higher levels, use leveled compaction
				compactor.scheduleLeveledCompaction(level, i, score)
			}

			compactor.lastCompaction = time.Now()
		}
	}

	// Sort compaction queue by priority
	sort.Slice(compactor.compactionQueue, func(i, j int) bool {
		return compactor.compactionQueue[i].priority > compactor.compactionQueue[j].priority
	})
}

// scheduleSizeTieredCompaction schedules a size-tiered compaction
func (compactor *Compactor) scheduleSizeTieredCompaction(level *Level, levelNum int, score float64) {
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
		compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
			level:       levelNum,
			priority:    score,
			ssTables:    selectedTables,
			targetLevel: levelNum + 1,
			inProgress:  false,
		})
	}
}

// scheduleLeveledCompaction schedules a leveled compaction
func (compactor *Compactor) scheduleLeveledCompaction(level *Level, levelNum int, score float64) {
	sstables := level.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		return
	}

	// For leveled compaction, pick the SSTable with the smallest key range
	// This is often more efficient than simply picking the oldest SSTable
	var selectedTable *SSTable
	var smallestRange int

	for i, table := range *sstables {
		// Calculate the key range size
		keyRangeSize := bytes.Compare(table.Max, table.Min)

		// Initialize on first table or update if we find a smaller range
		if i == 0 || keyRangeSize < smallestRange {
			smallestRange = keyRangeSize
			selectedTable = table
		}
	}

	// If we couldn't determine by key range (equal ranges), fall back to oldest
	if selectedTable == nil {
		selectedTable = (*sstables)[0]
		for _, table := range *sstables {
			if table.Id < selectedTable.Id {
				selectedTable = table
			}
		}
	}

	// Find overlapping SSTables in the next level
	nextLevelNum := levelNum + 1
	if nextLevelNum >= len(*compactor.db.levels.Load()) {
		return
	}

	nextLevel := (*compactor.db.levels.Load())[nextLevelNum]
	nextLevelTables := nextLevel.sstables.Load()
	if nextLevelTables == nil {
		// No tables in next level, just move the table down
		compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
			level:       levelNum,
			priority:    score,
			ssTables:    []*SSTable{selectedTable},
			targetLevel: nextLevelNum,
			inProgress:  false,
		})
		return
	}

	// Find overlapping tables in next level
	var overlappingTables []*SSTable
	for _, table := range *nextLevelTables {
		if bytes.Compare(table.Max, selectedTable.Min) >= 0 && bytes.Compare(table.Min, selectedTable.Max) <= 0 {
			overlappingTables = append(overlappingTables, table)
		}
	}

	// Create compaction job with selected table and overlapping tables
	selectedTables := []*SSTable{selectedTable}
	selectedTables = append(selectedTables, overlappingTables...)

	compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
		level:       levelNum,
		priority:    score,
		ssTables:    selectedTables,
		targetLevel: nextLevelNum,
		inProgress:  false,
	})
}

// executeCompactions processes pending compaction jobs
func (compactor *Compactor) executeCompactions() {
	// Skip if we've reached max concurrency
	if atomic.LoadInt32(&compactor.activeJobs) >= int32(compactor.maxConcurrency) {
		return
	}

	// Find the highest priority non-in-progress job
	var selectedJob *compactorJob
	var selectedIdx int

	for i, job := range compactor.compactionQueue {
		if !job.inProgress {
			selectedJob = job
			selectedIdx = i
			break
		}
	}

	if selectedJob == nil {
		return
	}

	// Mark the job as in progress
	selectedJob.inProgress = true
	atomic.AddInt32(&compactor.activeJobs, 1)

	// Execute the compaction in a goroutine
	go func(job *compactorJob, idx int) {
		defer func() {
			atomic.AddInt32(&compactor.activeJobs, -1)

			// Remove job from queue when done
			compactor.scoreLock.Lock()
			defer compactor.scoreLock.Unlock()

			// Only remove if it's still in the queue at the same position
			if idx < len(compactor.compactionQueue) && compactor.compactionQueue[idx] == job {
				compactor.compactionQueue = append(compactor.compactionQueue[:idx], compactor.compactionQueue[idx+1:]...)
			}
		}()

		// Execute the actual compaction
		err := compactor.compactSSTables(job.ssTables, job.level+1, job.targetLevel)
		if err != nil {
			return
		}
	}(selectedJob, selectedIdx)
}

// compactSSTables performs the actual compaction of SSTables
func (compactor *Compactor) compactSSTables(sstables []*SSTable, sourceLevel, targetLevel int) error {
	if len(sstables) == 0 {
		return nil
	}

	// Create a new SSTable for the target level
	newSSTable := &SSTable{
		Id:    compactor.db.sstIdGenerator.nextID(),
		db:    compactor.db,
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
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption))
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// Merge the SSTables
	err = compactor.mergeSSTables(sstables, klogBm, vlogBm, newSSTable)
	if err != nil {
		// Clean up on error
		_ = os.Remove(klogPath)
		_ = os.Remove(vlogPath)
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	// Add the new SSTable to the target level
	targetLevelPtr := (*compactor.db.levels.Load())[targetLevel]
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
		sourceLevelPtr := (*compactor.db.levels.Load())[sourceLevel]
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
	compactor.db.lru.Put(klogPath, klogBm)
	compactor.db.lru.Put(vlogPath, vlogBm)

	// Clean up the old SSTable files
	for _, table := range sstables {
		oldKlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, sourceLevel,
			string(os.PathSeparator), SSTablePrefix, table.Id, KLogExtension)
		oldVlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, sourceLevel,
			string(os.PathSeparator), SSTablePrefix, table.Id, VLogExtension)

		// Wait a bit before deleting to ensure no ongoing reads
		time.Sleep(100 * time.Millisecond)

		// Remove from LRU first
		if bm, ok := compactor.db.lru.Get(oldKlogPath); ok {
			if bm, ok := bm.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
			compactor.db.lru.Delete(oldKlogPath)
		}

		if bm, ok := compactor.db.lru.Get(oldVlogPath); ok {
			if bm, ok := bm.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
			compactor.db.lru.Delete(oldVlogPath)
		}

		// Delete the files
		_ = os.Remove(oldKlogPath)
		_ = os.Remove(oldVlogPath)
	}

	return nil
}

// mergeSSTables merges multiple SSTables into a new SSTable
func (compactor *Compactor) mergeSSTables(sstables []*SSTable, klogBm, vlogBm *blockmanager.BlockManager, newSSTable *SSTable) error {
	// Write metadata as first block
	sstableData, err := newSSTable.serializeSSTable()
	if err != nil {
		return fmt.Errorf("failed to serialize SSTable: %w", err)
	}

	_, err = klogBm.Append(sstableData)
	if err != nil {
		return fmt.Errorf("failed to write KLog: %w", err)
	}

	// Determine if this is the bottom level (for tombstone removal)
	isBottomLevel := newSSTable.Level == len(*compactor.db.levels.Load())-1

	// Get the oldest active read timestamp if available
	// If not tracking this, we use a conservative approach and keep tombstones
	var oldestReadTimestamp int64

	oldestReadTimestamp = atomic.LoadInt64(&compactor.db.oldestActiveRead)

	// Create a merged iterator over all input SSTables
	iterators := make([]*sstCompactionIterator, len(sstables))
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

	// Keep track of the last seen key for potential tombstone optimization
	var lastKey []byte
	var lastKeyTombstone bool
	var savedTombstones int
	var droppedTombstones int

	// Helper function to check if a value is a tombstone
	isTombstone := func(valueBlockID int64, value interface{}) bool {
		// Special marker for deletion
		if valueBlockID == -1 {
			return true
		}

		// Check if the value is nil or empty (another tombstone indicator)
		valueBytes, ok := value.([]byte)
		return !ok || valueBytes == nil || len(valueBytes) == 0
	}

	// Helper function to determine if a tombstone should be kept
	shouldKeepTombstone := func(key []byte, ts int64) bool {
		// Always keep tombstones in non-bottom levels
		if !isBottomLevel {
			return true
		}

		// Drop tombstones older than possible oldest active read in bottom level
		if oldestReadTimestamp > 0 && ts < oldestReadTimestamp {
			return false
		}

		// Check overlap with SSTables in other levels
		for levelNum, level := range *compactor.db.levels.Load() {
			// Skip the current level and target level
			if levelNum == newSSTable.Level || levelNum == sstables[0].Level {
				continue
			}

			levelSSTables := level.sstables.Load()
			if levelSSTables == nil {
				continue
			}

			// Check if the key might be in any SSTable range
			for _, table := range *levelSSTables {
				// Skip tables that are part of this compaction
				isCompacting := false
				for _, compactingTable := range sstables {
					if table.Id == compactingTable.Id {
						isCompacting = true
						break
					}
				}
				if isCompacting {
					continue
				}

				// If key is in the range of this SSTable, we need to keep the tombstone
				if bytes.Compare(key, table.Min) >= 0 && bytes.Compare(key, table.Max) <= 0 {
					return true
				}
			}
		}

		// If we reach here, the tombstone is likely not needed
		return false
	}

	// Iterate through all entries and merge them
	for {
		key, value, ts, valid := mergeIter.next()
		if !valid {
			break
		}

		// Check if this is a tombstone
		valueBlockID := int64(-1)
		if value != nil {
			// For real values, we write to VLog and get a block ID
			var err error
			valueBlockID, err = vlogBm.Append(value.([]byte))
			if err != nil {
				return fmt.Errorf("failed to write VLog: %w", err)
			}
		}

		isTomb := isTombstone(valueBlockID, value)

		if isTomb {
			// Tombstone found - decide if we should keep it
			if !shouldKeepTombstone(key, ts) {
				droppedTombstones++
				continue // Skip this tombstone entirely
			}

			// Check for consecutive tombstones
			if lastKeyTombstone && bytes.Equal(lastKey, key) {
				// Skip duplicate tombstones for the same key
				continue
			}

			savedTombstones++
		}

		// Remember last key and tombstone status for potential optimizations
		lastKey = key
		lastKeyTombstone = isTomb

		// Add the entry to the block set
		klogEntry := &KLogEntry{
			Key:          key,
			Timestamp:    ts,
			ValueBlockID: valueBlockID,
		}

		blockset.Entries = append(blockset.Entries, klogEntry)

		// Calculate entry size - for tombstones, only count the key size
		var entrySize int64
		if isTomb {
			entrySize = int64(len(key))
		} else {
			entrySize = int64(len(key) + len(value.([]byte)))
		}

		blockset.Size += entrySize
		totalSize += entrySize
		entryCount++

		// Flush the block set if it reaches the threshold
		if blockset.Size >= compactor.db.opts.BlockSetSize {
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
func newSSTCompactionIterator(sst *SSTable) *sstCompactionIterator {
	// Get the KLog block manager
	klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", sst.db.opts.Directory, LevelPrefix, sst.Level,
		string(os.PathSeparator), SSTablePrefix, sst.Id, KLogExtension)

	var klogBm *blockmanager.BlockManager
	var err error

	if v, ok := sst.db.lru.Get(klogPath); ok {
		klogBm = v.(*blockmanager.BlockManager)
	} else {
		klogBm, err = blockmanager.Open(klogPath, os.O_RDONLY, sst.db.opts.Permission,
			blockmanager.SyncOption(sst.db.opts.SyncOption))
		if err != nil {
			return &sstCompactionIterator{eof: true}
		}
		sst.db.lru.Put(klogPath, klogBm)
	}

	iter := &sstCompactionIterator{
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
func (iter *sstCompactionIterator) loadNextBlockSet() error {
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

// next returns the next key-value pair from the SSTable
func (iter *sstCompactionIterator) next() ([]byte, interface{}, int64, bool) {
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
		vlogBm, err = blockmanager.Open(vlogPath, os.O_RDONLY, iter.sstable.db.opts.Permission,
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

// newMergeCompactionIterator creates a new merge iterator
func newMergeCompactionIterator(iters []*sstCompactionIterator) *mergeCompactionIterator {
	m := &mergeCompactionIterator{
		iters:   iters,
		current: make([]*compactionEntry, len(iters)),
	}

	// Initialize the current entries
	for i, iter := range iters {
		key, value, ts, valid := iter.next()
		if valid {
			m.current[i] = &compactionEntry{
				key:       key,
				value:     value,
				timestamp: ts,
			}
		}
	}

	return m
}

// next returns the next key-value pair in sorted order
func (m *mergeCompactionIterator) next() ([]byte, interface{}, int64, bool) {
	// Find the smallest key
	var smallestIdx = -1
	var smallestKey []byte
	var latestTS int64

	for i, entry := range m.current {
		if entry == nil {
			continue
		}

		if smallestIdx == -1 || bytes.Compare(entry.key, smallestKey) < 0 {
			smallestIdx = i
			smallestKey = entry.key
			latestTS = entry.timestamp
		} else if bytes.Equal(entry.key, smallestKey) && entry.timestamp > latestTS {
			// If keys are equal, take the one with the latest timestamp
			smallestIdx = i
			latestTS = entry.timestamp
		}
	}

	if smallestIdx == -1 {
		return nil, nil, 0, false // No more entries
	}

	// Get the current smallest entry
	result := m.current[smallestIdx]

	// Advance the iterator that provided this entry
	key, value, ts, valid := m.iters[smallestIdx].next()
	if valid {
		m.current[smallestIdx] = &compactionEntry{
			key:       key,
			value:     value,
			timestamp: ts,
		}
	} else {
		m.current[smallestIdx] = nil
	}

	// For keys that match but have lower timestamps, skip them
	for i, entry := range m.current {
		if entry != nil && bytes.Equal(entry.key, result.key) && entry.timestamp < result.timestamp {
			// Advance this iterator
			key, value, ts, valid = m.iters[i].next()
			if valid {
				m.current[i] = &compactionEntry{
					key:       key,
					value:     value,
					timestamp: ts,
				}
			} else {
				m.current[i] = nil
			}
		}
	}

	return result.key, result.value, result.timestamp, true
}

// shouldCompact determines if compaction is needed
func (compactor *Compactor) shouldCompact() bool {
	levels := compactor.db.levels.Load()
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

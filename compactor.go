package wildcat

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/wildcatdb/wildcat/v2/blockmanager"
	"github.com/wildcatdb/wildcat/v2/tree"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// compactorJob represents a scheduled compaction
type compactorJob struct {
	level       int        // The level being compacted
	priority    float64    // The priority of the compaction job
	ssTables    []*SSTable // The SSTables to be compacted
	targetLevel int        // The target level for the compaction
	inProgress  bool       // Flag indicating if the job is in progress
}

// Compactor is responsible for managing compaction jobs
type Compactor struct {
	db              *DB             // Reference to the database
	compactionQueue []*compactorJob // Queue of compaction jobs
	activeJobs      int32           // Number of active compaction jobs
	maxConcurrency  int             // Maximum number of concurrent compactions
	lastCompaction  time.Time       // Timestamp of the last compaction
	scoreLock       sync.Mutex      // Mutex for synchronizing compaction score calculations
}

// iterState holds the state of an iterator for compaction
type iterState struct {
	iter      *tree.Iterator             // The iterator for the SSTable
	sstable   *SSTable                   // The SSTable being iterated
	klogBm    *blockmanager.BlockManager // The block manager for KLog
	vlogBm    *blockmanager.BlockManager // The block manager for VLog
	hasNext   bool                       // Flag indicating if there is a next entry
	nextKey   []byte                     // The next key to process
	nextValue *KLogEntry                 // The next KLog entry to process
}

// newCompactor creates a new compactor
func newCompactor(db *DB) *Compactor {

	return &Compactor{
		db:              db,
		compactionQueue: make([]*compactorJob, 0),
		maxConcurrency:  db.opts.MaxCompactionConcurrency,
		lastCompaction:  time.Now(),
	}
}

// backgroundProcess runs the compaction process in the background
func (compactor *Compactor) backgroundProcess() {
	defer compactor.db.wg.Done()
	ticker := time.NewTicker(compactor.db.opts.CompactorTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-compactor.db.closeCh:
			compactor.db.log("Compactor: shutting down background process")
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
	if time.Since(compactor.lastCompaction) < compactor.db.opts.CompactionCooldownPeriod {
		return
	}

	levels := compactor.db.levels.Load()
	if levels == nil {
		return
	}

	// Check if last level needs partitioning first
	lastLevelIdx := len(*levels) - 1
	if lastLevelIdx >= 2 {
		lastLevel := (*levels)[lastLevelIdx]
		if compactor.shouldPartitionLastLevel(lastLevel) {
			compactor.scheduleLastLevelPartitioning(lastLevel, lastLevelIdx)
			compactor.lastCompaction = time.Now()
			return // Handle partitioning before any other compactions
		}
	}

	// Regular compaction logic for all levels except the last
	for i, level := range *levels {
		// Skip last level for regular compaction
		if i == len(*levels)-1 {
			continue
		}

		sstables := level.sstables.Load()
		if sstables == nil || len(*sstables) == 0 {
			continue
		}

		// Calculate compaction score
		sizeScore := float64(atomic.LoadInt64(&level.currentSize)) / float64(level.capacity)
		countScore := float64(len(*sstables)) / float64(compactor.db.opts.CompactionSizeThreshold)

		// Weight the scores
		score := sizeScore*compactor.db.opts.CompactionScoreSizeWeight + countScore*compactor.db.opts.CompactionScoreCountWeight

		// Schedule compaction if score exceeds threshold
		if score > 1.0 {
			if i < 2 {
				compactor.scheduleSizeTieredCompaction(level, i, score)
			} else {
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
	if len(*sstables) < compactor.db.opts.CompactionSizeThreshold {
		return
	}

	compactor.db.log(fmt.Sprintf("Scheduling size-tiered compaction for level %d with score %.2f", levelNum, score))

	// Filter out SSTables that are already being merged
	var availableTables []*SSTable
	for _, table := range *sstables {
		if atomic.LoadInt32(&table.isMerging) == 0 {
			availableTables = append(availableTables, table)
		}
	}

	if len(availableTables) < 2 {
		compactor.db.log(fmt.Sprintf("Insufficient available SSTables for compaction on level %d (%d available, need at least 2)", levelNum, len(availableTables)))
		return
	}

	// Sort available SSTables by size for size-tiered compaction
	sort.Slice(availableTables, func(i, j int) bool {
		return availableTables[i].Size < availableTables[j].Size
	})

	// Find similar-sized SSTables among the available ones
	var selectedTables []*SSTable

	for i := 0; i < len(availableTables); {
		size := availableTables[i].Size
		similarSized := []*SSTable{availableTables[i]}

		j := i + 1
		for j < len(availableTables) && float64(availableTables[j].Size)/float64(size) <= compactor.db.opts.CompactionSizeTieredSimilarityRatio && len(similarSized) < compactor.db.opts.CompactionBatchSize {
			similarSized = append(similarSized, availableTables[j])
			j++
		}

		if len(similarSized) >= 2 {
			selectedTables = similarSized
			break
		}

		i = j
	}

	// If we couldn't find similar-sized tables, just take the smallest available ones
	if len(selectedTables) < 2 && len(availableTables) >= 2 {
		selectedTables = availableTables[:min(compactor.db.opts.CompactionBatchSize, len(availableTables))]
	}

	if len(selectedTables) >= 2 {
		var reservedTables []*SSTable
		for _, table := range selectedTables {
			if atomic.CompareAndSwapInt32(&table.isMerging, 0, 1) {
				reservedTables = append(reservedTables, table)
			} else {
				// If we can't reserve this table, it's already being merged
				compactor.db.log(fmt.Sprintf("SSTable %d is already being merged, skipping", table.Id))
			}
		}

		// Only proceed if we have at least 2 reserved tables
		if len(reservedTables) >= 2 {
			compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
				level:       levelNum,
				priority:    score,
				ssTables:    reservedTables,
				targetLevel: levelNum + 1,
				inProgress:  false,
			})

			compactor.db.log(fmt.Sprintf("Scheduled size-tiered compaction with %d reserved SSTables (out of %d available)",
				len(reservedTables), len(availableTables)))
		} else {
			// Release any tables we managed to reserve
			for _, table := range reservedTables {
				atomic.StoreInt32(&table.isMerging, 0)
			}
			compactor.db.log(fmt.Sprintf("Could not reserve enough SSTables for compaction on level %d", levelNum))
		}
	}
}

// scheduleLeveledCompaction schedules a leveled compaction
func (compactor *Compactor) scheduleLeveledCompaction(level *Level, levelNum int, score float64) {
	compactor.db.log(fmt.Sprintf("Scheduling leveled compaction for level %d with score %.2f", levelNum, score))
	sstables := level.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		return
	}

	// Filter available tables (not being merged)
	var availableTables []*SSTable
	for _, table := range *sstables {
		if atomic.LoadInt32(&table.isMerging) == 0 {
			availableTables = append(availableTables, table)
		}
	}

	if len(availableTables) == 0 {
		compactor.db.log(fmt.Sprintf("No available SSTables for leveled compaction on level %d", levelNum))
		return
	}

	// Pick the SSTable with the smallest key range among available tables
	var selectedTable *SSTable
	var smallestRange int

	for i, table := range availableTables {
		keyRangeSize := bytes.Compare(table.Max, table.Min)

		if i == 0 || keyRangeSize < smallestRange {
			smallestRange = keyRangeSize
			selectedTable = table
		}
	}

	// If we couldn't determine by key range, fall back to oldest available table
	if selectedTable == nil {
		selectedTable = availableTables[0]
		for _, table := range availableTables {
			if table.Id < selectedTable.Id {
				selectedTable = table
			}
		}
	}

	// Try to reserve the selected table
	if !atomic.CompareAndSwapInt32(&selectedTable.isMerging, 0, 1) {
		compactor.db.log(fmt.Sprintf("Selected SSTable %d is already being merged", selectedTable.Id))
		return
	}

	// Find overlapping SSTables in the next level
	nextLevelNum := levelNum + 1
	if nextLevelNum > len(*compactor.db.levels.Load()) {
		atomic.StoreInt32(&selectedTable.isMerging, 0)
		return
	}

	nextLevel := (*compactor.db.levels.Load())[nextLevelNum-1]
	nextLevelTables := nextLevel.sstables.Load()

	selectedTables := []*SSTable{selectedTable}

	if nextLevelTables != nil {
		var overlappingTables []*SSTable
		for _, table := range *nextLevelTables {

			// Check if this table overlaps with our selected table
			if bytes.Compare(table.Max, selectedTable.Min) >= 0 && bytes.Compare(table.Min, selectedTable.Max) <= 0 {

				// Try to reserve this overlapping table
				if atomic.CompareAndSwapInt32(&table.isMerging, 0, 1) {
					overlappingTables = append(overlappingTables, table)
				} else {
					compactor.db.log(fmt.Sprintf("Overlapping SSTable %d is already being merged", table.Id))
				}
			}
		}

		selectedTables = append(selectedTables, overlappingTables...)
	}

	compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
		level:       levelNum,
		priority:    score,
		ssTables:    selectedTables,
		targetLevel: nextLevelNum,
		inProgress:  false,
	})

	compactor.db.log(fmt.Sprintf("Scheduled leveled compaction for level %d with %d SSTables (%d available)",
		levelNum, len(selectedTables), len(availableTables)))
}

// executeCompactions processes pending compaction jobs
func (compactor *Compactor) executeCompactions() {
	if atomic.LoadInt32(&compactor.activeJobs) >= int32(compactor.maxConcurrency) {
		return
	}

	compactor.scoreLock.Lock()

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
		compactor.scoreLock.Unlock()
		return
	}

	// Mark the job as in progress
	selectedJob.inProgress = true

	compactor.scoreLock.Unlock()

	atomic.AddInt32(&compactor.activeJobs, 1)

	// Execute the compaction in a goroutine
	go func(job *compactorJob, idx int) {
		defer func() {
			atomic.AddInt32(&compactor.activeJobs, -1)
			compactor.scoreLock.Lock()
			defer compactor.scoreLock.Unlock()

			if idx < len(compactor.compactionQueue) && compactor.compactionQueue[idx] == job {
				compactor.compactionQueue = append(compactor.compactionQueue[:idx], compactor.compactionQueue[idx+1:]...)
			}

			for _, table := range job.ssTables {
				atomic.StoreInt32(&table.isMerging, 0)
			}
		}()

		var err error

		// Check if this is a partitioning job (targetLevel = -1)
		if job.targetLevel == -1 {
			compactor.db.log(fmt.Sprintf("Starting partitioning job for level %d with %d SSTables", job.level+1, len(job.ssTables)))
			err = compactor.partitionLastLevel(job.ssTables, job.level)
			if err != nil {
				compactor.db.log(fmt.Sprintf("Partitioning failed for level %d: %v", job.level+1, err))
			}
		} else {
			// Regular compaction
			compactor.db.log(fmt.Sprintf("Starting compaction job for level %d with %d SSTables", job.level, len(job.ssTables)))
			err = compactor.compactSSTables(job.ssTables, job.level, job.targetLevel)
			if err != nil {
				compactor.db.log(fmt.Sprintf("Compaction failed for level %d: %v", job.level, err))
			}
		}
	}(selectedJob, selectedIdx)
}

// compactSSTables performs the actual compaction of SSTables
func (compactor *Compactor) compactSSTables(sstables []*SSTable, sourceLevel, targetLevel int) error {
	if len(sstables) == 0 {
		return nil
	}

	compactor.db.log(fmt.Sprintf("Compacting %d SSTables from level %d to level %d", len(sstables), sourceLevel, targetLevel))

	// Verify all tables are properly reserved before proceeding
	for _, table := range sstables {
		if atomic.LoadInt32(&table.isMerging) != 1 {
			return fmt.Errorf("SSTable %d is not properly reserved for merging", table.Id)
		}
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
	}

	// Create new SSTable files for the compacted result
	klogTmpPath := fmt.Sprintf("%s%s%d%s%s%d%s%s", compactor.db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, KLogExtension, TempFileExtension)
	klogFinalPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, KLogExtension)
	vlogTmpPath := fmt.Sprintf("%s%s%d%s%s%d%s%s", compactor.db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, VLogExtension, TempFileExtension)
	vlogFinalPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, targetLevel,
		string(os.PathSeparator), SSTablePrefix, newSSTable.Id, VLogExtension)

	klogBm, err := blockmanager.Open(klogTmpPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open KLog block manager: %w", err)
	}
	defer func(klogBm *blockmanager.BlockManager) {
		err := klogBm.Close()
		if err != nil {
			compactor.db.log(fmt.Sprintf("Error closing KLog block manager: %v", err))
		}
	}(klogBm)

	vlogBm, err := blockmanager.Open(vlogTmpPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}
	defer func(vlogBm *blockmanager.BlockManager) {
		err := vlogBm.Close()
		if err != nil {
			compactor.db.log(fmt.Sprintf("Error closing VLog block manager: %v", err))
		}
	}(vlogBm)

	// Merge the SSTables
	err = compactor.mergeSSTables(sstables, klogBm, vlogBm, newSSTable)
	if err != nil {
		_ = os.Remove(klogTmpPath)
		_ = os.Remove(vlogTmpPath)
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	compactor.db.log(fmt.Sprintf("Merged %d SSTables into new SSTable %d", len(sstables), newSSTable.Id))

	// Wait for any active reads to merged sstables to complete before updating the level
	compactor.waitForActiveReads(sstables)

	// Add the new SSTable to the target level
	targetLevelPtr := (*compactor.db.levels.Load())[targetLevel-1]
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

	// Ensure the source level is valid
	if (*compactor.db.levels.Load()) == nil || sourceLevel < 1 || sourceLevel > len(*compactor.db.levels.Load()) {
		return fmt.Errorf("invalid source level %d for compaction", sourceLevel)
	}

	// Remove the original SSTables from the source level
	if sourceLevel != targetLevel {

		sourceLevelPtr := (*compactor.db.levels.Load())[sourceLevel-1]
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

	// Clean up the old SSTable files
	for _, table := range sstables {
		oldKlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, sourceLevel,
			string(os.PathSeparator), SSTablePrefix, table.Id, KLogExtension)
		oldVlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, sourceLevel,
			string(os.PathSeparator), SSTablePrefix, table.Id, VLogExtension)

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

	// Close temporary block managers before renaming
	if err := klogBm.Close(); err != nil {
		return fmt.Errorf("failed to close KLog block manager: %w", err)
	}

	if err := vlogBm.Close(); err != nil {
		return fmt.Errorf("failed to close VLog block manager: %w", err)
	}

	// Rename the temporary files to final names
	if err := os.Rename(klogTmpPath, klogFinalPath); err != nil {
		return fmt.Errorf("failed to rename KLog file: %w", err)
	}

	if err := os.Rename(vlogTmpPath, vlogFinalPath); err != nil {
		return fmt.Errorf("failed to rename VLog file: %w", err)
	}

	compactor.db.log(fmt.Sprintf("Renamed temporary files to final names: KLog=%s, VLog=%s", klogFinalPath, vlogFinalPath))

	// Reopen the final KLog and VLog block managers
	klogBm, err = blockmanager.Open(klogFinalPath, os.O_RDONLY, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open final KLog block manager: %w", err)
	}

	vlogBm, err = blockmanager.Open(vlogFinalPath, os.O_RDONLY, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open final VLog block manager: %w", err)
	}

	compactor.db.lru.Put(klogFinalPath, klogBm, func(key, value interface{}) {
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	compactor.db.lru.Put(vlogFinalPath, vlogBm, func(key, value interface{}) {
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	compactor.db.log(fmt.Sprintf("Compaction complete: %d SSTables merged into new SSTable %d at level %d",
		len(sstables), newSSTable.Id, targetLevel))

	return nil
}

// mergeSSTables merges multiple SSTables into a new SSTable
func (compactor *Compactor) mergeSSTables(sstables []*SSTable, klogBm, vlogBm *blockmanager.BlockManager, output *SSTable) error {
	readTs := time.Now().UnixNano() + 10000000000

	var iters []*iterState

	// Initialize iterators
	for _, sst := range sstables {
		klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, sst.Level, string(os.PathSeparator), SSTablePrefix, sst.Id, KLogExtension)
		vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, sst.Level, string(os.PathSeparator), SSTablePrefix, sst.Id, VLogExtension)

		kbm, err := getOrOpenBM(compactor.db, klogPath)
		if err != nil {
			return fmt.Errorf("failed to open KLog block manager: %w", err)
		}

		vbm, err := getOrOpenBM(compactor.db, vlogPath)
		if err != nil {
			return fmt.Errorf("failed to open VLog block manager: %w", err)
		}

		bt, err := tree.Open(kbm, compactor.db.opts.SSTableBTreeOrder, sst)
		if err != nil {
			return fmt.Errorf("failed to open BTree: %w", err)
		}

		iter, err := bt.Iterator(true)
		if err != nil {
			return fmt.Errorf("failed to open iterator: %w", err)
		}

		state := &iterState{
			iter:    iter,
			sstable: sst,
			klogBm:  kbm,
			vlogBm:  vbm,
		}

		state.hasNext = iter.Valid()
		if state.hasNext {
			key := iter.Key()

			val := iter.Value()
			var klogEntry *KLogEntry

			if entry, ok := val.(*KLogEntry); ok {
				klogEntry = entry
			} else if doc, ok := val.(primitive.D); ok {
				klogEntry = &KLogEntry{}
				for _, elem := range doc {
					switch elem.Key {
					case "key":
						if keyData, ok := elem.Value.(primitive.Binary); ok {
							klogEntry.Key = keyData.Data
						}
					case "timestamp":
						if ts, ok := elem.Value.(int64); ok {
							klogEntry.Timestamp = ts
						}
					case "valueblockid":
						if blockID, ok := elem.Value.(int64); ok {
							klogEntry.ValueBlockID = blockID
						}
					}
				}
			} else {

				bsonData, err := bson.Marshal(val)
				if err == nil {
					klogEntry = &KLogEntry{}
					_ = bson.Unmarshal(bsonData, klogEntry)
				}
			}

			if klogEntry != nil {
				state.nextKey = key
				state.nextValue = klogEntry
			} else {
				state.hasNext = false
			}
		}

		iters = append(iters, state)
	}

	bt, err := tree.Open(klogBm, compactor.db.opts.SSTableBTreeOrder, output)
	if err != nil {
		return fmt.Errorf("failed to create output BTree: %w", err)
	}

	var entryCount int64
	var totalSize int64

	for {
		var minKey []byte
		var candidates []*iterState

		// Find smallest key among all iterators
		for _, it := range iters {
			if it.hasNext {
				if minKey == nil || bytes.Compare(it.nextKey, minKey) < 0 {
					minKey = it.nextKey
				}
			}
		}

		if minKey == nil {
			break // Done
		}

		// Collect all versions of this key
		for _, it := range iters {
			if it.hasNext && bytes.Equal(it.nextKey, minKey) {
				candidates = append(candidates, it)
			}
		}

		// Choose latest visible version
		var latest *KLogEntry
		var latestState *iterState
		for _, it := range candidates {
			kv := it.nextValue
			if kv.Timestamp <= readTs {
				if latest == nil || kv.Timestamp > latest.Timestamp {
					latest = kv
					latestState = it
				}
			}
		}

		if latestState == nil {
			return fmt.Errorf("no valid latest state found for key %s", minKey)
		}

		// Insert if not a tombstone
		if latest != nil && latest.ValueBlockID != -1 {

			// Read the value from the original VLog
			val, _, err := latestState.vlogBm.Read(latest.ValueBlockID)
			if err != nil {
				return fmt.Errorf("failed to read value: %w", err)
			}

			if val != nil {
				// Write to new VLog
				vID, err := vlogBm.Append(val)
				if err != nil {
					return fmt.Errorf("failed to write to VLog: %w", err)
				}

				// Create new KLog entry
				klogEntry := &KLogEntry{
					Key:          latest.Key,
					Timestamp:    latest.Timestamp,
					ValueBlockID: vID,
				}

				// Insert into new KLog
				if err := bt.Put(latest.Key, klogEntry); err != nil {
					return fmt.Errorf("failed to insert into KLog: %w", err)
				}

				entryCount++
				totalSize += int64(len(latest.Key) + len(val))
			}
		}

		// Advance all iterators with that key
		for _, it := range candidates {
			// Move to next item
			it.iter.Next()
			it.hasNext = it.iter.Valid()

			if it.hasNext {
				key := it.iter.Key()
				val := it.iter.Value()

				var klogEntry *KLogEntry
				if entry, ok := val.(*KLogEntry); ok {
					klogEntry = entry
				} else if doc, ok := val.(primitive.D); ok {
					klogEntry = &KLogEntry{}
					for _, elem := range doc {
						switch elem.Key {
						case "key":
							if keyData, ok := elem.Value.(primitive.Binary); ok {
								klogEntry.Key = keyData.Data
							}
						case "timestamp":
							if ts, ok := elem.Value.(int64); ok {
								klogEntry.Timestamp = ts
							}
						case "valueblockid":
							if blockID, ok := elem.Value.(int64); ok {
								klogEntry.ValueBlockID = blockID
							}
						}
					}
				} else {
					bsonData, err := bson.Marshal(val)
					if err == nil {
						klogEntry = &KLogEntry{}
						_ = bson.Unmarshal(bsonData, klogEntry)
					}
				}

				if klogEntry != nil {
					it.nextKey = key
					it.nextValue = klogEntry
				} else {
					it.hasNext = false
				}
			}
		}
	}

	// Update output SSTable metadata
	output.EntryCount = int(entryCount)
	output.Size = totalSize

	return nil
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
		if atomic.LoadInt64(&level.currentSize) > int64(float64(level.capacity)*compactor.db.opts.CompactionSizeRatio) {
			return true
		}

		// Count-based criteria - if level has too many files
		if len(*sstables) >= compactor.db.opts.CompactionSizeThreshold {
			return true
		}
	}

	return false
}

// getOrOpenBM retrieves a BlockManager from the LRU cache or opens it if not found
func getOrOpenBM(db *DB, path string) (*blockmanager.BlockManager, error) {
	var ok bool
	var bmInterface interface{}

	if bmInterface, ok = db.lru.Get(path); !ok {

		bm, err := blockmanager.Open(path, os.O_RDONLY, db.opts.Permission,
			blockmanager.SyncOption(db.opts.SyncOption), db.opts.SyncInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to open block manager: %w", err)
		}
		db.lru.Put(path, bm, func(key, value interface{}) {
			if bm, ok := value.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
		})
	}

	if bm, ok := bmInterface.(*blockmanager.BlockManager); ok {
		return bm, nil
	}

	return nil, errors.New("failed to retrieve BlockManager from LRU cache")
}

// waitForActiveReads waits for any active reads on the given SSTables to complete
func (compactor *Compactor) waitForActiveReads(sstables []*SSTable) {

	for {
		select {
		case <-compactor.db.closeCh:
			return
		default:
			allClear := true

			for _, table := range sstables {
				if atomic.LoadInt32(&table.isBeingRead) == 1 {
					allClear = false
					break
				}
			}

			if allClear {
				return
			}

			time.Sleep(compactor.db.opts.CompactionActiveSSTReadWaitBackoff)
		}
	}

}

// shouldPartitionLastLevel checks if the last level should be partitioned
func (compactor *Compactor) shouldPartitionLastLevel(lastLevel *Level) bool {
	currentSize := atomic.LoadInt64(&lastLevel.currentSize)

	// Check if last level has exceeded its capacity
	if currentSize > int64(lastLevel.capacity) {
		compactor.db.log(fmt.Sprintf("Last level size (%d) exceeds capacity (%d), considering partitioning",
			currentSize, lastLevel.capacity))
		return true
	}

	return false
}

// scheduleLastLevelPartitioning schedules partitioning of the last level if needed
func (compactor *Compactor) scheduleLastLevelPartitioning(lastLevel *Level, levelNum int) {
	sstables := lastLevel.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		return
	}

	compactor.db.log(fmt.Sprintf("Scheduling last level partitioning for level %d with %d SSTables",
		levelNum+1, len(*sstables)))

	// Filter available SSTables (not currently being merged)
	var availableTables []*SSTable
	for _, table := range *sstables {
		if atomic.LoadInt32(&table.isMerging) == 0 {
			availableTables = append(availableTables, table)
		}
	}

	if len(availableTables) == 0 {
		compactor.db.log("No available SSTables for partitioning")
		return
	}

	// Sort by timestamp to prioritize older data for movement
	sort.Slice(availableTables, func(i, j int) bool {
		return availableTables[i].Timestamp < availableTables[j].Timestamp
	})

	// Calculate how much data to partition
	totalSize := atomic.LoadInt64(&lastLevel.currentSize)
	targetSize := int64(float64(totalSize) * compactor.db.opts.CompactionPartitionRatio)

	// Select SSTables to partition (starting with oldest)
	var selectedTables []*SSTable
	var selectedSize int64

	for _, table := range availableTables {
		if selectedSize >= targetSize {
			break
		}
		selectedTables = append(selectedTables, table)
		selectedSize += table.Size
	}

	if len(selectedTables) == 0 {
		compactor.db.log("No suitable SSTables selected for partitioning")
		return
	}

	// Reserve the selected tables
	var reservedTables []*SSTable
	for _, table := range selectedTables {
		if atomic.CompareAndSwapInt32(&table.isMerging, 0, 1) {
			reservedTables = append(reservedTables, table)
		}
	}

	if len(reservedTables) == 0 {
		compactor.db.log("Could not reserve any SSTables for partitioning")
		return
	}

	// Create high-priority partitioning job
	compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
		level:       levelNum,
		priority:    10.0, // Highest priority
		ssTables:    reservedTables,
		targetLevel: -1, // Special marker for partitioning job
		inProgress:  false,
	})

	compactor.db.log(fmt.Sprintf("Scheduled partitioning job with %d SSTables (%.2f MB)",
		len(reservedTables), float64(selectedSize)/(1024*1024)))
}

// groupSSTablesByKeyRange groups SSTables by overlapping key ranges
func (compactor *Compactor) groupSSTablesByKeyRange(sstables []*SSTable) [][]*SSTable {
	if len(sstables) == 0 {
		return nil
	}

	// Sort by min key for grouping
	sortedTables := make([]*SSTable, len(sstables))
	copy(sortedTables, sstables)
	sort.Slice(sortedTables, func(i, j int) bool {
		return bytes.Compare(sortedTables[i].Min, sortedTables[j].Min) < 0
	})

	var groups [][]*SSTable
	currentGroup := []*SSTable{sortedTables[0]}

	for i := 1; i < len(sortedTables); i++ {
		// Check if current SSTable overlaps with the group's key range
		groupMax := currentGroup[len(currentGroup)-1].Max
		if bytes.Compare(sortedTables[i].Min, groupMax) <= 0 {
			// Overlapping, add to current group

			currentGroup = append(currentGroup, sortedTables[i])
		} else {
			// No overlap, start new group
			groups = append(groups, currentGroup)
			currentGroup = []*SSTable{sortedTables[i]}
		}
	}

	// Add the last group
	groups = append(groups, currentGroup)

	return groups
}

// distributeTableGroups distributes SSTables into two levels based on the PartitionDistributionRatio
func (compactor *Compactor) distributeTableGroups(groups [][]*SSTable) ([]*SSTable, []*SSTable) {
	var level1Tables []*SSTable
	var level2Tables []*SSTable

	// Calculate total size
	totalSize := int64(0)
	for _, group := range groups {
		for _, table := range group {
			totalSize += table.Size
		}
	}

	// Distribute based on CompactionPartitionDistributionRatio
	targetLevel1Size := int64(float64(totalSize) * compactor.db.opts.CompactionPartitionDistributionRatio)
	currentLevel1Size := int64(0)

	for _, group := range groups {
		groupSize := int64(0)
		for _, table := range group {
			groupSize += table.Size
		}

		// Decide which level gets this group
		if currentLevel1Size+groupSize <= targetLevel1Size {
			level1Tables = append(level1Tables, group...)
			currentLevel1Size += groupSize
		} else {
			level2Tables = append(level2Tables, group...)
		}
	}

	return level1Tables, level2Tables
}

// removeSSTablesFromLevel removes specified SSTables from a given level
func (compactor *Compactor) removeSSTablesFromLevel(tablesToRemove []*SSTable, levelNum int) error {
	levels := compactor.db.levels.Load()
	level := (*levels)[levelNum]
	currentSSTables := level.sstables.Load()

	if currentSSTables == nil {
		return nil
	}

	// Create map for quick lookup
	toRemove := make(map[int64]bool)
	var totalRemovedSize int64
	for _, table := range tablesToRemove {
		toRemove[table.Id] = true
		totalRemovedSize += table.Size
	}

	// Filter out removed tables
	var remainingTables []*SSTable
	for _, table := range *currentSSTables {
		if !toRemove[table.Id] {
			remainingTables = append(remainingTables, table)
		}
	}

	// Update level
	level.sstables.Store(&remainingTables)
	atomic.AddInt64(&level.currentSize, -totalRemovedSize)

	for _, table := range tablesToRemove {
		klogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, levelNum+1,
			string(os.PathSeparator), SSTablePrefix, table.Id, KLogExtension)
		vlogPath := fmt.Sprintf("%s%s%d%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, levelNum+1,
			string(os.PathSeparator), SSTablePrefix, table.Id, VLogExtension)

		if bm, ok := compactor.db.lru.Get(klogPath); ok {
			if bm, ok := bm.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
			compactor.db.lru.Delete(klogPath)
		}

		if bm, ok := compactor.db.lru.Get(vlogPath); ok {
			if bm, ok := bm.(*blockmanager.BlockManager); ok {
				_ = bm.Close()
			}
			compactor.db.lru.Delete(vlogPath)
		}

		_ = os.Remove(klogPath)
		_ = os.Remove(vlogPath)
	}

	compactor.db.log(fmt.Sprintf("Removed %d SSTables from level %d, freed %.2f MB",
		len(tablesToRemove), levelNum+1, float64(totalRemovedSize)/(1024*1024)))

	return nil
}

// partitionLastLevel partitions SSTables from the last level into L-1 and L-2
func (compactor *Compactor) partitionLastLevel(sstables []*SSTable, lastLevelNum int) error {
	if len(sstables) == 0 {
		return nil
	}

	compactor.db.log(fmt.Sprintf("Starting partitioning of %d SSTables from last level %d", len(sstables), lastLevelNum+1))

	// Group SSTables by overlapping key ranges to maintain data locality
	groups := compactor.groupSSTablesByKeyRange(sstables)
	compactor.db.log(fmt.Sprintf("Grouped %d SSTables into %d key range groups", len(sstables), len(groups)))

	// Distribute groups between L-1 and L-2 based on PartitionDistributionRatio
	toLevel1, toLevel2 := compactor.distributeTableGroups(groups)

	compactor.db.log(fmt.Sprintf("Distribution: %d SSTables to L%d, %d SSTables to L%d",
		len(toLevel1), lastLevelNum, len(toLevel2), lastLevelNum-1))

	// Move SSTables to L-1 (one level up)
	if len(toLevel1) > 0 {
		err := compactor.redistributeToLevel(toLevel1, lastLevelNum-1)
		if err != nil {
			return fmt.Errorf("failed to redistribute SSTables to level %d: %w", lastLevelNum, err)
		}
	}

	// Move SSTables to L-2 (two levels up)
	if len(toLevel2) > 0 && lastLevelNum >= 2 {
		err := compactor.redistributeToLevel(toLevel2, lastLevelNum-2)
		if err != nil {
			return fmt.Errorf("failed to redistribute SSTables to level %d: %w", lastLevelNum-1, err)
		}
	}

	// Remove the original SSTables from the last level
	err := compactor.removeSSTablesFromLevel(sstables, lastLevelNum)
	if err != nil {
		return fmt.Errorf("failed to remove SSTables from last level: %w", err)
	}

	compactor.db.log(fmt.Sprintf("Partitioning completed: moved %d SSTables from last level", len(sstables)))
	return nil
}

// redistributeToLevel redistributes SSTables to a target level by merging them into a new SSTable
func (compactor *Compactor) redistributeToLevel(tables []*SSTable, targetLevelNum int) error {
	if len(tables) == 0 {
		return nil
	}

	// Validate target level exists
	levels := compactor.db.levels.Load()
	if targetLevelNum < 0 || targetLevelNum >= len(*levels) {
		return fmt.Errorf("invalid target level %d", targetLevelNum)
	}

	// Create a new SSTable by merging the selected tables
	newSSTable := &SSTable{
		Id:    compactor.db.sstIdGenerator.nextID(),
		db:    compactor.db,
		Level: targetLevelNum,
	}

	// Find min and max keys across all tables
	newSSTable.Min = make([]byte, len(tables[0].Min))
	copy(newSSTable.Min, tables[0].Min)
	newSSTable.Max = make([]byte, len(tables[0].Max))
	copy(newSSTable.Max, tables[0].Max)

	for _, table := range tables {
		if bytes.Compare(table.Min, newSSTable.Min) < 0 {
			newSSTable.Min = make([]byte, len(table.Min))
			copy(newSSTable.Min, table.Min)
		}
		if bytes.Compare(table.Max, newSSTable.Max) > 0 {
			newSSTable.Max = make([]byte, len(table.Max))
			copy(newSSTable.Max, table.Max)
		}
	}

	targetPath := fmt.Sprintf("%s%s%d%s", compactor.db.opts.Directory, LevelPrefix, targetLevelNum+1, string(os.PathSeparator))
	err := os.MkdirAll(targetPath, compactor.db.opts.Permission)
	if err != nil {
		return fmt.Errorf("failed to create target level directory: %w", err)
	}

	// Create paths for the new SSTable
	klogPath := fmt.Sprintf("%s%s%d%s", targetPath, SSTablePrefix, newSSTable.Id, KLogExtension)
	vlogPath := fmt.Sprintf("%s%s%d%s", targetPath, SSTablePrefix, newSSTable.Id, VLogExtension)

	// Create block managers for new files
	klogBm, err := blockmanager.Open(klogPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to create KLog: %w", err)
	}
	defer func(klogBm *blockmanager.BlockManager) {
		err := klogBm.Close()
		if err != nil {
			compactor.db.log(fmt.Sprintf("Error closing KLog block manager: %v", err))
		}
	}(klogBm)

	vlogBm, err := blockmanager.Open(vlogPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to create VLog: %w", err)
	}
	defer func(vlogBm *blockmanager.BlockManager) {
		err := vlogBm.Close()
		if err != nil {
			compactor.db.log(fmt.Sprintf("Error closing VLog block manager: %v", err))
		}
	}(vlogBm)

	// Merge the SSTables
	err = compactor.mergeSSTables(tables, klogBm, vlogBm, newSSTable)
	if err != nil {
		_ = os.Remove(klogPath)
		_ = os.Remove(vlogPath)
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	// Wait for any active reads to complete
	compactor.waitForActiveReads(tables)

	// Add new SSTable to target level
	targetLevel := (*levels)[targetLevelNum]
	currentSSTables := targetLevel.sstables.Load()

	var newSSTables []*SSTable
	if currentSSTables != nil {
		newSSTables = make([]*SSTable, len(*currentSSTables)+1)
		copy(newSSTables, *currentSSTables)
		newSSTables[len(*currentSSTables)] = newSSTable
	} else {
		newSSTables = []*SSTable{newSSTable}
	}

	targetLevel.sstables.Store(&newSSTables)
	atomic.AddInt64(&targetLevel.currentSize, newSSTable.Size)

	compactor.db.lru.Put(klogPath, klogBm, func(key, value interface{}) {
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	compactor.db.lru.Put(vlogPath, vlogBm, func(key, value interface{}) {
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	compactor.db.log(fmt.Sprintf("Successfully redistributed %d SSTables to level %d as SSTable %d",
		len(tables), targetLevelNum+1, newSSTable.Id))

	return nil
}

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
func newCompactor(db *DB, maxConcurrency int) *Compactor {
	if maxConcurrency <= 0 {
		maxConcurrency = db.opts.MaxCompactionConcurrency
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
		countScore := float64(len(*sstables)) / float64(compactor.db.opts.CompactionSizeThreshold)

		// Weight the scores
		score := sizeScore*compactor.db.opts.CompactionScoreSizeWeight + countScore*compactor.db.opts.CompactionScoreCountWeight

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
	if len(*sstables) < compactor.db.opts.CompactionSizeThreshold {
		return
	}

	compactor.db.log(fmt.Sprintf("Scheduling size-tiered compaction for level %d with score %.2f", levelNum, score))

	// Sort safe SSTables by size for size-tiered compaction
	sort.Slice(*sstables, func(i, j int) bool {
		return (*sstables)[i].Size < (*sstables)[j].Size
	})

	// Find similar-sized SSTables among the safe ones
	var selectedTables []*SSTable

	for i := 0; i < len(*sstables); {
		size := (*sstables)[i].Size
		similarSized := []*SSTable{(*sstables)[i]}

		j := i + 1
		for j < len(*sstables) && float64((*sstables)[j].Size)/float64(size) <= compactor.db.opts.CompactionSizeTieredSimilarityRatio && len(similarSized) < compactor.db.opts.CompactionBatchSize {
			similarSized = append(similarSized, (*sstables)[j])
			j++
		}

		if len(similarSized) >= 2 {
			selectedTables = similarSized
			break
		}

		i = j
	}

	// If we couldn't find similar-sized tables, just take the smallest safe ones
	if len(selectedTables) < 2 && len(*sstables) >= 2 {
		selectedTables = (*sstables)[:min(compactor.db.opts.CompactionBatchSize, len(*sstables))]
	}

	if len(selectedTables) >= 2 {
		compactor.compactionQueue = append(compactor.compactionQueue, &compactorJob{
			level:       levelNum,
			priority:    score,
			ssTables:    selectedTables,
			targetLevel: levelNum + 1,
			inProgress:  false,
		})

		compactor.db.log(fmt.Sprintf("Scheduled size-tiered compaction with %d safe SSTables (out of %d total)",
			len(selectedTables), len(*sstables)))
	}
}

// scheduleLeveledCompaction schedules a leveled compaction
func (compactor *Compactor) scheduleLeveledCompaction(level *Level, levelNum int, score float64) {
	compactor.db.log(fmt.Sprintf("Scheduling leveled compaction for level %d with score %.2f", levelNum, score))
	sstables := level.sstables.Load()
	if sstables == nil || len(*sstables) == 0 {
		return
	}

	// Pick the SSTable with the smallest key range among safe tables
	var selectedTable *SSTable
	var smallestRange int

	for i, table := range *sstables {
		keyRangeSize := bytes.Compare(table.Max, table.Min)

		if i == 0 || keyRangeSize < smallestRange {
			smallestRange = keyRangeSize
			selectedTable = table
		}
	}

	// If we couldn't determine by key range, fall back to oldest safe table
	if selectedTable == nil {
		selectedTable = (*sstables)[0]
		for _, table := range *sstables {
			if table.Id < selectedTable.Id {
				selectedTable = table
			}
		}
	}

	// Find overlapping SSTables in the next level (and filter those too)
	nextLevelNum := levelNum + 1
	if nextLevelNum > len(*compactor.db.levels.Load()) {
		return
	}

	nextLevel := (*compactor.db.levels.Load())[nextLevelNum-1]
	nextLevelTables := nextLevel.sstables.Load()

	selectedTables := []*SSTable{selectedTable}

	if nextLevelTables != nil {
		var overlappingTables []*SSTable
		for _, table := range *nextLevelTables {
			if bytes.Compare(table.Max, selectedTable.Min) >= 0 && bytes.Compare(table.Min, selectedTable.Max) <= 0 {
				overlappingTables = append(overlappingTables, table)
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

	compactor.db.log(fmt.Sprintf("Scheduled leveled compaction for level %d with %d SSTables (%d safe)",
		levelNum, len(selectedTables), len(*sstables)))
}

// executeCompactions processes pending compaction jobs
func (compactor *Compactor) executeCompactions() {
	// Skip if we've reached max concurrency
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
		compactor.db.log(fmt.Sprintf("Starting compaction job for level %d with %d SSTables", job.level, len(job.ssTables)))

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

	compactor.db.log(fmt.Sprintf("Compacting %d SSTables from level %d to level %d", len(sstables), sourceLevel, targetLevel))

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

	vlogBm, err := blockmanager.Open(vlogTmpPath, os.O_RDWR|os.O_CREATE, compactor.db.opts.Permission,
		blockmanager.SyncOption(compactor.db.opts.SyncOption), compactor.db.opts.SyncInterval)
	if err != nil {
		return fmt.Errorf("failed to open VLog block manager: %w", err)
	}

	// Merge the SSTables
	err = compactor.mergeSSTables(sstables, klogBm, vlogBm, newSSTable)
	if err != nil {
		// Clean up on error
		_ = os.Remove(klogTmpPath)
		_ = os.Remove(vlogTmpPath)
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	compactor.db.log(fmt.Sprintf("Merged %d SSTables into new SSTable %d", len(sstables), newSSTable.Id))

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

	// Wait for any active reads to merged sstables to complete before updating the level
	compactor.waitForActiveReads(sstables)

	targetLevelPtr.sstables.Store(&newSSTables)

	// Update the level size
	atomic.AddInt64(&targetLevelPtr.currentSize, newSSTable.Size)

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

	// Close new KLog and VLog block managers
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

	// Add KLog and VLog managers to LRU cache
	compactor.db.lru.Put(klogFinalPath, klogBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
		if bm, ok := value.(*blockmanager.BlockManager); ok {
			_ = bm.Close()
		}
	})

	compactor.db.lru.Put(vlogFinalPath, vlogBm, func(key, value interface{}) {
		// Close the block manager when evicted from LRU
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
				// Handle primitive.D case
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
	} else {
		if bm, ok := bmInterface.(*blockmanager.BlockManager); ok {
			return bm, nil
		} else {
			return nil, errors.New("invalid type in LRU cache")
		}
	}

	return bmInterface.(*blockmanager.BlockManager), nil
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

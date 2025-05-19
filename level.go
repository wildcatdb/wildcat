// Package wildcat
//
// (C) Copyright Alex Gaetano Padula
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
package wildcat

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"wildcat/blockmanager"
)

// Level is a disk level within Wildcat, which contains a list of immutable SSTables
type Level struct {
	id          int                        // The level ID
	path        string                     // The path to the level directory
	sstables    atomic.Pointer[[]*SSTable] // Atomic pointer to the list of SSTables
	capacity    int                        // The capacity of the level
	currentSize int64                      // Atomic size of the level
	db          *DB                        // Reference to the database\
}

// reopen opens an existing level directories sstables
// sstables are loaded and sorted by last modified time
func (l *Level) reopen() error {
	// Read the level directory
	files, err := os.ReadDir(l.path)
	if err != nil {
		return fmt.Errorf("failed to read level directory: %w", err)
	}

	// Find KLog files to identify SSTables
	var sstables []*SSTable

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), KLogExtension) {
			continue
		}

		// Extract SSTable ID from the filename
		if !strings.HasPrefix(file.Name(), SSTablePrefix) {
			continue
		}

		idStr := strings.TrimPrefix(file.Name(), SSTablePrefix)
		idStr = strings.TrimSuffix(idStr, KLogExtension)
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse SSTable ID from filename %s: %w", file.Name(), err)
		}

		levelPath := l.path
		if !strings.HasSuffix(levelPath, string(os.PathSeparator)) {
			levelPath += string(os.PathSeparator)
		}

		// Get corresponding VLog file path with proper path construction
		vlogPath := fmt.Sprintf("%s%s%d%s", levelPath, SSTablePrefix, id, VLogExtension)

		// Check if VLog file exists
		if _, err := os.Stat(vlogPath); os.IsNotExist(err) {
			// Log the issue but continue instead of failing completely
			l.db.log(fmt.Sprintf("Warning: VLog file not found for SSTable %d: %v - skipping", id, err))
			continue // Skip this SSTable instead of failing completely
		}

		// Create SSTable structure
		sstable := &SSTable{}

		// Load the SSTable metadata from the first block of KLog
		klogPath := fmt.Sprintf("%s%s%d%s", levelPath, SSTablePrefix, id, KLogExtension)

		// Open the KLog file
		klogBm, err := blockmanager.Open(klogPath, os.O_RDONLY, l.db.opts.Permission, blockmanager.SyncOption(0))
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to open KLog block manager for SSTable %d: %v - skipping", id, err))
			continue // Skip this SSTable instead of failing completely
		}

		// Add the KLog to cache
		l.db.lru.Put(klogPath, klogBm)

		// Read the first block which contains SSTable metadata
		data, _, err := klogBm.Read(1)
		if err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to read KLog metadata block for SSTable %d: %v - skipping", id, err))
			continue // Skip this SSTable instead of failing completely
		}

		// Deserialize the SSTable metadata
		if err := sstable.deserializeSSTable(data); err != nil {
			l.db.log(fmt.Sprintf("Warning: Failed to deserialize SSTable metadata for SSTable %d: %v - skipping", id, err))
			continue // Skip this SSTable instead of failing completely
		}

		sstable.db = l.db

		sstables = append(sstables, sstable)
	}

	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].Id < sstables[j].Id
	})

	// Update the level's total size
	var totalSize int64
	for _, sstable := range sstables {
		totalSize += sstable.Size
	}
	l.setSize(totalSize)

	// Store the sorted SSTables
	l.sstables.Store(&sstables)

	return nil
}

// getSize returns the current size of the level
func (l *Level) getSize() int64 {
	return atomic.LoadInt64(&l.currentSize)
}

// setSize sets the current size of the level
func (l *Level) setSize(size int64) {
	atomic.StoreInt64(&l.currentSize, size)
}

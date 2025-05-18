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
	"fmt"
	"orindb/blockmanager"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// Level is a disk level within OrinDB, which contains a list of immutable SSTables
type Level struct {
	id          int                        // The level ID
	path        string                     // The path to the level directory
	sstables    atomic.Pointer[[]*SSTable] // Atomic pointer to the list of SSTables
	capacity    int                        // The capacity of the level
	currentSize int64                      // atomic size of the level
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

		// Get file info to extract last modified time
		fileInfo, err := file.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", file.Name(), err)
		}

		// Get corresponding VLog file path
		vlogPath := fmt.Sprintf("%s%s%d%s", l.path, SSTablePrefix, id, VLogExtension)

		// Check if VLog file exists
		if _, err := os.Stat(vlogPath); os.IsNotExist(err) {
			return fmt.Errorf("VLog file not found for SSTable %d: %w", id, err)
		}

		// Create SSTable structure
		sstable := &SSTable{
			Id:      id,
			Level:   l.id,
			modTime: fileInfo.ModTime(),
		}

		// Load the SSTable metadata from the first block of KLog
		klogPath := fmt.Sprintf("%s%s%d%s", l.path, SSTablePrefix, id, KLogExtension)

		// Open the KLog file
		klogBm, err := blockmanager.Open(klogPath, os.O_RDONLY, 0666, blockmanager.SyncOption(0))
		if err != nil {
			return fmt.Errorf("failed to open KLog block manager: %w", err)
		}

		// Read the first block which contains SSTable metadata
		data, _, err := klogBm.Read(0)
		if err != nil {
			return fmt.Errorf("failed to read KLog metadata block: %w", err)
		}

		// Deserialize the SSTable metadata
		if err := sstable.deserializeSSTable(data); err != nil {
			return fmt.Errorf("failed to deserialize SSTable metadata: %w", err)
		}

		sstables = append(sstables, sstable)
	}

	// Sort SSTables by last modified time (oldest first)
	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].modTime.Before(sstables[j].modTime)
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

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

import "sync/atomic"

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
	// @todo
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

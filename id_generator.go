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
	"sync"
	"time"
)

// The IDGenerator is a thread-safe utility for generating unique, monotonic IDs.

// IDGenerator is a thread-safe ID generator
type IDGenerator struct {
	mu     *sync.Mutex
	lastID int64
}

// NewIDGenerator creates a new ID generator
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{
		mu:     &sync.Mutex{},
		lastID: time.Now().UnixNano(),
	}
}

// NextID generates the next unique ID
func (g *IDGenerator) nextID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Get current timestamp
	ts := time.Now().UnixNano()

	// Ensure monotonicity by using max of current time and last ID + 1
	if ts <= g.lastID {
		ts = g.lastID + 1
	}

	// Update last ID
	g.lastID = ts

	return ts
}

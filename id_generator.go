package wildcat

import (
	"math"
	"sync/atomic"
	"time"
)

// The IDGenerator is a thread-safe utility for generating unique, monotonic IDs.

// IDGenerator is a thread-safe ID generator
type IDGenerator struct {
	lastID int64
}

// newIDGenerator creates a new ID generator
func newIDGenerator() *IDGenerator {
	return &IDGenerator{
		lastID: 0,
	}
}

// newIDGeneratorWithTimestamp creates a new ID generator starting from current nanosecond
func newIDGeneratorWithTimestamp() *IDGenerator {
	return &IDGenerator{
		lastID: time.Now().UnixNano(),
	}
}

// reloadIDGenerator creates a new ID generator with a specified last ID
func reloadIDGenerator(lastId int64) *IDGenerator {
	return &IDGenerator{
		lastID: lastId,
	}
}

// nextID generates the next unique ID, resetting to 1 if int64 max is reached
func (g *IDGenerator) nextID() int64 {
	for {
		last := atomic.LoadInt64(&g.lastID)
		var next int64

		// Check if we're at max int64
		if last == math.MaxInt64 {
			next = 1 // Reset to 1
		} else {
			next = last + 1
		}

		if atomic.CompareAndSwapInt64(&g.lastID, last, next) {
			return next
		}
	}
}

// Save returns the last ID to be persisted
func (g *IDGenerator) save() int64 {
	return atomic.LoadInt64(&g.lastID)
}

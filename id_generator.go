package wildcat

import (
	"math"
	"sync/atomic"
	"time"
)

// The IDGenerator is a thread-safe utility for generating unique, monotonic IDs.

type IDGType int64

const (
	// IDGTypeInt64 represents an int64 ID generator
	IDGTypeInt64 IDGType = iota
	// IDGTypeTimestamp represents a timestamp-based ID generator
	IDGTypeTimestamp
)

// IDGenerator is a thread-safe ID generator
type IDGenerator struct {
	lastID  int64
	idgType IDGType // Type of ID generator, either int64 or timestamp
}

// newIDGenerator creates a new ID generator
func newIDGenerator() *IDGenerator {
	return &IDGenerator{
		lastID:  0,
		idgType: IDGTypeInt64, // Default to int64 ID generator
	}
}

// newIDGeneratorWithTimestamp creates a new ID generator starting from current nanosecond
func newIDGeneratorWithTimestamp() *IDGenerator {
	return &IDGenerator{
		lastID:  time.Now().UnixNano(),
		idgType: IDGTypeTimestamp,
	}
}

// reloadIDGenerator creates a new ID generator with a specified last ID
func reloadIDGenerator(lastId int64) *IDGenerator {
	return &IDGenerator{
		lastID: lastId,
	}
}

// last returns the last generated ID
func (g *IDGenerator) last() int64 {
	return atomic.LoadInt64(&g.lastID)
}

// nextID generates the next unique ID, resetting to 1 if int64 max is reached
func (g *IDGenerator) nextID() int64 {
	for {
		last := atomic.LoadInt64(&g.lastID)
		var next int64

		// Check if we're at max int64
		if last == math.MaxInt64 {
			if g.idgType == IDGTypeInt64 {
				// Reset to 1 if using int64 ID generator
				next = 1
			} else {
				// For timestamp-based, just increment by 1 nanosecond
				next = time.Now().UnixNano()
			}
		} else {
			if g.idgType == IDGTypeTimestamp {
				now := time.Now().UnixNano()
				// Always use max(now, last+1) to ensure monotonic increment
				if now > last {
					next = now
				} else {
					next = last + 1
				}
			} else {
				next = last + 1
			}
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

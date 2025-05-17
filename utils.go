package orindb

import (
	"strconv"
	"strings"
)

// extractTimestampFromFilename is a helper function to extract timestamp from a WAL filename (<timestamp>.wal).
func extractTimestampFromFilename(filename string) int64 {
	// Filename format is <timestamp>.wal
	parts := strings.Split(filename, ".")
	if len(parts) != 2 {
		return 0
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}

	return ts
}

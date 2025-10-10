package wildcat

import (
	"strconv"
	"strings"
)

// extractIDFromFilename extracts the ID from a given file name in format <id>.<ext>
func extractIDFromFilename(filename string) int64 {
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

// startsWith Helper function to check if a byte slice starts with a prefix
func startsWith(data, prefix []byte) bool {
	if len(data) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if data[i] != prefix[i] {
			return false
		}
	}
	return true
}

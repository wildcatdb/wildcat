package wildcat

import (
	"strconv"
	"strings"
)

// extractIDFromFilename extracts the ID from a given filename. <id>.<ext>
func extractIDFromFilename(filename string) int64 {
	parts := strings.Split(filename, ".")

	// WAL file names in wildcat are <id>.<ext>
	if len(parts) != 2 {
		return 0
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}

	return ts
}

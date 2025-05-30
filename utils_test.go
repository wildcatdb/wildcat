package wildcat

import "testing"

func TestExtractIDFromFilename(t *testing.T) {
	tests := []struct {
		filename string
		expected int64
	}{
		{"1234567890.wal", 1234567890},
		{"9876543210.wal", 9876543210},
		{"invalid.wal", 0},
		{"12345.invalid", 12345},
		{"", 0},
		{"12345", 0},
		{".wal", 0},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			result := extractIDFromFilename(tt.filename)
			if result != tt.expected {
				t.Errorf("extractIDFromFilename(%q) = %d; want %d", tt.filename, result, tt.expected)
			}
		})
	}
}

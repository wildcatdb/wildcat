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

import "testing"

func TestExtractTimestampFromFilename(t *testing.T) {
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
			result := extractTimestampFromFilename(tt.filename)
			if result != tt.expected {
				t.Errorf("extractTimestampFromFilename(%q) = %d; want %d", tt.filename, result, tt.expected)
			}
		})
	}
}

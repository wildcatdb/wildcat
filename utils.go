// Package wildcat
//
// (C) Copyright Alex Gaetano Padula
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
package wildcat

import (
	"strconv"
	"strings"
)

// extractIDFromFilename extracts the ID from a given filename. <id>.ext
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

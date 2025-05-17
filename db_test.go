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
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
)

// Tests opening a brand new instance.  Will setup an initial WAL and memory table and disk levels.
func TestOpen(t *testing.T) {
	defer os.RemoveAll("testdb")

	// Create a log channel
	logChannel := make(chan string, 100) // Buffer size of 100 messages

	opts := &Options{
		Directory:  "testdb",
		LogChannel: logChannel,
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)

	// Start a goroutine to listen to the log channel
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			t.Logf("Log message: %s", msg)
		}
	}()

	// Open or create the database
	db, err := Open(opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Verify all l1 to l7 directories exist
	for i := 1; i <= 7; i++ {
		dir := fmt.Sprintf("%s/l%d", opts.Directory, i)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Directory %s does not exist", dir)
		}
	}

	db.Close()

	wg.Wait()
}

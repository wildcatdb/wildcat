// Package blockmanager
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
package blockmanager

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"
)

//func TestWriteAndReadHeader(t *testing.T) {
//	// Create a temporary file for testing
//	tempFile, err := os.CreateTemp("", "blockmanager_test")
//	if err != nil {
//		t.Fatalf("Failed to create temp file: %v", err)
//	}
//	defer os.Remove(tempFile.Name()) // Clean up the file after the test
//	defer tempFile.Close()
//
//	// Write the header to the file
//	if err := writeHeader(tempFile); err != nil {
//		t.Fatalf("Failed to write header: %v", err)
//	}
//
//	// Reset the file pointer to the beginning
//	if _, err := tempFile.Seek(0, 0); err != nil {
//		t.Fatalf("Failed to seek to the beginning of the file: %v", err)
//	}
//
//	// Read the header from the file
//	if err := readHeader(tempFile); err != nil {
//		t.Fatalf("Failed to read header: %v", err)
//	}
//}

func TestOpenNewFile(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_open_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file (should create it)
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open new file: %v", err)
	}
	defer bm.Close()

	// Check if the allocation table has the expected initial blocks
	if bm.allocationTable.IsEmpty() {
		t.Fatalf("Allocation table should not be empty for a new file")
	}

	// Check that we have Allotment number of blocks
	// Convert the stack to a slice to count elements
	count := 0
	for !bm.allocationTable.IsEmpty() {
		bm.allocationTable.Pop()
		count++
	}

	if count+1 != int(Allotment) {
		t.Fatalf("Expected %d blocks in allocation table, got %d", Allotment, count)
	}
}

func TestAppendSmallData(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_append_small_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Small data (less than one block)
	data := []byte("This is a test data for small append")

	// Append the data
	blockID, err := bm.Append(data)
	if err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}

	// Read back the data
	readData, blockId, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	t.Logf("Read block ID: %d", blockId)

	// Verify the data
	if !bytes.Equal(data, readData) {
		t.Fatalf("Data mismatch. Expected: %s, Got: %s", string(data), string(readData))
	}
}

func TestAppendLargeData(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_append_large_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 066, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Calculate size for data larger than one block
	blockHeaderSize := 16 // Size of BlockHeader struct (4 uint32 fields)
	dataPerBlock := int(BlockSize) - blockHeaderSize

	// Create large data (spans multiple blocks)
	dataSize := dataPerBlock*3 + 100 // Should span 4 blocks
	data := make([]byte, dataSize)

	// Fill with random data
	rand.Read(data)

	// Append the data
	blockID, err := bm.Append(data)
	if err != nil {
		t.Fatalf("Failed to append large data: %v", err)
	}

	// Read back the data
	readData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read large data: %v", err)
	}

	// Verify the data
	if !bytes.Equal(data, readData) {
		t.Fatalf("Large data mismatch. Data sizes - Expected: %d, Got: %d", len(data), len(readData))
	}
}

func TestMultipleAppendsAndReads(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_multiple_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Test with multiple appends of different sizes
	testData := [][]byte{
		[]byte("Small data 1"),
		[]byte("Medium sized data that still fits in one block"),
		make([]byte, 1000), // Larger data that might span blocks
	}

	// Fill the larger data with random bytes
	rand.Read(testData[2])

	// Store block IDs for later reading
	blockIDs := make([]int64, len(testData))

	// Append all test data
	for i, data := range testData {
		blockID, err := bm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append data %d: %v", i, err)
		}
		blockIDs[i] = blockID
	}

	// Read all data back and verify
	for i, expectedData := range testData {
		readData, _, err := bm.Read(blockIDs[i])
		if err != nil {
			t.Fatalf("Failed to read data %d: %v", i, err)
		}

		if !bytes.Equal(expectedData, readData) {
			t.Fatalf("Data mismatch for data %d. Expected length: %d, Got length: %d",
				i, len(expectedData), len(readData))
		}
	}
}

func TestReopenFile(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_reopen_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Data to write
	data := []byte("Test data for reopening file")
	var blockID int64

	// First session: create file and write data
	{
		bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*24)
		if err != nil {
			t.Fatalf("Failed to open file first time: %v", err)
		}

		blockID, err = bm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}

		if err := bm.Close(); err != nil {
			t.Fatalf("Failed to close file: %v", err)
		}
	}

	// Second session: reopen file and read data
	{
		bm, err := Open(tempFilePath, os.O_RDWR, 0666, SyncPartial, time.Millisecond*24)
		if err != nil {
			t.Fatalf("Failed to reopen file: %v", err)
		}
		defer bm.Close()

		readData, _, err := bm.Read(blockID)
		if err != nil {
			t.Fatalf("Failed to read data after reopening: %v", err)
		}

		if !bytes.Equal(data, readData) {
			t.Fatalf("Data mismatch after reopening. Expected: %s, Got: %s",
				string(data), string(readData))
		}
	}
}

func TestAllotmentExhaustion(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_allotment_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Small data to append
	data := []byte("Small test data")

	// Allocate all initial blocks
	initialBlocks := int(Allotment)
	blockIDs := make([]int64, initialBlocks+5) // We'll allocate more than the initial allotment

	// Use up all initial blocks and some more
	for i := 0; i < initialBlocks+5; i++ {
		blockID, err := bm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append data in iteration %d: %v", i, err)
		}
		blockIDs[i] = blockID
	}

	// Verify we can read from all blocks
	for i, blockID := range blockIDs {
		readData, _, err := bm.Read(blockID)
		if err != nil {
			t.Fatalf("Failed to read data from block %d: %v", i, err)
		}

		if !bytes.Equal(data, readData) {
			t.Fatalf("Data mismatch for block %d. Expected: %s, Got: %s",
				i, string(data), string(readData))
		}
	}
}

func TestInvalidBlockRead(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_invalid_read_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Try to read from an invalid block ID
	_, _, err = bm.Read(-1)
	if err == nil {
		t.Fatalf("Expected an error when reading from invalid block ID")
	}

	// Try to read from a non-existent block ID (valid but too high)
	_, _, err = bm.Read(1000)
	if err == nil {
		t.Fatalf("Expected an error when reading from non-existent block ID")
	}
}

func TestEmptyAppend(t *testing.T) {
	// Create a path for a new file
	tempFilePath := os.TempDir() + "/blockmanager_empty_append_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Try to append empty data
	_, err = bm.Append([]byte{})
	if err == nil {
		t.Fatalf("Expected an error when appending empty data")
	}
}

//func TestCorruptHeader(t *testing.T) {
//	// Create a path for a new file
//	tempFilePath := os.TempDir() + "/blockmanager_corrupt_header_test"
//	defer os.Remove(tempFilePath) // Clean up after test
//
//	// Create a file with a valid header
//	{
//		file, err := os.Create(tempFilePath)
//		if err != nil {
//			t.Fatalf("Failed to create file: %v", err)
//		}
//
//		if err := writeHeader(file); err != nil {
//			t.Fatalf("Failed to write header: %v", err)
//		}
//
//		file.Close()
//	}
//
//	// Corrupt the header by writing junk to the beginning of the file
//	{
//		file, err := os.OpenFile(tempFilePath, os.O_RDWR, 0666)
//		if err != nil {
//			t.Fatalf("Failed to open file for corruption: %v", err)
//		}
//
//		// Write junk to the magic number part
//		if _, err := file.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 4); err != nil {
//			t.Fatalf("Failed to corrupt file: %v", err)
//		}
//
//		file.Close()
//	}
//
//	// Try to open the file with the corrupted header
//	_, err := Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
//	if err == nil {
//		t.Fatalf("Expected an error when opening file with corrupted header")
//	}
//}

func TestGetInitialBlockID(t *testing.T) {
	// Create a temporary file for testing
	tempFilePath := os.TempDir() + "/blockmanager_initial_block_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer bm.Close()

	// Write a single block to the file
	data := []byte("Test data for initial block")
	blockID, err := bm.Append(data)
	if err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}

	// Verify the initial block ID matches the written block's ID
	if uint32(blockID) != 1 {
		t.Fatalf("Initial block ID mismatch. Expected: %d, Got: %d", 1, blockID)
	}
}

func TestIterator(t *testing.T) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "blockmanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Open the BlockManager
	bm, err := Open(tempFile.Name(), os.O_RDWR|os.O_CREATE, 0644, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open BlockManager: %v", err)
	}
	defer bm.Close()

	// Append some data to the BlockManager
	data1 := []byte("Block 1 data")
	data2 := []byte("Block 2 data")
	data3 := make([]byte, BlockSize*2)
	data4 := []byte("Block 4 data")

	expect := make(map[string]bool)

	// Fill data3 with random bytes
	for i := 0; i < len(data3); i++ {
		data3[i] = byte(rand.Intn(256))

	}

	expect[string(data1)] = false
	expect[string(data2)] = false
	expect[string(data3)] = false
	expect[string(data4)] = false

	_, err = bm.Append(data1)
	if err != nil {
		t.Fatalf("Failed to append data1: %v", err)
	}

	_, err = bm.Append(data2)
	if err != nil {
		t.Fatalf("Failed to append data2: %v", err)
	}

	_, err = bm.Append(data3)
	if err != nil {
		t.Fatalf("Failed to append data3: %v", err)
	}

	_, err = bm.Append(data4)
	if err != nil {
		t.Fatalf("Failed to append data4: %v", err)
	}

	// Create an iterator
	iterator := bm.Iterator()

	for {
		data, _, err := iterator.Next()
		if err != nil {
			break
		}

		if _, ok := expect[string(data)]; !ok {
			t.Fatalf("Unexpected data: %s", string(data))
		}

		expect[string(data)] = true
	}

	// Check expect map
	for k, v := range expect {
		if !v {
			t.Fatalf("Expected data not found: %s", k)
		}
		v = false
	}

	for {
		data, _, err := iterator.Prev()
		if err != nil {
			break
		}

		if _, ok := expect[string(data)]; !ok {
			t.Fatalf("Unexpected data: %s", string(data))
		}

		expect[string(data)] = true
	}

	// Check expect map
	for k, v := range expect {
		if !v {
			t.Fatalf("Expected data not found: %s", k)
		}
		v = false
	}
}

func BenchmarkBlockManagerWriteSmall(b *testing.B) {
	tmpFile := os.TempDir() + "/bm_write_small_bench"
	defer os.Remove(tmpFile)

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bm.Close()

	data := []byte("This is a small benchmark write block")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bm.Append(data); err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

func BenchmarkBlockManagerWriteLarge(b *testing.B) {
	tmpFile := os.TempDir() + "/bm_write_large_bench"
	defer os.Remove(tmpFile)

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bm.Close()

	data := make([]byte, 4096*4) // 4 blocks worth
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bm.Append(data); err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

func BenchmarkBlockManagerRead(b *testing.B) {
	tmpFile := os.TempDir() + "/bm_read_bench"
	defer os.Remove(tmpFile)

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bm.Close()

	// Prepare dataset
	data := make([]byte, 512)
	rand.Read(data)

	var blockIDs []int64
	for i := 0; i < 1000; i++ {
		blockID, err := bm.Append(data)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
		blockIDs = append(blockIDs, blockID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := blockIDs[i%len(blockIDs)]
		readData, _, err := bm.Read(id)
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
		if !bytes.Equal(data, readData) {
			b.Fatal("data mismatch")
		}
	}
}

func BenchmarkBlockManagerWriteSmallParallel(b *testing.B) {
	tmpFile := os.TempDir() + "/bm_write_small_parallel_bench"
	defer os.Remove(tmpFile)

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer bm.Close()

	data := []byte("Concurrent small write")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bm.Append(data); err != nil {
				b.Fatalf("append failed: %v", err)
			}
		}
	})
}

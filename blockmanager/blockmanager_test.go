// Package blockmanager
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
package blockmanager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/guycipher/wildcat/queue"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteHeader(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := ioutil.TempDir("", "blockmanager-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test file path
	testFilePath := filepath.Join(tmpDir, "test-write-header.db")

	// Open the file for writing
	file, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	// Create a BlockManager with the file
	bm := &BlockManager{
		file: file,
		fd:   file.Fd(),
	}

	// Test the writeHeader method
	err = bm.writeHeader()
	if err != nil {
		t.Fatalf("writeHeader failed: %v", err)
	}

	// Verify file size matches expected header size
	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}

	expectedHeaderSize := binary.Size(Header{})
	if fileInfo.Size() != int64(expectedHeaderSize) {
		t.Errorf("File size mismatch. Expected: %d, Got: %d", expectedHeaderSize, fileInfo.Size())
	}

	// Read the header back to verify
	header := make([]byte, expectedHeaderSize)
	_, err = file.ReadAt(header, 0)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	// Parse the header to verify values
	var parsedHeader Header
	err = binary.Read(bytes.NewReader(header), binary.LittleEndian, &parsedHeader)
	if err != nil {
		t.Fatalf("Failed to parse header: %v", err)
	}

	// Verify header fields
	if parsedHeader.MagicNumber != MagicNumber {
		t.Errorf("Magic number mismatch. Expected: %d, Got: %d", MagicNumber, parsedHeader.MagicNumber)
	}
	if parsedHeader.Version != Version {
		t.Errorf("Version mismatch. Expected: %d, Got: %d", Version, parsedHeader.Version)
	}
	if parsedHeader.BlockSize != BlockSize {
		t.Errorf("Block size mismatch. Expected: %d, Got: %d", BlockSize, parsedHeader.BlockSize)
	}
	if parsedHeader.Allotment != Allotment {
		t.Errorf("Allotment mismatch. Expected: %d, Got: %d", Allotment, parsedHeader.Allotment)
	}

	// Verify CRC
	expectedCRC := parsedHeader.CRC
	parsedHeader.CRC = 0

	// Create buffer with CRC set to 0 to calculate expected CRC
	bufWithoutCRC := new(bytes.Buffer)
	err = binary.Write(bufWithoutCRC, binary.LittleEndian, &parsedHeader)
	if err != nil {
		t.Fatalf("Failed to write header for CRC check: %v", err)
	}
	calculatedCRC := crc32.ChecksumIEEE(bufWithoutCRC.Bytes())

	if calculatedCRC != expectedCRC {
		t.Errorf("CRC mismatch. Expected: %d, Got: %d", expectedCRC, calculatedCRC)
	}
}

func TestReadHeader(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := ioutil.TempDir("", "blockmanager-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test file path
	testFilePath := filepath.Join(tmpDir, "test-read-header.db")

	// Open the file for writing
	file, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	// Create a BlockManager with the file
	bm := &BlockManager{
		file: file,
		fd:   file.Fd(),
	}

	// First, write a header to the file
	err = bm.writeHeader()
	if err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	// Test readHeader
	err = bm.readHeader()
	if err != nil {
		t.Fatalf("readHeader failed: %v", err)
	}

	// Test with invalid magic number
	invalidFile := filepath.Join(tmpDir, "invalid-magic.db")
	invalidF, err := os.OpenFile(invalidFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid test file: %v", err)
	}
	defer invalidF.Close()

	// Create an invalid header
	invalidHeader := Header{
		MagicNumber: 0x12345678, // Wrong magic number
		Version:     Version,
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	// Calculate CRC
	buf := new(bytes.Buffer)
	invalidHeader.CRC = 0
	err = binary.Write(buf, binary.LittleEndian, &invalidHeader)
	if err != nil {
		t.Fatalf("Failed to write invalid header: %v", err)
	}
	invalidHeader.CRC = crc32.ChecksumIEEE(buf.Bytes())

	// Write the header to the file
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, &invalidHeader)
	if err != nil {
		t.Fatalf("Failed to write invalid header: %v", err)
	}
	_, err = invalidF.Write(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}

	// Create BlockManager with invalid file
	invalidBM := &BlockManager{
		file: invalidF,
		fd:   invalidF.Fd(),
	}

	// Test readHeader with invalid magic number
	err = invalidBM.readHeader()
	if err == nil || err.Error() != "invalid magic number" {
		t.Errorf("Expected 'invalid magic number' error, got: %v", err)
	}

	// Test with invalid CRC
	invalidCRCFile := filepath.Join(tmpDir, "invalid-crc.db")
	invalidCRCF, err := os.OpenFile(invalidCRCFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid CRC test file: %v", err)
	}
	defer invalidCRCF.Close()

	// Create a valid header
	validHeader := Header{
		MagicNumber: MagicNumber,
		Version:     Version,
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	// Calculate CRC
	buf = new(bytes.Buffer)
	validHeader.CRC = 0
	err = binary.Write(buf, binary.LittleEndian, &validHeader)
	if err != nil {
		t.Fatalf("Failed to write valid header: %v", err)
	}
	validHeader.CRC = crc32.ChecksumIEEE(buf.Bytes())

	// Corrupt the CRC
	validHeader.CRC = validHeader.CRC + 1

	// Write the header to the file
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, &validHeader)
	if err != nil {
		t.Fatalf("Failed to write corrupted header: %v", err)
	}
	_, err = invalidCRCF.Write(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}

	// Create BlockManager with invalid CRC file
	invalidCRCBM := &BlockManager{
		file: invalidCRCF,
		fd:   invalidCRCF.Fd(),
	}

	// Test readHeader with invalid CRC
	err = invalidCRCBM.readHeader()
	if err == nil || err.Error() != "header CRC mismatch" {
		t.Errorf("Expected 'header CRC mismatch' error, got: %v", err)
	}

	// Test with unsupported version
	invalidVersionFile := filepath.Join(tmpDir, "invalid-version.db")
	invalidVersionF, err := os.OpenFile(invalidVersionFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create invalid version test file: %v", err)
	}
	defer invalidVersionF.Close()

	// Create a header with invalid version
	invalidVersionHeader := Header{
		MagicNumber: MagicNumber,
		Version:     Version + 1, // Unsupported version
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	// Calculate CRC
	buf = new(bytes.Buffer)
	invalidVersionHeader.CRC = 0
	err = binary.Write(buf, binary.LittleEndian, &invalidVersionHeader)
	if err != nil {
		t.Fatalf("Failed to write invalid version header: %v", err)
	}
	invalidVersionHeader.CRC = crc32.ChecksumIEEE(buf.Bytes())

	// Write the header to the file
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, &invalidVersionHeader)
	if err != nil {
		t.Fatalf("Failed to write invalid version header: %v", err)
	}
	_, err = invalidVersionF.Write(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}

	// Create BlockManager with invalid version file
	invalidVersionBM := &BlockManager{
		file: invalidVersionF,
		fd:   invalidVersionF.Fd(),
	}

	// Test readHeader with invalid version
	err = invalidVersionBM.readHeader()
	if err == nil || err.Error() != "unsupported version" {
		t.Errorf("Expected 'unsupported version' error, got: %v", err)
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := ioutil.TempDir("", "blockmanager-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test file path
	testFilePath := filepath.Join(tmpDir, "test-round-trip.db")

	// Open the file for writing
	file, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	// Create a BlockManager with the file
	bm := &BlockManager{
		file: file,
		fd:   file.Fd(),
	}

	// Write the header
	err = bm.writeHeader()
	if err != nil {
		t.Fatalf("writeHeader failed: %v", err)
	}

	// Close the file to ensure data is flushed
	err = file.Close()
	if err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Reopen the file
	reopenedFile, err := os.OpenFile(testFilePath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to reopen test file: %v", err)
	}
	defer reopenedFile.Close()

	// Create a new BlockManager with the reopened file
	reopenedBM := &BlockManager{
		file: reopenedFile,
		fd:   reopenedFile.Fd(),
	}

	// Read the header
	err = reopenedBM.readHeader()
	if err != nil {
		t.Fatalf("readHeader failed: %v", err)
	}

	// If we get here, the test has passed
	t.Log("Successfully wrote and read header")
}

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
	// Convert the queue to a slice to count elements
	count := 0
	for !bm.allocationTable.IsEmpty() {
		bm.allocationTable.Dequeue()
		count++
	}

	if count != int(Allotment) {
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

		//log.Println(string(data))

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

func TestScanForFreeBlocks(t *testing.T) {
	// Create a temporary file for testing
	tempFilePath := os.TempDir() + "/blockmanager_scan_free_test"
	defer os.Remove(tempFilePath) // Clean up after test

	// Open a new file
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Write some initial data blocks
	data1 := []byte("First block data")
	data2 := []byte("Second block data")

	// Append data blocks
	_, err = bm.Append(data1)
	if err != nil {
		t.Fatalf("Failed to append data1: %v", err)
	}

	_, err = bm.Append(data2)
	if err != nil {
		t.Fatalf("Failed to append data2: %v", err)
	}

	// Get the number of free blocks before closing
	freeBlocksBeforeClose := 0
	originalAllocationTable := bm.allocationTable
	for !originalAllocationTable.IsEmpty() {
		originalAllocationTable.Dequeue()
		freeBlocksBeforeClose++
	}

	// Close the file
	if err := bm.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Reopen the file to trigger scanForFreeBlocks
	bm, err = Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to reopen file: %v", err)
	}
	defer bm.Close()

	// Count free blocks after reopening
	freeBlocksAfterReopen := 0
	for !bm.allocationTable.IsEmpty() {
		bm.allocationTable.Dequeue()
		freeBlocksAfterReopen++
	}

	// Verify number of free blocks is the same
	if freeBlocksBeforeClose != freeBlocksAfterReopen {
		t.Fatalf("Free blocks count mismatch. Before: %d, After: %d",
			freeBlocksBeforeClose, freeBlocksAfterReopen)
	}

	// Reset allocation table for the next test
	bm.allocationTable = queue.New()

	// Append a large number of blocks to verify scanning from end efficiency
	largeDataCount := 100
	// Keep track of block IDs we've written to
	usedBlockIDs := make([]int64, largeDataCount)

	for i := 0; i < largeDataCount; i++ {
		data := []byte(fmt.Sprintf("Test data block %d", i))
		blockID, err := bm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append data block %d: %v", i, err)
		}
		usedBlockIDs[i] = blockID
	}

	// Close and reopen to force scan
	if err := bm.Close(); err != nil {
		t.Fatalf("Failed to close file again: %v", err)
	}

	// Start measuring time for the optimized scan
	scanStart := time.Now()

	bm, err = Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to reopen file again: %v", err)
	}
	defer bm.Close()

	scanDuration := time.Since(scanStart)

	// The optimization should make the scan efficient even with many blocks
	t.Logf("Scanning %d blocks took %v", largeDataCount, scanDuration)

	// Verify we can still read all the blocks correctly
	for i, blockID := range usedBlockIDs {
		expectedData := []byte(fmt.Sprintf("Test data block %d", i))
		readData, _, err := bm.Read(blockID)
		if err != nil {
			t.Fatalf("Failed to read data from block %d: %v", i, err)
		}

		if !bytes.Equal(expectedData, readData) {
			t.Fatalf("Data mismatch for block %d. Expected: %s, Got: %s",
				i, string(expectedData), string(readData))
		}
	}

	// Test edge case: Append free blocks at the end, then blocks with data, then scan
	// First close the current file
	if err := bm.Close(); err != nil {
		t.Fatalf("Failed to close file for edge case test: %v", err)
	}

	// Create a new file for this specific test
	edgeFilePath := os.TempDir() + "/blockmanager_scan_edge_test"
	defer os.Remove(edgeFilePath)

	edgeBm, err := Open(edgeFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open edge case file: %v", err)
	}

	// Exhaust all initial free blocks by writing something to them
	initialFreeBlocks := 0
	for !edgeBm.allocationTable.IsEmpty() {
		edgeBm.allocationTable.Dequeue()
		initialFreeBlocks++
	}

	for i := 0; i < initialFreeBlocks; i++ {
		data := []byte(fmt.Sprintf("Initial block %d", i))
		_, err := edgeBm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append initial data: %v", err)
		}
	}

	// Force allocation of new free blocks at the end
	if err := edgeBm.appendFreeBlocks(); err != nil {
		t.Fatalf("Failed to append free blocks: %v", err)
	}

	// Write data after the free blocks to create a non-continuous pattern
	data := []byte("Data after free blocks")
	_, err = edgeBm.Append(data)
	if err != nil {
		t.Fatalf("Failed to append data after free blocks: %v", err)
	}

	// Close and reopen to force scan
	if err := edgeBm.Close(); err != nil {
		t.Fatalf("Failed to close edge case file: %v", err)
	}

	edgeBm, err = Open(edgeFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to reopen edge case file: %v", err)
	}
	defer edgeBm.Close()

	// Verify we have free blocks in the allocation table
	if edgeBm.allocationTable.IsEmpty() {
		t.Fatalf("Expected free blocks in allocation table after edge case test")
	}

	// Make sure we can still append and read data
	verifyData := []byte("Verification data after edge case")
	verifyBlockID, err := edgeBm.Append(verifyData)
	if err != nil {
		t.Fatalf("Failed to append verification data: %v", err)
	}

	readVerifyData, _, err := edgeBm.Read(verifyBlockID)
	if err != nil {
		t.Fatalf("Failed to read verification data: %v", err)
	}

	if !bytes.Equal(verifyData, readVerifyData) {
		t.Fatalf("Verification data mismatch. Expected: %s, Got: %s",
			string(verifyData), string(readVerifyData))
	}
}

func TestPartialBlockWriteRecovery(t *testing.T) {
	tempFilePath := os.TempDir() + "/bm_partial_block_test"
	defer os.Remove(tempFilePath)

	// Write a broken block manually
	f, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}

	header := Header{
		MagicNumber: MagicNumber,
		Version:     Version,
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}
	header.CRC = 0
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, &header)
	header.CRC = crc32.ChecksumIEEE(buf.Bytes())

	buf.Reset()
	_ = binary.Write(buf, binary.LittleEndian, &header)

	// Write header
	_, _ = f.WriteAt(buf.Bytes(), 0)

	// Manually write a truncated block header (simulate crash mid-write)
	incompleteBlockHeader := make([]byte, 4) // only CRC field
	copy(incompleteBlockHeader, []byte{0x00, 0x00, 0x00, 0x00})
	_, _ = f.WriteAt(incompleteBlockHeader, int64(binary.Size(Header{})+int(BlockSize)))

	f.Close()

	// Now open with blockmanager (should not panic or fail)
	bm, err := Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Open failed on partial block recovery: %v", err)
	}

	defer bm.Close()

	// Append some data to trigger recovery
	data := []byte("Test data after partial block write")

	blockID, err := bm.Append(data)

	if err != nil {
		t.Fatalf("Failed to append data after partial block write: %v", err)
	}

	// Read back the data
	readData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read data after partial block write: %v", err)
	}

	// Verify the data
	if !bytes.Equal(data, readData) {
		t.Fatalf("Data mismatch after partial block write. Expected: %s, Got: %s", string(data), string(readData))

	}

}

func TestPowerOutageDuringHeaderWrite(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_power_outage_test"
	defer os.Remove(tempFilePath)

	// Simulate writing only half the header
	file, err := os.Create(tempFilePath)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	header := Header{
		MagicNumber: MagicNumber,
		Version:     Version,
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, &header)

	// Write only part of the header
	corruptedBytes := buf.Bytes()[:10]
	if _, err := file.Write(corruptedBytes); err != nil {
		t.Fatalf("Failed to write corrupted header: %v", err)
	}
	file.Close()

	// Try to open the block manager — it should fail
	_, err = Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err == nil {
		t.Fatalf("Expected error due to corrupted header, got nil")
	}

	t.Logf("Power outage test passed. Error: %v", err)
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

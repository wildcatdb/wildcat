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
	"github.com/wildcatdb/wildcat/v2/queue"
	"hash/crc32"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWriteHeader(t *testing.T) {

	tmpDir := os.TempDir()

	testFilePath := filepath.Join(tmpDir, "test-write-header.db")

	file, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)

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

	tmpDir := os.TempDir()

	testFilePath := filepath.Join(tmpDir, "test-read-header.db")

	file, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)

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
	defer func(invalidF *os.File) {
		_ = invalidF.Close()
	}(invalidF)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(invalidFile)

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
	defer func(invalidCRCF *os.File) {
		_ = invalidCRCF.Close()
	}(invalidCRCF)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(invalidCRCFile)

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
	defer func(invalidVersionF *os.File) {
		_ = invalidVersionF.Close()
	}(invalidVersionF)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(invalidVersionFile)

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

	tmpDir := os.TempDir()

	testFilePath := filepath.Join(tmpDir, "test-round-trip.db")

	file, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)

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
	defer func(reopenedFile *os.File) {
		_ = reopenedFile.Close()
	}(reopenedFile)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(testFilePath)

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

	tempFilePath := os.TempDir() + "/blockmanager_open_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open new file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Check if the allocation table has the expected initial blocks
	if bm.allocationTable.IsEmpty() {
		t.Fatalf("Allocation table should not be empty for a new file")
	}

	// Check that we have Allotment number of blocks
	// Convert the queue to a slice to count elements
	count := uint64(0) // The block manager's default allotment is 16 blocks thus
	// we expect 16 blocks in the allocation table
	for !bm.allocationTable.IsEmpty() {
		bm.allocationTable.Dequeue()
		count++

	}

	log.Println("Number of blocks in allocation table:", count)

	if count != Allotment {
		t.Fatalf("Expected %d blocks in allocation table, got %d", Allotment, count)
	}
}

func TestAppendSmallData(t *testing.T) {

	tempFilePath := os.TempDir() + "/blockmanager_append_small_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	tempFilePath := os.TempDir() + "/blockmanager_append_large_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	tempFilePath := os.TempDir() + "/blockmanager_multiple_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	tempFilePath := os.TempDir() + "/blockmanager_reopen_test"

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

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Second session: reopen file and read data
	{
		bm, err := Open(tempFilePath, os.O_RDWR, 0666, SyncPartial, time.Millisecond*24)
		if err != nil {
			t.Fatalf("Failed to reopen file: %v", err)
		}

		defer func(bm *BlockManager) {
			_ = bm.Close()
		}(bm)

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

	tempFilePath := os.TempDir() + "/blockmanager_allotment_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	tempFilePath := os.TempDir() + "/blockmanager_invalid_read_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	tempFilePath := os.TempDir() + "/blockmanager_empty_append_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Try to append empty data
	_, err = bm.Append([]byte{})
	if err == nil {
		t.Fatalf("Expected an error when appending empty data")
	}
}

func TestGetInitialBlockID(t *testing.T) {

	tempFilePath := os.TempDir() + "/blockmanager_initial_block_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	tempFile, err := os.CreateTemp("", "blockmanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	bm, err := Open(tempFile.Name(), os.O_RDWR|os.O_CREATE, 0644, SyncPartial, time.Millisecond*24)
	if err != nil {
		t.Fatalf("Failed to open BlockManager: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFile.Name())

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

func TestIteratorFromBlock(t *testing.T) {
	tempFile, err := os.CreateTemp("", "blockmanager_iterator_from_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	bm, err := Open(tempFile.Name(), os.O_RDWR|os.O_CREATE, 0644, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open BlockManager: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFile.Name())

	// Append several blocks of data
	testData := [][]byte{
		[]byte("Block 1 data"),
		[]byte("Block 2 data"),
		[]byte("Block 3 data"),
		[]byte("Block 4 data"),
		[]byte("Block 5 data"),
	}

	blockIDs := make([]int64, len(testData))
	for i, data := range testData {
		blockID, err := bm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append data %d: %v", i, err)
		}
		blockIDs[i] = blockID
	}

	// Test iterator starting from block 3
	iterator := bm.IteratorFromBlock(3)
	if iterator == nil {
		t.Fatalf("IteratorFromBlock returned nil")
	}

	// Should start reading from block 3
	expectedStartData := []string{"Block 3 data", "Block 4 data", "Block 5 data"}

	got := make([]string, 0)

	for {
		data, _, err := iterator.Next()
		if err != nil {
			break
		}
		got = append(got, string(data))

	}

	if len(got) != len(expectedStartData) {
		t.Fatalf("Expected %d blocks, got %d", len(expectedStartData), len(got))

	}

}

func TestScanForFreeBlocks(t *testing.T) {

	tempFilePath := os.TempDir() + "/blockmanager_scan_free_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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
	defer func(bm *BlockManager) {
		_ = bm.Close()

	}(bm)

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
	defer func(bm *BlockManager) {
		_ = bm.Close()

	}(bm)

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

	edgeBm, err := Open(edgeFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open edge case file: %v", err)
	}

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(edgeFilePath)

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

	defer func(edgeBm *BlockManager) {
		_ = edgeBm.Close()
	}(edgeBm)

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

	// Write a broken block manually
	f, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	_ = f.Close()

	// Now open with blockmanager (should not panic or fail)
	bm, err := Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Open failed on partial block recovery: %v", err)
	}

	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

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

	// Simulate writing only half the header
	file, err := os.Create(tempFilePath)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	defer func() {
		_ = os.RemoveAll(tempFilePath)
	}()

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

	_ = file.Close()

	// Try to open the block manager — it should fail
	_, err = Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err == nil {
		t.Fatalf("Expected error due to corrupted header, got nil")
	}

	t.Logf("Power outage test passed. Error: %v", err)
}

func TestUpdateSameSize(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_update_same_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Append initial data
	originalData := []byte("This is the original data")
	blockID, err := bm.Append(originalData)
	if err != nil {
		t.Fatalf("Failed to append original data: %v", err)
	}

	// Update with same size data
	newData := []byte("This is the updated data!")
	updatedBlockID, err := bm.Update(blockID, newData)
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Verify block ID remains the same
	if updatedBlockID != blockID {
		t.Errorf("Block ID changed after update. Expected: %d, Got: %d", blockID, updatedBlockID)
	}

	// Read and verify updated data
	readData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read updated data: %v", err)
	}

	if !bytes.Equal(newData, readData) {
		t.Errorf("Updated data mismatch. Expected: %s, Got: %s", string(newData), string(readData))
	}
}

func TestUpdateSmallerData(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_update_smaller_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()

	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Append large data that spans multiple blocks
	blockHeaderSize := 16 // Size of BlockHeader struct
	dataPerBlock := int(BlockSize) - blockHeaderSize
	largeData := make([]byte, dataPerBlock*3) // 3 blocks worth
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	blockID, err := bm.Append(largeData)
	if err != nil {
		t.Fatalf("Failed to append large data: %v", err)
	}

	// Update with much smaller data
	smallData := []byte("Small update")
	updatedBlockID, err := bm.Update(blockID, smallData)
	if err != nil {
		t.Fatalf("Failed to update with smaller data: %v", err)
	}

	// Verify block ID remains the same
	if updatedBlockID != blockID {
		t.Errorf("Block ID changed after update. Expected: %d, Got: %d", blockID, updatedBlockID)
	}

	// Read and verify updated data
	readData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read updated data: %v", err)
	}

	if !bytes.Equal(smallData, readData) {
		t.Errorf("Updated data mismatch. Expected: %s, Got: %s", string(smallData), string(readData))
	}
}

func TestUpdateLargerData(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_update_larger_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Append small initial data
	smallData := []byte("Small data")
	blockID, err := bm.Append(smallData)
	if err != nil {
		t.Fatalf("Failed to append small data: %v", err)
	}

	// Update with much larger data that spans multiple blocks
	blockHeaderSize := 16 // Size of BlockHeader struct
	dataPerBlock := int(BlockSize) - blockHeaderSize
	largeData := make([]byte, dataPerBlock*3) // 3 blocks worth
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	updatedBlockID, err := bm.Update(blockID, largeData)
	if err != nil {
		t.Fatalf("Failed to update with larger data: %v", err)
	}

	// Verify block ID remains the same
	if updatedBlockID != blockID {
		t.Errorf("Block ID changed after update. Expected: %d, Got: %d", blockID, updatedBlockID)
	}

	// Read and verify updated data
	readData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read updated data: %v", err)
	}

	if !bytes.Equal(largeData, readData) {
		t.Errorf("Updated data length mismatch. Expected: %d, Got: %d", len(largeData), len(readData))
	}
}

func TestUpdateMultipleBlocks(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_update_multiple_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Create several blocks with different data
	data1 := []byte("First block data")
	data2 := []byte("Second block data")
	data3 := []byte("Third block data")

	blockID1, err := bm.Append(data1)
	if err != nil {
		t.Fatalf("Failed to append data1: %v", err)
	}

	blockID2, err := bm.Append(data2)
	if err != nil {
		t.Fatalf("Failed to append data2: %v", err)
	}

	blockID3, err := bm.Append(data3)
	if err != nil {
		t.Fatalf("Failed to append data3: %v", err)
	}

	// Update middle block
	newData2 := []byte("Updated second block")
	updatedBlockID, err := bm.Update(blockID2, newData2)
	if err != nil {
		t.Fatalf("Failed to update middle block: %v", err)
	}

	if updatedBlockID != blockID2 {
		t.Errorf("Block ID changed after update. Expected: %d, Got: %d", blockID2, updatedBlockID)
	}

	// Verify all blocks still readable and correct
	readData1, _, err := bm.Read(blockID1)
	if err != nil || !bytes.Equal(data1, readData1) {
		t.Errorf("First block corrupted after middle block update")
	}

	readData2, _, err := bm.Read(blockID2)
	if err != nil || !bytes.Equal(newData2, readData2) {
		t.Errorf("Updated middle block incorrect")
	}

	readData3, _, err := bm.Read(blockID3)
	if err != nil || !bytes.Equal(data3, readData3) {
		t.Errorf("Third block corrupted after middle block update")
	}
}

func TestUpdateInvalidBlockID(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_update_invalid_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Try to update non-existent block
	newData := []byte("Update data")
	_, err = bm.Update(999, newData)
	if err == nil {
		t.Fatalf("Expected error when updating non-existent block")
	}

	// Try to update with invalid block ID
	_, err = bm.Update(-1, newData)
	if err == nil {
		t.Fatalf("Expected error when updating with invalid block ID")
	}

	// Try to update with empty data
	_, err = bm.Update(1, []byte{})
	if err == nil {
		t.Fatalf("Expected error when updating with empty data")
	}
}

func TestUpdatePreservesOtherData(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_update_preserve_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Create multiple entries
	testEntries := [][]byte{
		[]byte("Entry 1: Some data here"),
		[]byte("Entry 2: Different data"),
		[]byte("Entry 3: Yet another entry"),
		make([]byte, 1000), // Large entry
		[]byte("Entry 5: Final entry"),
	}

	// Fill large entry with pattern
	for i := range testEntries[3] {
		testEntries[3][i] = byte(i % 256)
	}

	// Store all entries
	blockIDs := make([]int64, len(testEntries))
	for i, data := range testEntries {
		blockID, err := bm.Append(data)
		if err != nil {
			t.Fatalf("Failed to append entry %d: %v", i, err)
		}
		blockIDs[i] = blockID
	}

	// Update middle entry with different sized data
	newMiddleData := []byte("Updated middle entry with completely different data that's longer")
	_, err = bm.Update(blockIDs[2], newMiddleData)
	if err != nil {
		t.Fatalf("Failed to update middle entry: %v", err)
	}

	// Verify all other entries are unchanged
	for i, expectedData := range testEntries {
		if i == 2 {
			continue // Skip the updated entry
		}

		readData, _, err := bm.Read(blockIDs[i])
		if err != nil {
			t.Fatalf("Failed to read entry %d after update: %v", i, err)
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("Entry %d was corrupted after updating different entry", i)
		}
	}

	// Verify updated entry
	readUpdatedData, _, err := bm.Read(blockIDs[2])
	if err != nil {
		t.Fatalf("Failed to read updated entry: %v", err)
	}

	if !bytes.Equal(newMiddleData, readUpdatedData) {
		t.Errorf("Updated entry data mismatch")
	}
}

func TestConcurrentAppends(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_concurrent_appends_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Number of concurrent goroutines and operations per goroutine
	numGoroutines := 10
	operationsPerGoroutine := 20

	// Channel to collect results
	resultChan := make(chan struct {
		goroutineID int
		blockID     int64
		data        []byte
		err         error
	}, numGoroutines*operationsPerGoroutine)

	var wg sync.WaitGroup

	// Start concurrent append operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				data := []byte(fmt.Sprintf("Goroutine %d, Operation %d: Some test data", goroutineID, j))

				blockID, err := bm.Append(data)

				// Send result through channel
				resultChan <- struct {
					goroutineID int
					blockID     int64
					data        []byte
					err         error
				}{goroutineID, blockID, data, err}
			}
		}(i)
	}

	// Close result channel when all goroutines finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and check for duplicates properly
	blockIDSet := make(map[int64]bool) // Track which block IDs we've seen
	results := make([]struct {
		goroutineID int
		blockID     int64
		data        []byte
	}, 0, numGoroutines*operationsPerGoroutine)

	errorCount := 0
	duplicateCount := 0

	for result := range resultChan {
		if result.err != nil {
			t.Errorf("Append failed for goroutine %d: %v", result.goroutineID, result.err)
			errorCount++
			continue
		}

		// Check for duplicate block IDs - this should never happen with proper atomic allocation
		if blockIDSet[result.blockID] {
			t.Errorf("Duplicate block ID %d allocated! (goroutine %d)", result.blockID, result.goroutineID)
			duplicateCount++
		} else {
			blockIDSet[result.blockID] = true
		}

		results = append(results, struct {
			goroutineID int
			blockID     int64
			data        []byte
		}{result.goroutineID, result.blockID, result.data})
	}

	if errorCount > 0 {
		t.Fatalf("Had %d errors during concurrent appends", errorCount)
	}

	if duplicateCount > 0 {
		t.Fatalf("Found %d duplicate block IDs - atomic allocation failed!", duplicateCount)
	}

	// Verify all data can be read back correctly
	for _, result := range results {
		readData, _, err := bm.Read(result.blockID)
		if err != nil {
			t.Errorf("Failed to read block %d: %v", result.blockID, err)
			continue
		}

		if !bytes.Equal(result.data, readData) {
			t.Errorf("Data mismatch for block %d. Expected: %s, Got: %s",
				result.blockID, string(result.data), string(readData))
		}
	}

	// Verify we have the expected number of results
	expectedResults := numGoroutines * operationsPerGoroutine
	if len(results) != expectedResults {
		t.Errorf("Expected %d results, got %d", expectedResults, len(results))
	}

	// Verify block IDs are unique and > 0
	uniqueBlockIDs := len(blockIDSet)
	if uniqueBlockIDs != expectedResults {
		t.Errorf("Expected %d unique block IDs, got %d", expectedResults, uniqueBlockIDs)
	}

	// Check that all block IDs are > 0 (block ID 0 is reserved)
	for blockID := range blockIDSet {
		if blockID <= 0 {
			t.Errorf("Invalid block ID allocated: %d", blockID)
		}
	}

	t.Logf("Successfully completed %d concurrent appends with no duplicate block IDs", len(results))
}

func TestUpdateDuringConcurrentRead(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_concurrent_update_read_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Create initial data
	originalData := []byte("Original data that will be updated during concurrent reads")
	blockID, err := bm.Append(originalData)
	if err != nil {
		t.Fatalf("Failed to append original data: %v", err)
	}

	// Updated data (same size to test in-place update)
	updatedData := []byte("Updated data!! that replaces original during concurrent reads")

	// Channels for coordination
	startReaders := make(chan struct{})
	readersStarted := make(chan struct{})
	updateComplete := make(chan error, 1)

	// Channel to collect read results
	readResults := make(chan struct {
		data      []byte
		err       error
		timestamp time.Time
	}, 200)

	// Number of concurrent readers
	numReaders := 5
	readsPerReader := 10
	var readersWG sync.WaitGroup

	// Start concurrent readers
	for i := 0; i < numReaders; i++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()

			// Signal that this reader is ready
			if readerID == 0 {
				close(readersStarted)
			}

			// Wait for signal to start reading
			<-startReaders

			// Perform multiple reads
			for j := 0; j < readsPerReader; j++ {
				data, _, err := bm.Read(blockID)
				readResults <- struct {
					data      []byte
					err       error
					timestamp time.Time
				}{data, err, time.Now()}

				// Small delay between reads
				time.Sleep(time.Microsecond * 500)
			}
		}(i)
	}

	// Wait for readers to be ready
	<-readersStarted

	// Start update goroutine
	go func() {
		// Wait a bit for readers to start
		time.Sleep(time.Millisecond * 5)

		// Perform the update
		_, err := bm.Update(blockID, updatedData)
		updateComplete <- err
	}()

	// Start all readers
	close(startReaders)

	// Wait for update to complete
	updateErr := <-updateComplete
	if updateErr != nil {
		t.Fatalf("Update failed: %v", updateErr)
	}

	// Wait for all readers to complete
	readersWG.Wait()
	close(readResults)

	// Analyze read results
	var originalDataReads, updatedDataReads, errorReads, inconsistentReads int
	var firstInconsistentData string

	for result := range readResults {
		if result.err != nil {
			errorReads++
			t.Logf("Read error: %v", result.err)
			continue
		}

		if bytes.Equal(result.data, originalData) {
			originalDataReads++
		} else if bytes.Equal(result.data, updatedData) {
			updatedDataReads++
		} else {
			// This indicates data corruption or partial writes
			inconsistentReads++
			if firstInconsistentData == "" {
				firstInconsistentData = string(result.data)
			}
		}
	}

	// Verify results
	if errorReads > 0 {
		t.Logf("Had %d read errors during concurrent update (may be acceptable)", errorReads)
	}

	// Most critical we have no inconsistent/corrupted data
	if inconsistentReads > 0 {
		t.Errorf("Found %d reads with inconsistent data. First example: %s",
			inconsistentReads, firstInconsistentData)
	}

	// Should have seen some combination of original and updated data
	totalValidReads := originalDataReads + updatedDataReads
	expectedTotalReads := numReaders * readsPerReader

	if totalValidReads != expectedTotalReads-errorReads {
		t.Errorf("Read count mismatch. Expected %d valid reads, got %d",
			expectedTotalReads-errorReads, totalValidReads)
	}

	// Final verification read the block to ensure it has the updated data
	finalData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read final data: %v", err)
	}

	if !bytes.Equal(finalData, updatedData) {
		t.Errorf("Final data verification failed. Expected updated data, got: %s", string(finalData))
	}

	t.Logf("Concurrent read/update test completed:")
	t.Logf("  - Original data reads: %d", originalDataReads)
	t.Logf("  - Updated data reads: %d", updatedDataReads)
	t.Logf("  - Error reads: %d", errorReads)
	t.Logf("  - Inconsistent reads: %d (should be 0)", inconsistentReads)
	t.Logf("  - Final data is correctly updated")

	if inconsistentReads == 0 {
		t.Log("✓ No data corruption detected during concurrent access")
	}
}

func TestIteratorWithCorruptedBlocks(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_iterator_corrupted_test"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Create some valid blocks
	validData1 := []byte("Valid block 1 data")
	validData2 := []byte("Valid block 2 data")
	validData3 := []byte("Valid block 3 data")

	blockID1, err := bm.Append(validData1)
	if err != nil {
		t.Fatalf("Failed to append valid data 1: %v", err)
	}

	blockID2, err := bm.Append(validData2)
	if err != nil {
		t.Fatalf("Failed to append valid data 2: %v", err)
	}

	blockID3, err := bm.Append(validData3)
	if err != nil {
		t.Fatalf("Failed to append valid data 3: %v", err)
	}

	// Close BlockManager to corrupt the file
	err = bm.Close()
	if err != nil {
		t.Fatalf("Failed to close BlockManager: %v", err)
	}

	// Manually corrupt the middle block's CRC
	file, err := os.OpenFile(tempFilePath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to reopen file: %v", err)
	}

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	headerSize := binary.Size(Header{})

	// Calculate position of block 2 and corrupt its CRC (first 4 bytes)
	position := int64(headerSize) + int64(blockID2)*int64(BlockSize)
	corruptedCRC := []byte{0xFF, 0xFF, 0xFF, 0xFF} // Invalid CRC
	_, err = file.WriteAt(corruptedCRC, position)
	if err != nil {
		t.Fatalf("Failed to corrupt block CRC: %v", err)
	}

	err = file.Close()
	if err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Reopen with BlockManager
	bm, err = Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to reopen BlockManager: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	// Test that corrupted block can't be read directly
	_, _, err = bm.Read(blockID2)
	if err == nil {
		t.Errorf("Expected error when reading corrupted block, got nil")
	} else {
		t.Logf("Corrupted block correctly rejected: %v", err)
	}

	// Test that valid blocks can still be read
	readData1, _, err := bm.Read(blockID1)
	if err != nil {
		t.Errorf("Failed to read valid block 1: %v", err)
	} else if !bytes.Equal(validData1, readData1) {
		t.Errorf("Valid block 1 data mismatch")
	}

	readData3, _, err := bm.Read(blockID3)
	if err != nil {
		t.Errorf("Failed to read valid block 3: %v", err)
	} else if !bytes.Equal(validData3, readData3) {
		t.Errorf("Valid block 3 data mismatch")
	}

	// Test iterator - it should skip corrupted blocks
	iterator := bm.Iterator()

	validBlocksFound := 0
	corruptedBlocksEncountered := 0

	for {
		data, blockId, err := iterator.Next()
		if err != nil {
			break // End of iteration
		}

		dataStr := string(data)
		t.Logf("Iterator found block %d with data: %s", blockId, dataStr)

		// Check if this is one of our valid blocks
		if bytes.Equal(data, validData1) || bytes.Equal(data, validData3) {
			validBlocksFound++
		} else if bytes.Equal(data, validData2) {
			// This should not happen - corrupted block should be skipped
			corruptedBlocksEncountered++
			t.Errorf("Iterator returned corrupted block data: %s", dataStr)
		}
		// Note that iterator might also find other allocated blocks, which is fine
	}

	if corruptedBlocksEncountered > 0 {
		t.Errorf("Iterator returned %d corrupted blocks", corruptedBlocksEncountered)
	}

	t.Logf("Iterator found %d valid blocks and correctly skipped corrupted blocks", validBlocksFound)
}

func TestBackwardCompatibilityV1ToV2(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_backward_compat_test"
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	file, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	v1Header := Header{
		MagicNumber: MagicNumber,
		Version:     1, // V1 format
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	buf := new(bytes.Buffer)
	v1Header.CRC = 0
	if err := binary.Write(buf, binary.LittleEndian, &v1Header); err != nil {
		t.Fatalf("Failed to write V1 header: %v", err)
	}
	v1Header.CRC = crc32.ChecksumIEEE(buf.Bytes())

	// Write V1 header to file
	buf.Reset()
	if err := binary.Write(buf, binary.LittleEndian, &v1Header); err != nil {
		t.Fatalf("Failed to write V1 header with CRC: %v", err)
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		t.Fatalf("Failed to write header to file: %v", err)
	}

	// Create some V1 blocks with header-only CRC
	testData := [][]byte{
		[]byte("V1 block data 1"),
		[]byte("V1 block data 2"),
		[]byte("V1 block data 3"),
	}

	blockIDs := make([]int64, len(testData))

	for i, data := range testData {
		blockID := int64(i + 1) // Block IDs start from 1
		blockIDs[i] = blockID

		// Create V1 block header (CRC only on header, not data)
		blockHeader := BlockHeader{
			BlockID:   uint64(blockID),
			DataSize:  uint64(len(data)),
			NextBlock: EndOfChain,
		}

		// Calculate V1 CRC (header only)
		headerBuf := new(bytes.Buffer)
		blockHeader.CRC = 0
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			t.Fatalf("Failed to write block header: %v", err)
		}
		blockHeader.CRC = crc32.ChecksumIEEE(headerBuf.Bytes()) // V1: header-only CRC

		// Create full block
		blockBuffer := make([]byte, BlockSize)
		headerBuf.Reset()
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			t.Fatalf("Failed to write final block header: %v", err)
		}

		copy(blockBuffer, headerBuf.Bytes())
		copy(blockBuffer[binary.Size(BlockHeader{}):], data)

		// Write block to file
		position := int64(binary.Size(Header{})) + int64(blockID)*int64(BlockSize)
		if _, err := file.WriteAt(blockBuffer, position); err != nil {
			t.Fatalf("Failed to write V1 block: %v", err)
		}
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Failed to close V1 file: %v", err)
	}

	// Open the V1 file with V2 BlockManager
	bm, err := Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open V1 file with V2 BlockManager: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	// Verify the file version was detected correctly
	if bm.fileVersion != 1 {
		t.Errorf("Expected fileVersion 1, got %d", bm.fileVersion)
	}

	//  Read V1 data successfully
	for i, expectedData := range testData {
		readData, _, err := bm.Read(blockIDs[i])
		if err != nil {
			t.Errorf("Failed to read V1 block %d: %v", i, err)
			continue
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("V1 data mismatch for block %d. Expected: %s, Got: %s",
				i, string(expectedData), string(readData))
		}
	}

	// Write new data (should use V1 format since file is V1)
	newData := []byte("New data written to V1 file")
	newBlockID, err := bm.Append(newData)
	if err != nil {
		t.Fatalf("Failed to append new data to V1 file: %v", err)
	}

	// Verify new data can be read
	readNewData, _, err := bm.Read(newBlockID)
	if err != nil {
		t.Fatalf("Failed to read new data from V1 file: %v", err)
	}

	if !bytes.Equal(newData, readNewData) {
		t.Errorf("New data mismatch. Expected: %s, Got: %s",
			string(newData), string(readNewData))
	}

	//  Update existing V1 data
	updatedData := []byte("Updated V1 data")
	_, err = bm.Update(blockIDs[0], updatedData)
	if err != nil {
		t.Fatalf("Failed to update V1 block: %v", err)
	}

	// Verify updated data
	readUpdatedData, _, err := bm.Read(blockIDs[0])
	if err != nil {
		t.Fatalf("Failed to read updated V1 data: %v", err)
	}

	if !bytes.Equal(updatedData, readUpdatedData) {
		t.Errorf("Updated data mismatch. Expected: %s, Got: %s",
			string(updatedData), string(readUpdatedData))
	}

	t.Log("V1 file successfully opened and operated on with V2 BlockManager")
}

func TestNewFileCreatedAsV2(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_new_v2_test"
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Create a new file (should be V2 by default)
	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to create new file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	// Verify new files are created as V2
	if bm.fileVersion != 2 {
		t.Errorf("Expected new file to be V2 (version 2), got version %d", bm.fileVersion)
	}

	// Test V2 functionality
	testData := []byte("Test data for V2 file")
	blockID, err := bm.Append(testData)
	if err != nil {
		t.Fatalf("Failed to append data to V2 file: %v", err)
	}

	readData, _, err := bm.Read(blockID)
	if err != nil {
		t.Fatalf("Failed to read data from V2 file: %v", err)
	}

	if !bytes.Equal(testData, readData) {
		t.Errorf("V2 data mismatch. Expected: %s, Got: %s",
			string(testData), string(readData))
	}

	t.Log("New file correctly created as V2 format")
}

func TestV1FileWithCorruptedData(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_v1_corrupted_test"
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Create a V1 file with intentionally corrupted data
	file, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create V1 header
	v1Header := Header{
		MagicNumber: MagicNumber,
		Version:     1,
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	buf := new(bytes.Buffer)
	v1Header.CRC = 0
	binary.Write(buf, binary.LittleEndian, &v1Header)
	v1Header.CRC = crc32.ChecksumIEEE(buf.Bytes())

	buf.Reset()
	binary.Write(buf, binary.LittleEndian, &v1Header)
	file.Write(buf.Bytes())

	// Create a block with corrupted data but valid V1 header CRC
	originalData := []byte("Original data")
	corruptedData := []byte("Corrupted!!!!")

	blockHeader := BlockHeader{
		BlockID:   1,
		DataSize:  uint64(len(originalData)),
		NextBlock: EndOfChain,
	}

	// Calculate valid V1 CRC (header only)
	headerBuf := new(bytes.Buffer)
	blockHeader.CRC = 0
	binary.Write(headerBuf, binary.LittleEndian, &blockHeader)
	blockHeader.CRC = crc32.ChecksumIEEE(headerBuf.Bytes())

	// Create block with corrupted data
	blockBuffer := make([]byte, BlockSize)
	headerBuf.Reset()
	binary.Write(headerBuf, binary.LittleEndian, &blockHeader)
	copy(blockBuffer, headerBuf.Bytes())
	copy(blockBuffer[binary.Size(BlockHeader{}):], corruptedData) // Write corrupted data

	position := int64(binary.Size(Header{})) + int64(BlockSize)
	file.WriteAt(blockBuffer, position)
	file.Close()

	// Open with V2 BlockManager
	bm, err := Open(tempFilePath, os.O_RDWR, 0666, SyncNone)
	if err != nil {
		t.Fatalf("Failed to open V1 file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	// V1 format should pass validation (header CRC is valid)
	// but return the corrupted data since V1 doesn't validate data
	readData, _, err := bm.Read(1)
	if err != nil {
		t.Fatalf("V1 block with corrupted data should be readable: %v", err)
	}

	// This demonstrates V1's limitation - corrupted data is returned
	if bytes.Equal(readData, originalData) {
		t.Error("Expected corrupted data to be returned (V1 limitation)")
	}

	t.Log("✓ V1 file with corrupted data demonstrates header-only CRC limitation")
}

func TestConcurrentClose(t *testing.T) {
	tempFilePath := os.TempDir() + "/blockmanager_concurrent_close_test"
	defer os.RemoveAll(tempFilePath)

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncPartial, time.Millisecond*10)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Start multiple goroutines that try to close simultaneously
	var wg sync.WaitGroup
	numGoroutines := 10
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := bm.Close()
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check if any goroutine panicked or returned an error
	for err := range errors {
		if err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	}

	t.Log("Concurrent close test passed - no panics or errors")
}

func BenchmarkUpdate(b *testing.B) {
	tempFilePath := os.TempDir() + "/blockmanager_update_bench"

	bm, err := Open(tempFilePath, os.O_RDWR|os.O_CREATE, 0666, SyncNone)
	if err != nil {
		b.Fatalf("Failed to open file: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempFilePath)

	// Prepare test data
	originalData := make([]byte, 500)
	updateData := make([]byte, 500)
	for i := range originalData {
		originalData[i] = byte(i % 256)
		updateData[i] = byte((i + 100) % 256)
	}

	// Create initial blocks for benchmarking
	blockIDs := make([]int64, b.N)
	for i := 0; i < b.N; i++ {
		blockID, err := bm.Append(originalData)
		if err != nil {
			b.Fatalf("Failed to append data: %v", err)
		}
		blockIDs[i] = blockID
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bm.Update(blockIDs[i], updateData)
		if err != nil {
			b.Fatalf("Update failed: %v", err)
		}
	}
}

func BenchmarkBlockManagerWriteSmall(b *testing.B) {
	tmpFile := os.TempDir() + "/bm_write_small_bench"

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpFile)

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

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpFile)

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

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpFile)

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

	bm, err := Open(tmpFile, os.O_CREATE|os.O_RDWR, 0666, SyncNone)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer func(bm *BlockManager) {
		_ = bm.Close()
	}(bm)

	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpFile)

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

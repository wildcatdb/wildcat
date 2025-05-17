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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"orindb/stack"
	"os"
	"sync"
	"syscall"
	"time"
)

const MagicNumber = uint32(0x4F52494E)        // "ORIN" for OrinDB
const Version = uint32(1)                     // Version of the file format
const BlockSize = uint32(512)                 // Smaller the better, faster in our tests
const Allotment = uint64(16)                  // How many blocks we can allot at once from the file
const EndOfChain = uint64(0xFFFFFFFFFFFFFFFF) // Marker for end of blockchain (overflowed block)

// SyncOption defines the synchronization options for the file
type SyncOption int

const (
	SyncNone    SyncOption = iota // Don't sync at all
	SyncFull                      // Do a sync after every write
	SyncPartial                   // Do a sync in the background at intervals
)

// Header represents the header of the file
type Header struct {
	CRC         uint32 // CRC32 checksum of the header
	MagicNumber uint32 // Magic number to identify the file format
	Version     uint32 // Version of the file format
	BlockSize   uint32 // Size of each block in bytes
	Allotment   uint64 // Number of blocks to allot at once
}

// BlockHeader represents the header of a block in the file
type BlockHeader struct {
	CRC       uint32 // CRC32 checksum of the block header
	BlockID   uint64 // Changed from uint32 to uint64
	DataSize  uint64 // Changed from uint32 to uint64
	NextBlock uint64 // Changed from uint32 to uint64
}

// BlockManager manages the allocation and deallocation of blocks in a file
type BlockManager struct {
	allocationTable *stack.Stack    // An atomic stack we store free available block ids
	file            *os.File        // File handle for the block manager
	fd              uintptr         // File descriptor for direct syscalls
	syncOption      SyncOption      // Synchronization option for the file
	syncInterval    time.Duration   // Interval for background sync (if applicable)
	closeChan       chan struct{}   // Channel to signal closure of the background sync
	wg              *sync.WaitGroup // WaitGroup to wait for background sync to finish
}

// Iterator is used to traverse the blocks in the file
type Iterator struct {
	blockManager *BlockManager // Reference to the BlockManager
	blockID      uint32        // Current block ID in the iteration
	lastBlockID  uint32        // Last block ID in the file
	history      []uint32      // History of block IDs visited during iteration
}

// pwrite performs an atomic write at a specific offset without needing to Seek first
func pwrite(fd uintptr, data []byte, offset int64) (int, error) {
	return syscall.Pwrite(int(fd), data, offset)
}

// pread performs an atomic read from a specific offset without needing to Seek first
func pread(fd uintptr, data []byte, offset int64) (int, error) {
	return syscall.Pread(int(fd), data, offset)
}

// Open opens a file and initializes the BlockManager.
func Open(filename string, flag int, perm os.FileMode, syncOpt SyncOption, duration ...time.Duration) (*BlockManager, error) {
	file, err := os.OpenFile(filename, flag, perm)
	if err != nil {
		return nil, err
	}

	// Get the file descriptor for direct syscalls
	fd := file.Fd()

	// We get stats on the file and check if its empty
	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}

	allocationTable := stack.New()
	bm := &BlockManager{
		allocationTable: allocationTable,
		file:            file,
		fd:              fd,
		syncOption:      syncOpt,
		closeChan:       make(chan struct{}),
		wg:              &sync.WaitGroup{},
	}

	if len(duration) > 0 {
		bm.syncInterval = duration[0]
	} else {
		bm.syncInterval = 0
	}

	if stats.Size() == 0 {
		// If the file is empty, we need to write the header
		if err := bm.writeHeader(); err != nil {
			return nil, err
		}

		// After creating a new file, append initial free blocks
		if err := bm.appendFreeBlocks(); err != nil {
			return nil, err
		}
	} else {
		// If the file is not empty, we need to read the header and check if it matches
		if err := bm.readHeader(); err != nil {
			return nil, err
		}

		// Scan existing blocks to find free ones and add them to the allocation table
		if err := bm.scanForFreeBlocks(); err != nil {
			return nil, err
		}

		// If we don't have any free blocks, append new ones
		if bm.allocationTable.IsEmpty() {
			if err := bm.appendFreeBlocks(); err != nil {
				return nil, err
			}
		}
	}

	if bm.syncOption == SyncPartial {
		bm.wg.Add(1)
		go bm.backgroundSync()
	}

	return bm, nil
}

// writeHeader writes the header to the file.
func (bm *BlockManager) writeHeader() error {
	header := Header{
		MagicNumber: MagicNumber,
		Version:     Version,
		BlockSize:   BlockSize,
		Allotment:   Allotment,
	}

	// Calculate header size
	headerSize := binary.Size(header)
	if headerSize < 0 {
		return errors.New("failed to calculate header size")
	}

	// Create a buffer to hold the header
	buf := new(bytes.Buffer)

	// Write the header without CRC to the buffer
	header.CRC = 0
	if err := binary.Write(buf, binary.LittleEndian, &header); err != nil {
		return err
	}

	// Calculate CRC for the header
	header.CRC = crc32.ChecksumIEEE(buf.Bytes())

	// Reset the buffer and write the header with CRC
	buf.Reset()
	if err := binary.Write(buf, binary.LittleEndian, &header); err != nil {
		return err
	}

	// Use pwrite to write at the beginning of the file (offset 0)
	_, err := pwrite(bm.fd, buf.Bytes(), 0)
	return err
}

// readHeader reads the header from the file and validates it.
func (bm *BlockManager) readHeader() error {
	// Calculate header size
	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return errors.New("failed to calculate header size")
	}

	// Create a buffer to hold the header
	buf := make([]byte, headerSize)

	// Read the header using pread
	_, err := pread(bm.fd, buf, 0)
	if err != nil {
		return err
	}

	// Decode the header
	var header Header
	if err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &header); err != nil {
		return err
	}

	// Validate the CRC
	expectedCRC := header.CRC
	header.CRC = 0

	// Recalculate the CRC using the buffer with the CRC field set to 0
	bufWithoutCRC := new(bytes.Buffer)
	if err := binary.Write(bufWithoutCRC, binary.LittleEndian, &header); err != nil {
		return err
	}
	calculatedCRC := crc32.ChecksumIEEE(bufWithoutCRC.Bytes())

	if expectedCRC != calculatedCRC {
		return errors.New("header CRC mismatch")
	}

	// Validate the header fields
	if header.MagicNumber != MagicNumber {
		return errors.New("invalid magic number")
	}
	if header.Version != Version {
		return errors.New("unsupported version")
	}

	return nil
}

// backgroundSync performs periodic synchronization of the file to disk.
func (bm *BlockManager) backgroundSync() {
	defer bm.wg.Done()

	if bm.syncInterval == 0 && bm.syncOption == SyncNone {
		return // No background sync set
	}
	ticker := time.NewTicker(bm.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			syscall.Fdatasync(int(bm.fd)) // Use fdatasync for better performance

		case <-bm.closeChan:
			return
		}
	}
}

// appendFreeBlocks appends free blocks to the file.
func (bm *BlockManager) appendFreeBlocks() error {
	// Get current file size to determine next block ID
	fileInfo, err := bm.file.Stat()
	if err != nil {
		return err
	}

	fileSize := fileInfo.Size()
	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return errors.New("failed to calculate header size")
	}

	// Calculate how many blocks we currently have
	dataSize := fileSize - int64(headerSize)
	blockCount := uint64(dataSize / int64(BlockSize))

	// Create a buffer for a free block
	blockBuffer := make([]byte, BlockSize)

	// Append Allotment number of free blocks in reverse order
	for i := Allotment; i > 0; i-- {
		newBlockID := blockCount + i - 1

		// Block ID 0 is reserved, skip it
		if newBlockID == 0 {
			continue // Skip this iteration entirely
		}

		// Create a block header for a free block
		blockHeader := BlockHeader{
			BlockID:   newBlockID,
			DataSize:  0,          // No data for free blocks
			NextBlock: EndOfChain, // End of chain for free blocks
		}

		// Calculate and set the CRC
		headerBuf := new(bytes.Buffer)
		blockHeader.CRC = 0
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}
		blockHeader.CRC = crc32.ChecksumIEEE(headerBuf.Bytes())

		// Write the block header to the buffer
		headerBuf.Reset()
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}

		// Copy header bytes to the block buffer
		copy(blockBuffer, headerBuf.Bytes())

		// Zero out the rest of the block to prevent junk data
		for i := len(headerBuf.Bytes()); i < len(blockBuffer); i++ {
			blockBuffer[i] = 0
		}

		// Calculate position for the new block
		position := int64(headerSize) + int64(newBlockID)*int64(BlockSize)

		// Use pwrite to write the block at the calculated position
		_, err := pwrite(bm.fd, blockBuffer, position)
		if err != nil {
			return err
		}

		// Push the block ID to the allocation table
		bm.allocationTable.Push(newBlockID)
	}

	// Sync changes to disk
	if bm.syncOption == SyncFull {
		syscall.Fdatasync(int(bm.fd))
	}
	return nil
}

// allocateBlock allocates a block ID from the allocation table.
func (bm *BlockManager) allocateBlock() (uint64, error) {
	// Check if we have free blocks
	if bm.allocationTable.IsEmpty() {
		// If not, append more free blocks
		if err := bm.appendFreeBlocks(); err != nil {
			return 0, err
		}
	}

	// Pop a free block ID from the allocation table
	blockIDValue := bm.allocationTable.Pop()

	// Check if we got a valid value back
	if blockIDValue == nil {
		// If Pop returns nil, it means the stack was actually empty
		// even though IsEmpty() didn't report it (could be a race condition)
		// So try appending more blocks
		if err := bm.appendFreeBlocks(); err != nil {
			return 0, err
		}

		// Try popping again
		blockIDValue = bm.allocationTable.Pop()
		if blockIDValue == nil {
			return 0, errors.New("failed to allocate block: allocation table is empty")
		}
	}

	// Convert the interface{} value to uint32
	blockID, ok := blockIDValue.(uint64)
	if !ok {
		return 0, errors.New("failed to allocate block: invalid block ID type")
	}

	// Ensure we never return 0 as a valid block ID
	if blockID == 0 {
		// Try to allocate another block instead
		return bm.allocateBlock()
	}

	return blockID, nil
}

// scanForFreeBlocks scans the file for free blocks and populates the allocation table.
// scanForFreeBlocks scans the file for free blocks and populates the allocation table.
// It optimizes by first checking from the end of the file until it finds the first used block
// or end of chain block, since newly appended blocks are guaranteed to be free.
func (bm *BlockManager) scanForFreeBlocks() error {
	// Get file size
	fileInfo, err := bm.file.Stat()
	if err != nil {
		return err
	}

	fileSize := fileInfo.Size()
	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return errors.New("failed to calculate header size")
	}

	// Calculate how many blocks we have
	dataSize := fileSize - int64(headerSize)
	blockCount := uint64(dataSize / int64(BlockSize))

	// Reset allocation table
	bm.allocationTable = stack.New()

	blockHeaderSize := binary.Size(BlockHeader{})
	headerBuf := make([]byte, blockHeaderSize)

	// First pass: scan from the end until we find a used block or end of chain block
	var firstNonFreeBlockFromEnd uint64 = blockCount
	for i := blockCount - 1; i >= 1; i-- { // Start from the end, go backward to block ID 1
		// Calculate position for this block
		position := int64(headerSize) + int64(i)*int64(BlockSize)

		// Read the block header using pread
		_, err := pread(bm.fd, headerBuf, position)
		if err != nil {
			return err
		}

		// Decode the header
		var blockHeader BlockHeader
		if err := binary.Read(bytes.NewReader(headerBuf), binary.LittleEndian, &blockHeader); err != nil {
			return err
		}

		// Verify the CRC of the header
		expectedCRC := blockHeader.CRC
		blockHeader.CRC = 0
		headerWithoutCRC := new(bytes.Buffer)
		if err := binary.Write(headerWithoutCRC, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}
		calculatedCRC := crc32.ChecksumIEEE(headerWithoutCRC.Bytes())

		// If CRC is invalid, continue to the next block
		if expectedCRC != calculatedCRC {
			continue
		}

		// A block is considered non-free if:
		// 1. It has data (DataSize > 0)
		// 2. It's marked as an end of chain (NextBlock == EndOfChain and DataSize > 0)
		//    Note: Free blocks might also have NextBlock == EndOfChain but they have DataSize == 0
		if blockHeader.DataSize > 0 {
			firstNonFreeBlockFromEnd = i
			break
		}

		// Also check if this block is referenced by any other block
		// (We'll do this in a second pass)
	}

	// All blocks from firstNonFreeBlockFromEnd+1 to blockCount-1 are free
	// Add them directly to the allocation table (in reverse order to maintain LIFO ordering)
	for i := blockCount - 1; i > firstNonFreeBlockFromEnd; i-- {
		bm.allocationTable.Push(i)
	}

	// If all blocks after firstNonFreeBlockFromEnd are free, and firstNonFreeBlockFromEnd is 0,
	// we can return early without checking for chain blocks
	if firstNonFreeBlockFromEnd == 0 {
		return nil
	}

	// For the remaining blocks (1 to firstNonFreeBlockFromEnd), we need to do a more careful scan
	// to find used blocks and chain blocks
	usedBlocks := make(map[uint64]bool)
	chainBlocks := make(map[uint64]bool)

	// Scan blocks from 1 to firstNonFreeBlockFromEnd to identify used blocks and chain blocks
	for i := uint64(1); i <= firstNonFreeBlockFromEnd; i++ {
		// Calculate position for this block
		position := int64(headerSize) + int64(i)*int64(BlockSize)

		// Read the block header using pread
		_, err := pread(bm.fd, headerBuf, position)
		if err != nil {
			return err
		}

		// Decode the header
		var blockHeader BlockHeader
		if err := binary.Read(bytes.NewReader(headerBuf), binary.LittleEndian, &blockHeader); err != nil {
			return err
		}

		// Verify the CRC of the header
		expectedCRC := blockHeader.CRC
		blockHeader.CRC = 0
		headerWithoutCRC := new(bytes.Buffer)
		if err := binary.Write(headerWithoutCRC, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}
		calculatedCRC := crc32.ChecksumIEEE(headerWithoutCRC.Bytes())

		// Skip blocks with invalid CRC - they could be corrupted or uninitialized
		if expectedCRC != calculatedCRC {
			continue
		}

		// If block has data, mark it as used
		if blockHeader.DataSize > 0 {
			usedBlocks[i] = true

			// If it has a next block, add it to chain blocks
			if blockHeader.NextBlock != EndOfChain && blockHeader.NextBlock <= blockCount {
				chainBlocks[blockHeader.NextBlock] = true
			}
		}
	}

	// Add chain blocks to used blocks
	for blockID := range chainBlocks {
		if blockID <= firstNonFreeBlockFromEnd {
			usedBlocks[blockID] = true
		}
	}

	// Add all blocks from 1 to firstNonFreeBlockFromEnd that are not used to the allocation table
	// Add in reverse order to maintain LIFO ordering
	for i := firstNonFreeBlockFromEnd; i >= 1; i-- {
		if !usedBlocks[i] {
			bm.allocationTable.Push(i)
		}
	}

	return nil
}

// Close closes the file and releases any resources held by the BlockManager.
func (bm *BlockManager) Close() error {
	// Check if the channel is already closed
	select {
	case <-bm.closeChan:
		// Channel is already closed, do nothing
	default:
		// Close the channel to stop background sync
		close(bm.closeChan)
	}

	// Wait for the background sync goroutine to finish
	bm.wg.Wait()

	if bm.file != nil {
		return bm.file.Close()
	}

	return nil
}

// Append writes data to the file by allocating one or more blocks
// and returns the ID of the first block containing the data.
func (bm *BlockManager) Append(data []byte) (int64, error) {
	// Check if there's any data to append
	if len(data) == 0 {
		return -1, errors.New("no data to append")
	}

	// Calculate how many blocks we need to store the data
	blockHeaderSize := binary.Size(BlockHeader{})
	if blockHeaderSize < 0 {
		return -1, errors.New("failed to calculate block header size")
	}

	// Available space per block for data
	dataSpacePerBlock := int(BlockSize) - blockHeaderSize

	// Total blocks needed to store all data
	totalBlocks := (len(data) + dataSpacePerBlock - 1) / dataSpacePerBlock

	// Allocate the first block
	firstBlockID, err := bm.allocateBlock()
	if err != nil {
		return -1, err
	}

	currentBlockID := firstBlockID
	remainingData := data
	headerSize := binary.Size(Header{})

	// Loop until all data is written
	for blockNum := 0; blockNum < totalBlocks; blockNum++ {
		// Calculate position of the current block
		position := int64(headerSize) + int64(currentBlockID)*int64(BlockSize)

		// Determine how much data to write in this block
		dataToWrite := remainingData
		var nextBlockID uint64 = EndOfChain // Default to end of chain

		if len(dataToWrite) > dataSpacePerBlock {
			// We need another block
			dataToWrite = remainingData[:dataSpacePerBlock]
			remainingData = remainingData[dataSpacePerBlock:]

			// Allocate next block
			nextBlockID, err = bm.allocateBlock()
			if err != nil {
				return -1, err
			}
		} else {
			// All remaining data fits in this block
			remainingData = nil
		}

		// Create a block header
		blockHeader := BlockHeader{
			BlockID:   currentBlockID,
			DataSize:  uint64(len(dataToWrite)),
			NextBlock: nextBlockID,
		}

		// Calculate and set the CRC for the header
		headerBuf := new(bytes.Buffer)
		blockHeader.CRC = 0
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return -1, err
		}
		blockHeader.CRC = crc32.ChecksumIEEE(headerBuf.Bytes())

		// Create a buffer for the entire block
		blockBuffer := make([]byte, BlockSize)

		// Reset header buffer and write header with CRC
		headerBuf.Reset()
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return -1, err
		}

		// Copy header to block buffer
		copy(blockBuffer, headerBuf.Bytes())

		// Copy data to block buffer after the header
		copy(blockBuffer[blockHeaderSize:], dataToWrite)

		// Zero out the rest of the buffer to prevent junk data
		for i := blockHeaderSize + len(dataToWrite); i < len(blockBuffer); i++ {
			blockBuffer[i] = 0
		}

		// Write the block to the file using atomic pwrite operation
		_, err := pwrite(bm.fd, blockBuffer, position)
		if err != nil {
			return -1, err
		}

		// Update current block ID for the next iteration
		currentBlockID = nextBlockID
	}

	if bm.syncOption == SyncFull {
		// Sync changes to disk
		syscall.Fdatasync(int(bm.fd))
	}

	return int64(firstBlockID), nil
}

// Read reads data from the file starting at the specified block ID.
// It follows the chain of blocks if the data spans multiple blocks.
// Returns the data, the final block ID (if chained) or the first block ID (if not chained), and any error.
func (bm *BlockManager) Read(blockID int64) ([]byte, int64, error) {
	if blockID <= 0 {
		return nil, -1, errors.New("invalid block ID")
	}

	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return nil, -1, errors.New("failed to calculate header size")
	}

	blockHeaderSize := binary.Size(BlockHeader{})
	if blockHeaderSize < 0 {
		return nil, -1, errors.New("failed to calculate block header size")
	}

	// Buffer to hold the result
	var resultBuffer bytes.Buffer
	currentBlockID := uint64(blockID)
	initialBlockID := currentBlockID
	lastBlockID := currentBlockID
	isMultiBlock := false

	// Buffer for reading a block
	blockBuffer := make([]byte, BlockSize)

	// Loop until we've read all blocks in the chain
	for currentBlockID != EndOfChain {
		// Calculate the position of the current block
		position := int64(headerSize) + int64(currentBlockID)*int64(BlockSize)

		// Read the block using atomic pread operation
		bytesRead, err := pread(bm.fd, blockBuffer, position)
		if err != nil {
			return nil, -1, err
		}

		if bytesRead != int(BlockSize) {
			return nil, -1, errors.New("incomplete block read")
		}

		// Decode the block header
		var blockHeader BlockHeader
		if err := binary.Read(bytes.NewReader(blockBuffer[:blockHeaderSize]), binary.LittleEndian, &blockHeader); err != nil {
			return nil, -1, err
		}

		// Verify the CRC
		expectedCRC := blockHeader.CRC
		blockHeader.CRC = 0

		// Recalculate the CRC
		headerBuf := new(bytes.Buffer)
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return nil, -1, err
		}
		calculatedCRC := crc32.ChecksumIEEE(headerBuf.Bytes())

		if expectedCRC != calculatedCRC {
			return nil, -1, errors.New("block header CRC mismatch")
		}

		// Verify that this is the block we expect
		if blockHeader.BlockID != currentBlockID {
			return nil, -1, errors.New("block ID mismatch")
		}

		// Check if the block has any data
		if blockHeader.DataSize == 0 {
			return nil, -1, errors.New("block contains no data")
		}

		// Extract the data from the block
		dataStart := blockHeaderSize
		dataEnd := dataStart + int(blockHeader.DataSize)

		if dataEnd > int(BlockSize) {
			return nil, -1, errors.New("data size exceeds block size")
		}

		// Append the data to the result buffer
		resultBuffer.Write(blockBuffer[dataStart:dataEnd])

		// Save the current block ID before moving to the next
		lastBlockID = currentBlockID

		// If we're moving to a next block, set the multi-block flag
		if blockHeader.NextBlock != EndOfChain {
			isMultiBlock = true
		}

		// Move to the next block if there is one
		currentBlockID = blockHeader.NextBlock
	}

	// If the data spanned multiple blocks, return the last block ID
	// Otherwise, return the first (and only) block ID
	returnBlockID := int64(initialBlockID)
	if isMultiBlock {
		returnBlockID = int64(lastBlockID)
	}

	// Return the collected data and the relevant block ID
	return resultBuffer.Bytes(), returnBlockID, nil
}

// Iterator returns an iterator for traversing the blocks in the file
func (bm *BlockManager) Iterator() *Iterator {
	// Get current last block based on file size
	fileInfo, err := bm.file.Stat()
	if err != nil {
		return nil
	}

	fileSize := fileInfo.Size()
	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return nil
	}

	// Calculate how many blocks we have
	dataSize := fileSize - int64(headerSize)
	blockCount := uint32(dataSize) / BlockSize

	return &Iterator{
		blockManager: bm,
		blockID:      1,
		lastBlockID:  blockCount,
		history:      []uint32{},
	}
}

// Next moves the iterator to the next block
func (it *Iterator) Next() ([]byte, int64, error) {
	if it.blockID > it.lastBlockID {
		return nil, -1, errors.New("no more blocks to iterate")
	}

	if it.blockID == 0 {
		return nil, -1, errors.New("no more blocks to iterate")
	}

	data, blockID, err := it.blockManager.Read(int64(it.blockID))
	if err != nil {
		return nil, -1, err
	}

	it.history = append(it.history, it.blockID)
	it.blockID = uint32(blockID) + 1

	return data, int64(blockID), nil
}

// Prev moves the iterator to the previous block
func (it *Iterator) Prev() ([]byte, int64, error) {
	if len(it.history) == 0 {
		return nil, -1, errors.New("no previous blocks to iterate")
	}

	// Pop the last block ID from the history
	it.blockID = it.history[len(it.history)-1]
	it.history = it.history[:len(it.history)-1]

	// Read the block data
	data, blockID, err := it.blockManager.Read(int64(it.blockID))
	if err != nil {
		return nil, -1, err
	}

	return data, int64(blockID), nil
}

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
	"errors"
	"github.com/wildcatdb/wildcat/queue"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const MagicNumber = uint32(0x57494C44)        // "WILD"
const Version = uint32(1)                     // Version of the file format
const BlockSize = uint32(512)                 // Smaller the better, faster in our tests
const Allotment = uint64(16)                  // How many blocks we can allot at once to the file.  We allocate this many blocks once allocationTable is empty
const EndOfChain = uint64(0xFFFFFFFFFFFFFFFF) // Marker for end of blockchain (overflowed block)

// SyncOption defines the synchronization options for the file
type SyncOption int

const (
	SyncNone    SyncOption = iota // Don't sync at all
	SyncFull                      // Do a sync after every write
	SyncPartial                   // Do a sync in the background at intervals
)

// Why use pread and pwrite? https://stackoverflow.com/questions/7592822/what-are-the-advantages-of-pwrite-and-pread-over-fwrite-and-fread

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
	BlockID   uint64 // Unique ID of the block
	DataSize  uint64 // Size of the data in the block
	NextBlock uint64 // ID of the next block in the chain (or EndOfChain if this is the last block)
}

// BlockManager manages the allocation and deallocation of blocks in a file
type BlockManager struct {
	allocationTable *queue.Queue    // An atomic queue we store free available block ids
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
	prevBlockID  uint64        // Previous block ID in the iteration
	blockID      uint64        // Current block ID in the iteration
	lastBlockID  uint64        // Last block ID in the file
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

	allocationTable := queue.New()
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
		if err = bm.writeHeader(); err != nil {
			return nil, err
		}

		// After creating a new file, append initial free blocks
		if err = bm.appendFreeBlocks(); err != nil {
			return nil, err
		}
	} else {
		// If the file is not empty, we need to read the header and check if it matches
		if err = bm.readHeader(); err != nil {
			return nil, err
		}

		// Scan existing blocks to find free ones and add them to the allocation table
		if err = bm.scanForFreeBlocks(); err != nil {
			return nil, err
		}

		// If we don't have any free blocks, append new ones
		if bm.allocationTable.IsEmpty() {
			if err = bm.appendFreeBlocks(); err != nil {
				return nil, err
			}
		}
	}

	// If syncOption is SyncPartial, start the background sync goroutine
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
			_ = Fdatasync(bm.fd)

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

	// Append Allotment number of free blocks
	for i := uint64(0); i < Allotment; i++ {
		newBlockID := blockCount + (i + 1) // New block ID is the current count + 1

		// Block ID 0 is reserved, skip it
		if newBlockID == 0 {
			continue // Skip this iteration entirely
		}

		// Create a block header for a free block
		blockHeader := BlockHeader{
			BlockID:   newBlockID, // Unique ID of the block
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
		for j := len(headerBuf.Bytes()); j < len(blockBuffer); j++ {
			blockBuffer[j] = 0
		}

		// Calculate position for the new block
		position := int64(headerSize) + int64(newBlockID)*int64(BlockSize)

		// Use pwrite to write the block at the calculated position
		_, err := pwrite(bm.fd, blockBuffer, position)
		if err != nil {
			return err
		}

		// Push the block ID to the allocation table
		bm.allocationTable.Enqueue(newBlockID)
	}

	// Sync changes to disk
	if bm.syncOption == SyncFull {
		_ = syscall.Fdatasync(int(bm.fd))
	}
	return nil
}

// allocateBlock allocates a block ID from the allocation table.
func (bm *BlockManager) allocateBlock() (uint64, error) {
	// First, check if we have free blocks atomically
	if bm.allocationTable.IsEmpty() {
		// We need to append blocks, but we need to ensure only one goroutine does this
		// We'll use atomic CAS operations for this

		// We'll use a sync.Once pattern but with atomic operations
		// This ensures multiple goroutines won't all try to append blocks simultaneously

		// Create a flag to track if we've started appending blocks
		appendingFlag := int32(0)

		// Try to set the flag from 0 to 1
		if atomic.CompareAndSwapInt32(&appendingFlag, 0, 1) {
			// We successfully set the flag, so we're the one to append blocks
			if err := bm.appendFreeBlocks(); err != nil {
				// Reset the flag and return the error
				atomic.StoreInt32(&appendingFlag, 0)
				return 0, err
			}
			// Reset the flag
			atomic.StoreInt32(&appendingFlag, 0)
		} else {
			// Someone else is appending blocks, let's wait a tiny bit
			// This is better than spinning aggressively
			time.Sleep(time.Microsecond)
		}
	}

	// Now try to get a block atomically
	// Loop until we either get a block or confirm the queue is truly empty
	for {
		blockIDValue := bm.allocationTable.Dequeue()

		if blockIDValue == nil {
			// Queue might be empty, but we need to make sure it's not just a race condition
			// Check again and maybe append more blocks
			if bm.allocationTable.IsEmpty() {
				// If it's still empty, try to append more blocks
				appendingFlag := int32(0)
				if atomic.CompareAndSwapInt32(&appendingFlag, 0, 1) {
					if err := bm.appendFreeBlocks(); err != nil {
						atomic.StoreInt32(&appendingFlag, 0)
						return 0, err
					}
					atomic.StoreInt32(&appendingFlag, 0)
				} else {
					// Someone else is appending blocks, wait a bit
					time.Sleep(time.Microsecond)
				}

				// Try again to get a block
				continue
			}

			// The queue wasn't actually empty, just try again
			continue
		}

		// Convert the interface{} value to uint64
		blockID, ok := blockIDValue.(uint64)
		if !ok {
			return 0, errors.New("failed to allocate block: invalid block ID type")
		}

		// Ensure we never return 0 as a valid block ID
		if blockID == 0 {
			// Try to allocate another block instead
			continue
		}

		return blockID, nil
	}
}

// scanForFreeBlocks scans the file for free blocks and populates the allocation table.
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
	bm.allocationTable = queue.New()

	blockHeaderSize := binary.Size(BlockHeader{})
	headerBuf := make([]byte, blockHeaderSize)

	// First pass we scan from the end until we find a used block or end of chain block
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

		// A block is considered non-free if
		// 1. It has data (DataSize > 0)
		// 2. It's marked as an end of chain (NextBlock == EndOfChain and DataSize > 0)
		// Also free blocks might also have NextBlock == EndOfChain but they have DataSize == 0
		if blockHeader.DataSize > 0 {
			firstNonFreeBlockFromEnd = i
			break
		}

		// Also check if this block is referenced by any other block
		// (We'll do this in a second pass)
	}

	// All blocks from firstNonFreeBlockFromEnd+1 to blockCount-1 are free
	// Add them directly to the allocation table in ASCENDING order to prioritize lower block IDs
	for i := firstNonFreeBlockFromEnd + 1; i <= blockCount-1; i++ {
		bm.allocationTable.Enqueue(i)
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
	// Add in ascending order to prioritize lower block IDs
	for i := uint64(1); i <= firstNonFreeBlockFromEnd; i++ {
		if !usedBlocks[i] {
			bm.allocationTable.Enqueue(i)
		}
	}

	return nil
}

// Close closes the file and releases any resources held by the BlockManager.
func (bm *BlockManager) Close() error {

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

	// Pre-allocate all blocks we'll need to maintain atomicity
	blockChain := make([]uint64, totalBlocks)

	// Allocate first block
	firstBlockID, err := bm.allocateBlock()
	if err != nil {
		return -1, err
	}
	blockChain[0] = firstBlockID

	// Allocate remaining blocks if needed
	for i := 1; i < totalBlocks; i++ {
		blockID, err := bm.allocateBlock()
		if err != nil {
			// If allocation fails, we have a partial chain
			// In a production system, you might want to free these blocks
			// But for now we'll just return the error
			return -1, err
		}
		blockChain[i] = blockID
	}

	// Now we have all blocks allocated, we can write data without allocation races
	remainingData := data
	headerSize := binary.Size(Header{})

	// Write data to each allocated block
	for i := 0; i < totalBlocks; i++ {
		currentBlockID := blockChain[i]
		position := int64(headerSize) + int64(currentBlockID)*int64(BlockSize)

		// Determine how much data to write in this block
		var dataToWrite []byte
		var nextBlockID uint64 = EndOfChain // Default to end of chain

		if len(remainingData) > dataSpacePerBlock {
			// We need another block
			dataToWrite = remainingData[:dataSpacePerBlock]
			remainingData = remainingData[dataSpacePerBlock:]

			// If there's a next block in our chain, use its ID
			if i < totalBlocks-1 {
				nextBlockID = blockChain[i+1]
			}
		} else {
			// All remaining data fits in this block
			dataToWrite = remainingData
			remainingData = nil
		}

		// Create and write the block
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

		copy(blockBuffer, headerBuf.Bytes())
		copy(blockBuffer[blockHeaderSize:], dataToWrite)

		// Zero out the rest of the buffer to prevent junk data
		for j := blockHeaderSize + len(dataToWrite); j < len(blockBuffer); j++ {
			blockBuffer[j] = 0
		}

		// Write the block to the file using atomic pwrite operation
		_, err := pwrite(bm.fd, blockBuffer, position)
		if err != nil {
			return -1, err
		}
	}

	if bm.syncOption == SyncFull {
		_ = syscall.Fdatasync(int(bm.fd))
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

	var resultBuffer bytes.Buffer
	currentBlockID := uint64(blockID)
	initialBlockID := currentBlockID
	lastBlockID := currentBlockID
	isMultiBlock := false

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

		if blockHeader.BlockID != currentBlockID {
			return nil, -1, errors.New("block ID mismatch")
		}

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

	return resultBuffer.Bytes(), returnBlockID, nil
}

// File returns the associated file handle for the BlockManager
func (bm *BlockManager) File() *os.File {
	return bm.file
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
	blockCount := uint64(dataSize) / uint64(BlockSize)

	return &Iterator{
		blockManager: bm,
		blockID:      1,
		prevBlockID:  1,
		lastBlockID:  blockCount,
	}
}

// IteratorFromBlock returns an iterator that starts at the specified block ID
func (bm *BlockManager) IteratorFromBlock(startBlockID uint64) *Iterator {

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
	blockCount := uint64(dataSize) / uint64(BlockSize)

	// Validate the starting block ID
	if startBlockID < 1 || startBlockID > blockCount {
		return nil // Invalid starting block ID
	}

	return &Iterator{
		blockManager: bm,
		blockID:      startBlockID,
		prevBlockID:  startBlockID,
		lastBlockID:  blockCount,
	}
}

// Next moves the iterator to the next block
func (it *Iterator) Next() ([]byte, int64, error) {
	// Start from the current blockID and find the next block with data
	for it.blockID <= it.lastBlockID {
		currentBlockID := it.blockID

		// Try to read the current block
		data, returnedBlockID, err := it.blockManager.Read(int64(currentBlockID))
		if err != nil {
			// If this block has an error, skip to the next block
			it.blockID++
			continue
		}

		// Successfully read data from this block
		it.prevBlockID = currentBlockID

		// For multi-block data, the returnedBlockID is the last block in the chain
		// So the next block to check would be returnedBlockID + 1
		it.blockID = uint64(returnedBlockID) + 1

		return data, returnedBlockID, nil
	}

	// No more blocks with data found
	return nil, -1, errors.New("no more blocks to iterate")
}

// Prev moves the iterator to the previous block
func (it *Iterator) Prev() ([]byte, int64, error) {
	// If we're at block 1 or less, we can't go back further
	if it.prevBlockID <= 1 {
		return nil, -1, errors.New("no previous blocks to iterate")
	}

	// Start searching backwards from the previous block
	searchBlockID := it.prevBlockID - 1

	// Search backwards for the previous block that contains data
	for searchBlockID >= 1 {
		// First, check if this block has valid data
		if !it.isValidBlock(searchBlockID) {
			searchBlockID--
			continue
		}

		// Find the head of the chain for this block
		headBlockID, err := it.findChainHead(searchBlockID)
		if err != nil {
			searchBlockID--
			continue
		}

		// Try to read from the head block
		data, returnedBlockID, err := it.blockManager.Read(int64(headBlockID))
		if err != nil {
			searchBlockID--
			continue
		}

		// Successfully found previous block with data
		// Update iterator state
		it.blockID = it.prevBlockID
		it.prevBlockID = headBlockID

		return data, returnedBlockID, nil
	}

	// No previous blocks with data found
	return nil, -1, errors.New("no previous blocks to iterate")
}

// isValidBlock checks if a block exists and has valid data
func (it *Iterator) isValidBlock(blockID uint64) bool {
	if blockID == 0 || blockID > it.lastBlockID {
		return false
	}

	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return false
	}

	blockHeaderSize := binary.Size(BlockHeader{})
	if blockHeaderSize < 0 {
		return false
	}

	// Calculate position for this block
	position := int64(headerSize) + int64(blockID)*int64(BlockSize)

	// Read block header
	headerBuf := make([]byte, blockHeaderSize)
	_, err := pread(it.blockManager.fd, headerBuf, position)
	if err != nil {
		return false
	}

	// Decode the header
	var blockHeader BlockHeader
	if err := binary.Read(bytes.NewReader(headerBuf), binary.LittleEndian, &blockHeader); err != nil {
		return false
	}

	// Verify the CRC
	expectedCRC := blockHeader.CRC
	blockHeader.CRC = 0
	headerBuf2 := new(bytes.Buffer)
	if err := binary.Write(headerBuf2, binary.LittleEndian, &blockHeader); err != nil {
		return false
	}
	calculatedCRC := crc32.ChecksumIEEE(headerBuf2.Bytes())

	if expectedCRC != calculatedCRC {
		return false
	}

	// Block is valid if it has data
	return blockHeader.DataSize > 0
}

// findChainHead finds the head block of a chain that the given block belongs to
func (it *Iterator) findChainHead(blockID uint64) (uint64, error) {
	// Check if any block points to our target block
	// We need to scan backwards to find a block that chains to our target

	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return 0, errors.New("failed to calculate header size")
	}

	blockHeaderSize := binary.Size(BlockHeader{})
	if blockHeaderSize < 0 {
		return 0, errors.New("failed to calculate block header size")
	}

	// Start from block 1 and scan forward to find if any block chains to our target
	for candidateHead := uint64(1); candidateHead < blockID; candidateHead++ {
		if !it.isValidBlock(candidateHead) {
			continue
		}

		// Check if this candidate block chains to our target block
		if it.chainsToTarget(candidateHead, blockID) {
			return candidateHead, nil
		}
	}

	// If no block chains to our target, then our target is itself a head
	return blockID, nil
}

// chainsToTarget checks if startBlock eventually chains to targetBlock
func (it *Iterator) chainsToTarget(startBlock, targetBlock uint64) bool {
	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return false
	}

	blockHeaderSize := binary.Size(BlockHeader{})
	if blockHeaderSize < 0 {
		return false
	}

	currentBlock := startBlock
	visited := make(map[uint64]bool) // Prevent infinite loops**

	// Follow the chain from startBlock
	for currentBlock != EndOfChain && currentBlock != 0 {
		// Prevent infinite loops
		if visited[currentBlock] {
			return false
		}
		visited[currentBlock] = true

		// If we've reached our target, we found it
		if currentBlock == targetBlock {
			return true
		}

		// Read the current block header to get the next block
		position := int64(headerSize) + int64(currentBlock)*int64(BlockSize)
		headerBuf := make([]byte, blockHeaderSize)
		_, err := pread(it.blockManager.fd, headerBuf, position)
		if err != nil {
			return false
		}

		var blockHeader BlockHeader
		if err := binary.Read(bytes.NewReader(headerBuf), binary.LittleEndian, &blockHeader); err != nil {
			return false
		}

		// Verify CRC
		expectedCRC := blockHeader.CRC
		blockHeader.CRC = 0
		headerBuf2 := new(bytes.Buffer)
		if err := binary.Write(headerBuf2, binary.LittleEndian, &blockHeader); err != nil {
			return false
		}
		calculatedCRC := crc32.ChecksumIEEE(headerBuf2.Bytes())

		if expectedCRC != calculatedCRC {
			return false
		}

		// Move to next block in chain
		currentBlock = blockHeader.NextBlock
	}

	return false
}

// BlockManager returns the block manager pointer from iterator
func (it *Iterator) BlockManager() *BlockManager {
	return it.blockManager
}

// Update modifies the data at the specified block ID in place.
// If the new data is larger than the existing data, it will extend the chain.
// If the new data is smaller, it will free unused blocks.
// Returns the ID of the first block or an error.
func (bm *BlockManager) Update(blockID int64, newData []byte) (int64, error) {
	if blockID <= 0 {
		return -1, errors.New("invalid block ID")
	}

	if len(newData) == 0 {
		return -1, errors.New("no data to update")
	}

	// First, verify the block exists and is readable
	_, _, err := bm.Read(blockID)
	if err != nil {
		return -1, err
	}

	// Calculate space requirements
	blockHeaderSize := binary.Size(BlockHeader{})
	if blockHeaderSize < 0 {
		return -1, errors.New("failed to calculate block header size")
	}

	dataSpacePerBlock := int(BlockSize) - blockHeaderSize

	// Calculate blocks needed for new data
	newBlocksNeeded := (len(newData) + dataSpacePerBlock - 1) / dataSpacePerBlock

	// Find existing chain length and collect existing block IDs
	existingBlocks, err := bm.getBlockChain(uint64(blockID))
	if err != nil {
		return -1, err
	}

	existingBlocksCount := len(existingBlocks)

	headerSize := binary.Size(Header{})
	if headerSize < 0 {
		return -1, errors.New("failed to calculate header size")
	}

	// Case 1: New data fits in existing blocks (same size or smaller)
	if newBlocksNeeded <= existingBlocksCount {
		err := bm.updateExistingBlocks(existingBlocks, newData, newBlocksNeeded)
		if err != nil {
			return -1, err
		}

		// If we're using fewer blocks, free the unused ones
		if newBlocksNeeded < existingBlocksCount {
			err := bm.freeUnusedBlocks(existingBlocks[newBlocksNeeded:])
			if err != nil {
				return -1, err
			}
		}

		return blockID, nil
	}

	// Case 2: New data requires more blocks (extension)
	additionalBlocksNeeded := newBlocksNeeded - existingBlocksCount

	// Allocate additional blocks
	additionalBlocks := make([]uint64, additionalBlocksNeeded)
	for i := 0; i < additionalBlocksNeeded; i++ {
		newBlockID, err := bm.allocateBlock()
		if err != nil {

			// If allocation fails, free any blocks we've already allocated
			for j := 0; j < i; j++ {
				bm.allocationTable.Enqueue(additionalBlocks[j])
			}
			return -1, err
		}
		additionalBlocks[i] = newBlockID
	}

	// Combine existing and new blocks
	allBlocks := append(existingBlocks, additionalBlocks...)

	// Update all blocks with new data
	err = bm.updateExistingBlocks(allBlocks, newData, newBlocksNeeded)
	if err != nil {

		// If update fails, free the additional blocks we allocated
		for _, bid := range additionalBlocks {
			bm.allocationTable.Enqueue(bid)
		}
		return -1, err
	}

	return blockID, nil
}

// getBlockChain returns all block IDs in the chain starting from the given block
func (bm *BlockManager) getBlockChain(startBlockID uint64) ([]uint64, error) {
	var chainBlocks []uint64
	currentBlockID := startBlockID

	headerSize := binary.Size(Header{})
	blockHeaderSize := binary.Size(BlockHeader{})

	visited := make(map[uint64]bool) // Prevent infinite loops

	for currentBlockID != EndOfChain {

		// Prevent infinite loops
		if visited[currentBlockID] {
			return nil, errors.New("circular reference detected in block chain")
		}
		visited[currentBlockID] = true

		chainBlocks = append(chainBlocks, currentBlockID)

		// Read block header to get next block
		position := int64(headerSize) + int64(currentBlockID)*int64(BlockSize)
		headerBuf := make([]byte, blockHeaderSize)

		_, err := pread(bm.fd, headerBuf, position)
		if err != nil {
			return nil, err
		}

		var blockHeader BlockHeader
		if err := binary.Read(bytes.NewReader(headerBuf), binary.LittleEndian, &blockHeader); err != nil {
			return nil, err
		}

		// Verify CRC
		expectedCRC := blockHeader.CRC
		blockHeader.CRC = 0
		headerBuf2 := new(bytes.Buffer)
		if err := binary.Write(headerBuf2, binary.LittleEndian, &blockHeader); err != nil {
			return nil, err
		}
		calculatedCRC := crc32.ChecksumIEEE(headerBuf2.Bytes())

		if expectedCRC != calculatedCRC {
			return nil, errors.New("block header CRC mismatch")
		}

		currentBlockID = blockHeader.NextBlock
	}

	return chainBlocks, nil
}

// updateExistingBlocks writes new data to the specified blocks
func (bm *BlockManager) updateExistingBlocks(blockIDs []uint64, data []byte, blocksToUse int) error {

	blockHeaderSize := binary.Size(BlockHeader{})
	dataSpacePerBlock := int(BlockSize) - blockHeaderSize
	headerSize := binary.Size(Header{})

	remainingData := data

	for i := 0; i < blocksToUse; i++ {
		currentBlockID := blockIDs[i]
		position := int64(headerSize) + int64(currentBlockID)*int64(BlockSize)

		// Determine data for this block
		var dataToWrite []byte
		var nextBlockID uint64 = EndOfChain

		if len(remainingData) > dataSpacePerBlock {
			dataToWrite = remainingData[:dataSpacePerBlock]
			remainingData = remainingData[dataSpacePerBlock:]

			// Set next block if we have more blocks to write
			if i < blocksToUse-1 {
				nextBlockID = blockIDs[i+1]
			}
		} else {
			dataToWrite = remainingData
			remainingData = nil
		}

		// Create block header
		blockHeader := BlockHeader{
			BlockID:   currentBlockID,
			DataSize:  uint64(len(dataToWrite)),
			NextBlock: nextBlockID,
		}

		// Calculate CRC
		headerBuf := new(bytes.Buffer)
		blockHeader.CRC = 0
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}
		blockHeader.CRC = crc32.ChecksumIEEE(headerBuf.Bytes())

		// Create block buffer
		blockBuffer := make([]byte, BlockSize)

		// Write header with CRC
		headerBuf.Reset()
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}

		copy(blockBuffer, headerBuf.Bytes())
		copy(blockBuffer[blockHeaderSize:], dataToWrite)

		// Zero out remaining space
		for j := blockHeaderSize + len(dataToWrite); j < len(blockBuffer); j++ {
			blockBuffer[j] = 0
		}

		// Write block to file
		_, err := pwrite(bm.fd, blockBuffer, position)
		if err != nil {
			return err
		}
	}

	// Sync if needed
	if bm.syncOption == SyncFull {
		_ = syscall.Fdatasync(int(bm.fd))
	}

	return nil
}

// freeUnusedBlocks marks the specified blocks as free and adds them back to allocation table
func (bm *BlockManager) freeUnusedBlocks(blockIDs []uint64) error {
	headerSize := binary.Size(Header{})

	for _, blockID := range blockIDs {

		// Create a free block header
		blockHeader := BlockHeader{
			BlockID:   blockID,
			DataSize:  0,          // No data for free blocks
			NextBlock: EndOfChain, // End of chain for free blocks
		}

		// Calculate CRC
		headerBuf := new(bytes.Buffer)
		blockHeader.CRC = 0
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}
		blockHeader.CRC = crc32.ChecksumIEEE(headerBuf.Bytes())

		// Create block buffer
		blockBuffer := make([]byte, BlockSize)

		// Write header with CRC
		headerBuf.Reset()
		if err := binary.Write(headerBuf, binary.LittleEndian, &blockHeader); err != nil {
			return err
		}

		copy(blockBuffer, headerBuf.Bytes())

		// Zero out the rest
		for j := len(headerBuf.Bytes()); j < len(blockBuffer); j++ {
			blockBuffer[j] = 0
		}

		// Write to file
		position := int64(headerSize) + int64(blockID)*int64(BlockSize)
		_, err := pwrite(bm.fd, blockBuffer, position)
		if err != nil {
			return err
		}

		// Add back to allocation table
		bm.allocationTable.Enqueue(blockID)
	}

	// Sync if needed
	if bm.syncOption == SyncFull {
		_ = syscall.Fdatasync(int(bm.fd))
	}

	return nil
}

// Sync escalates the Fdatasync operation to ensure data integrity.  Only allowed when syncOption is SyncNone.
func (bm *BlockManager) Sync() error {
	if bm.syncOption != SyncNone {
		return errors.New("escalate fsync is only allowed when syncOption is SyncNone")
	}

	return syscall.Fdatasync(int(bm.fd))
}

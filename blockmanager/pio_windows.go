//go:build windows
// +build windows

package blockmanager

import (
	"syscall"
	"unsafe"
)

var (
	kernel32           = syscall.NewLazyDLL("kernel32.dll")
	procSetFilePointer = kernel32.NewProc("SetFilePointer")
	procReadFile       = kernel32.NewProc("ReadFile")
	procWriteFile      = kernel32.NewProc("WriteFile")
)

// pwrite performs an atomic write at a specific offset without needing to Seek first
func pwrite(fd uintptr, data []byte, offset int64) (int, error) {
	// Use SetFilePointer + WriteFile as Windows doesn't have native pwrite
	handle := syscall.Handle(fd)

	// Set file pointer to desired offset
	low := uint32(offset)
	high := uint32(offset >> 32)

	_, _, err := procSetFilePointer.Call(
		uintptr(handle),
		uintptr(low),
		uintptr(unsafe.Pointer(&high)),
		0, // FILE_BEGIN
	)
	if err != nil && err.(syscall.Errno) != 0 {
		return 0, err
	}

	// Perform the write
	var bytesWritten uint32
	err2 := syscall.WriteFile(handle, data, &bytesWritten, nil)
	return int(bytesWritten), err2
}

// pread performs an atomic read from a specific offset without needing to Seek first
func pread(fd uintptr, data []byte, offset int64) (int, error) {
	handle := syscall.Handle(fd)

	// Set file pointer to desired offset
	low := uint32(offset)
	high := uint32(offset >> 32)

	_, _, err := procSetFilePointer.Call(
		uintptr(handle),
		uintptr(low),
		uintptr(unsafe.Pointer(&high)),
		0, // FILE_BEGIN
	)
	if err != nil && err.(syscall.Errno) != 0 {
		return 0, err
	}

	// Perform the read
	var bytesRead uint32
	err2 := syscall.ReadFile(handle, data, &bytesRead, nil)
	return int(bytesRead), err2
}

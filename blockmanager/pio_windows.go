//go:build windows
// +build windows

package blockmanager

import "syscall"

// pwrite performs an atomic write at a specific offset without needing to Seek first
func pwrite(fd uintptr, data []byte, offset int64) (int, error) {
	var overlapped syscall.Overlapped
	overlapped.OffsetHigh = uint32(offset >> 32)
	overlapped.Offset = uint32(offset)

	var bytesWritten uint32
	err := syscall.WriteFile(syscall.Handle(fd), data, &bytesWritten, &overlapped)
	return int(bytesWritten), err
}

// pread performs an atomic read from a specific offset without needing to Seek first
func pread(fd uintptr, data []byte, offset int64) (int, error) {
	var overlapped syscall.Overlapped
	overlapped.OffsetHigh = uint32(offset >> 32)
	overlapped.Offset = uint32(offset)

	var bytesRead uint32
	err := syscall.ReadFile(syscall.Handle(fd), data, &bytesRead, &overlapped)
	return int(bytesRead), err
}

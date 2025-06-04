//go:build windows
// +build windows

package blockmanager

import (
	"syscall"
)

const (
	FLUSH_FLAGS_FILE_DATA_SYNC_ONLY = 0x00000004
)

var (
	modntdll                 = syscall.NewLazyDLL("ntdll.dll")
	procNtFlushBuffersFileEx = modntdll.NewProc("NtFlushBuffersFileEx")
)

// Fdatasync is a Windows-specific implementation of fdatasync.
// https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntflushbuffersfileex
func Fdatasync(fd uintptr) error {
	status, _, _ := procNtFlushBuffersFileEx.Call(
		fd,
		FLUSH_FLAGS_FILE_DATA_SYNC_ONLY,
		0,
		0,
		0,
	)

	if status != 0 {
		// Fall back to regular FlushFileBuffers if NtFlushBuffersFileEx fails or isn't available
		return syscall.FlushFileBuffers(syscall.Handle(fd))
	}

	return nil
}

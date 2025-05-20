//go:build windows
// +build windows

package blockmanager

var (
	modntdll                 = syscall.NewLazyDLL("ntdll.dll")
	procNtFlushBuffersFileEx = modntdll.NewProc("NtFlushBuffersFileEx")
)

// Fdatasync__Win is a Windows-specific implementation of fdatasync.
// https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntflushbuffersfileex
func Fdatasync(fd uintptr) error {

	// Try to use NtFlushBuffersFileEx with FLUSH_FLAGS_FILE_DATA_SYNC_ONLY flag
	status, _, err := procNtFlushBuffersFileEx.Call(
		fd,
		FLUSH_FLAGS_FILE_DATA_SYNC_ONLY,
		0,
		0,
		0,
	)

	// Check for error (0 means success in Windows API)
	if status != 0 {
		// Fall back to regular FlushFileBuffers if NtFlushBuffersFileEx fails or isn't available
		return syscall.FlushFileBuffers(syscall.Handle(fd))
	}

	return nil
}

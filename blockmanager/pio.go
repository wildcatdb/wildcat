//go:build darwin || linux || freebsd || netbsd || openbsd

package blockmanager

import (
	"os"
	"syscall"
)

// pwrite performs an atomic write at a specific offset without needing to Seek first
func pwrite(fd uintptr, data []byte, offset int64, f *os.File) (int, error) {
	return syscall.Pwrite(int(fd), data, offset)
}

// pread performs an atomic read from a specific offset without needing to Seek first
func pread(fd uintptr, data []byte, offset int64, f *os.File) (int, error) {
	return syscall.Pread(int(fd), data, offset)
}

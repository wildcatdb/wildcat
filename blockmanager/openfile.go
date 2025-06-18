//go:build darwin || linux || freebsd || netbsd || openbsd

package blockmanager

import (
	"os"
	"syscall"
)

// OpenFile opens a file with the specified name and flags, returning a file handle.
func OpenFile(name string, flags int, perm uint32) (uintptr, error) {
	fd, err := syscall.Open(name, flags, perm)
	if err != nil {
		return 0, err
	}
	return uintptr(fd), nil
}

// NewFileFromFd creates a new os.File from a file descriptor handle and a name.
func NewFileFromFd(handle uintptr, name string) *os.File {
	return os.NewFile(handle, name)
}

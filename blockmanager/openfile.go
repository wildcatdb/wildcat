//go:build darwin || linux || freebsd || netbsd || openbsd

package blockmanager

import (
	"os"
	"syscall"
)

func OpenFile(name string, flags int, perm uint32) (uintptr, error) {
	fd, err := syscall.Open(name, flags, perm)
	if err != nil {
		return 0, err
	}
	return uintptr(fd), nil
}

func NewFileFromFd(handle uintptr, name string) *os.File {
	return os.NewFile(handle, name)
}

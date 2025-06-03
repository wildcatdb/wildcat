//go:build windows

package blockmanager

import (
	"os"
	"syscall"
)

func OpenFile(name string, flags int, perm uint32) (uintptr, error) {
	var access uint32
	var creation uint32
	var windowsFlags uint32 = syscall.FILE_FLAG_OVERLAPPED | syscall.FILE_FLAG_RANDOM_ACCESS

	// Map common Unix flags to Windows equivalents
	switch flags & (syscall.O_RDONLY | syscall.O_WRONLY | syscall.O_RDWR) {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	default:
		access = syscall.GENERIC_READ
	}

	// Handle creation flags
	if flags&syscall.O_CREATE != 0 {
		if flags&syscall.O_EXCL != 0 {
			creation = syscall.CREATE_NEW
		} else if flags&syscall.O_TRUNC != 0 {
			creation = syscall.CREATE_ALWAYS
		} else {
			creation = syscall.OPEN_ALWAYS
		}
	} else if flags&syscall.O_TRUNC != 0 {
		creation = syscall.TRUNCATE_EXISTING
	} else {
		creation = syscall.OPEN_EXISTING
	}

	// Handle append mode
	if flags&syscall.O_APPEND != 0 {
		access |= syscall.FILE_APPEND_DATA
	}

	// Convert filename to UTF16
	namePtr, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}

	// Open the file
	handle, err := syscall.CreateFile(
		namePtr,
		access,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
		nil,
		creation,
		windowsFlags,
		0,
	)

	if err != nil {
		return 0, err
	}

	return uintptr(handle), nil
}

func NewFileFromFd(handle uintptr, name string) *os.File {
	return os.NewFile(handle, name)
}

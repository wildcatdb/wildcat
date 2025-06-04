//go:build windows

package blockmanager

import (
	"golang.org/x/sys/windows"
	"os"
	"syscall"
)

func OpenFile(name string, flags int, perm uint32) (uintptr, error) {
	var access uint32
	var creation uint32
	var windowsFlags uint32 = windows.FILE_FLAG_RANDOM_ACCESS

	// Map common Unix flags to Windows equivalents
	switch flags & (windows.O_RDONLY | windows.O_WRONLY | windows.O_RDWR) {
	case windows.O_RDONLY:
		access = syscall.GENERIC_READ
	case windows.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case windows.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	default:
		access = syscall.GENERIC_READ
	}

	hasCreate := flags&windows.O_CREAT != 0
	hasTrunc := flags&windows.O_TRUNC != 0
	hasExcl := flags&windows.O_EXCL != 0

	if hasCreate {
		if hasExcl {
			// Create new file, fail if exists
			creation = syscall.CREATE_NEW
		} else if hasTrunc {
			// Create new or truncate existing
			creation = syscall.CREATE_ALWAYS
		} else {
			// Create new or open existing (don't truncate)
			creation = syscall.OPEN_ALWAYS
		}
	} else {
		if hasTrunc {
			// Truncate existing file, fail if doesn't exist
			creation = syscall.TRUNCATE_EXISTING
		} else {
			// Open existing file, fail if doesn't exist
			creation = syscall.OPEN_EXISTING
		}
	}

	// Convert filename to UTF16
	namePtr, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}

	// Open the file with enhanced sharing for better multi-process support
	handle, err := syscall.CreateFile(
		namePtr,
		access,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
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

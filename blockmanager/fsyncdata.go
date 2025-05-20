//go:build darwin || linux || freebsd || netbsd || openbsd

package blockmanager

import (
	"runtime"
	"syscall"
)

func Fdatasync(fd uintptr) error {
	// On Darwin/macOS, Fdatasync is not available, so we fall back to Fsync..
	if runtime.GOOS == "darwin" {
		return syscall.Fsync(int(fd))
	}

	err := syscall.Fdatasync(int(fd))
	if err != nil {
		return err
	}

	return nil
}

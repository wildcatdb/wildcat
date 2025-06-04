//go:build linux || freebsd || netbsd || openbsd

package blockmanager

import (
	"syscall"
)

func Fdatasync(fd uintptr) error {
	err := syscall.Fdatasync(int(fd))
	if err != nil {
		return err
	}

	return nil
}

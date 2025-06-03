//go:build darwin
// +build darwin

package blockmanager

func Fdatasync(fd uintptr) error {
	// F_FULLFSYNC forces the drive to flush its buffers to stable storage.
	_, _, errno := unix.Syscall(
		unix.SYS_FCNTL,
		fd,
		unix.F_FULLFSYNC,
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

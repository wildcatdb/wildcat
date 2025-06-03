//go:build windows
// +build windows

package blockmanager

// pwrite performs an atomic write at a specific offset without needing to Seek first
func pwrite(fd uintptr, data []byte, offset int64, f *os.File) (int, error) {
	return f.WriteAt(data, offset)
}

// pread performs an atomic read from a specific offset without needing to Seek first
func pread(fd uintptr, data []byte, offset int64, f *os.File) (int, error) {
	return f.ReadAt(data, offset)
}

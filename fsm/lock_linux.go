//go:build linux

package fsm

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// readLock acquires a shared (read) OFD lock on f's [start, start+n) byte
// range, blocking until available if requested.
func readLock(f *os.File, start, n int64, blocking bool) error {
	fl := unix.Flock_t{Type: unix.F_RDLCK, Whence: io.SeekStart, Start: start, Len: n}
	cmd := unix.F_OFD_SETLK
	if blocking {
		cmd = unix.F_OFD_SETLKW
	}
	return unix.FcntlFlock(f.Fd(), cmd, &fl)
}

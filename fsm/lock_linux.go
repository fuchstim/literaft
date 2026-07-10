//go:build linux

package fsm

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// readLock acquires a shared (read) OFD lock on f's [start, start+n) byte
// range, blocking until available if requested. OFD locks (as opposed to
// classic per-process fcntl locks) are required here for the same reason
// internal/fsm/walappender/shm uses them: multiple *os.File opens of the
// same path within this one process must hold independent lock instances,
// not silently merge or release each other's on close.
func readLock(f *os.File, start, n int64, blocking bool) error {
	fl := unix.Flock_t{Type: unix.F_RDLCK, Whence: io.SeekStart, Start: start, Len: n}
	cmd := unix.F_OFD_SETLK
	if blocking {
		cmd = unix.F_OFD_SETLKW
	}
	return unix.FcntlFlock(f.Fd(), cmd, &fl)
}

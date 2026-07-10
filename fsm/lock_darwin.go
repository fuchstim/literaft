//go:build darwin

package fsm

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Darwin's OFD lock commands aren't exposed as named constants by
// golang.org/x/sys/unix; these raw values are pinned from
// https://github.com/apple/darwin-xnu/blob/main/bsd/sys/fcntl.h (as
// upstream itself documents) -- same values internal/fsm/walappender/shm's
// own lock_darwin.go pins, for the same reason.
const (
	_F_OFD_SETLK  = 90
	_F_OFD_SETLKW = 91
)

// readLock acquires a shared (read) OFD lock on f's [start, start+n) byte
// range, blocking until available if requested. OFD locks (as opposed to
// classic per-process fcntl locks) are required here for the same reason
// internal/fsm/walappender/shm uses them: multiple *os.File opens of the
// same path within this one process must hold independent lock instances,
// not silently merge or release each other's on close.
func readLock(f *os.File, start, n int64, blocking bool) error {
	fl := unix.Flock_t{Type: unix.F_RDLCK, Whence: io.SeekStart, Start: start, Len: n}
	cmd := _F_OFD_SETLK
	if blocking {
		cmd = _F_OFD_SETLKW
	}
	return unix.FcntlFlock(f.Fd(), cmd, &fl)
}

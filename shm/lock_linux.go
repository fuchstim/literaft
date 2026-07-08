//go:build linux

package shm

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Adapted from upstream/os_linux.go.upstream's osReadLock/osWriteLock/
// osLock/osUnlock: byte-range OFD locks via fcntl. Linux exposes the OFD
// lock commands directly as named golang.org/x/sys/unix constants.

func readLock(f *os.File, start, n int64, blocking bool) error {
	return lock(f, unix.F_RDLCK, start, n, blocking)
}

func writeLock(f *os.File, start, n int64, blocking bool) error {
	return lock(f, unix.F_WRLCK, start, n, blocking)
}

func lock(f *os.File, typ int16, start, n int64, blocking bool) error {
	fl := unix.Flock_t{Type: typ, Whence: io.SeekStart, Start: start, Len: n}
	cmd := unix.F_OFD_SETLK
	if blocking {
		cmd = unix.F_OFD_SETLKW
	}
	return unix.FcntlFlock(f.Fd(), cmd, &fl)
}

func unlock(f *os.File, start, n int64) error {
	fl := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart, Start: start, Len: n}
	for {
		err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &fl)
		if err != unix.EINTR {
			return err
		}
	}
}

// testLock reports the lock type (F_RDLCK/F_WRLCK/F_UNLCK) currently held
// on the range by any other open file description, without acquiring it.
func testLock(f *os.File, start, n int64) (int16, error) {
	fl := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart, Start: start, Len: n}
	for {
		err := unix.FcntlFlock(f.Fd(), unix.F_GETLK, &fl)
		if err == nil {
			return fl.Type, nil
		}
		if err != unix.EINTR {
			return 0, err
		}
	}
}

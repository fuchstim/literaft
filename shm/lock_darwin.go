//go:build darwin

package shm

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Adapted from upstream/os_darwin.go.upstream. Darwin's OFD lock commands
// aren't exposed as named constants by golang.org/x/sys/unix; these raw
// values are pinned from
// https://github.com/apple/darwin-xnu/blob/main/bsd/sys/fcntl.h (as
// upstream itself documents). F_GETLK, by contrast, is a plain, portable
// fcntl command and needs no substitution.
const (
	_F_OFD_SETLK  = 90
	_F_OFD_SETLKW = 91
)

func readLock(f *os.File, start, n int64, blocking bool) error {
	return lock(f, unix.F_RDLCK, start, n, blocking)
}

func writeLock(f *os.File, start, n int64, blocking bool) error {
	return lock(f, unix.F_WRLCK, start, n, blocking)
}

func lock(f *os.File, typ int16, start, n int64, blocking bool) error {
	fl := unix.Flock_t{Type: typ, Whence: io.SeekStart, Start: start, Len: n}
	cmd := _F_OFD_SETLK
	if blocking {
		cmd = _F_OFD_SETLKW
	}
	return unix.FcntlFlock(f.Fd(), cmd, &fl)
}

func unlock(f *os.File, start, n int64) error {
	fl := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart, Start: start, Len: n}
	for {
		err := unix.FcntlFlock(f.Fd(), _F_OFD_SETLK, &fl)
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

//go:build linux

package lock

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func ReadLock(f *os.File, start, n int64, blocking bool) error {
	return lock(f, unix.F_RDLCK, start, n, blocking)
}

func WriteLock(f *os.File, start, n int64, blocking bool) error {
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

func Unlock(f *os.File, start, n int64) error {
	fl := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart, Start: start, Len: n}
	for {
		err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &fl)
		if err != unix.EINTR {
			return err
		}
	}
}

// TestLock reports the lock type (F_RDLCK/F_WRLCK/F_UNLCK) currently held
// on the range by any other open file description, without acquiring it.
func TestLock(f *os.File, start, n int64) (int16, error) {
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

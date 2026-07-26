package shm

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/fuchstim/literaft/internal/lock"
	"github.com/hashicorp/go-hclog"
	"golang.org/x/sys/unix"
)

const (
	dmsLockRetryDelay   = 10 * time.Millisecond
	dmsLockRetryTimeout = 2 * time.Second
)

// SQLite WALINDEX_PGSZ
const regionSize = 32768

// Lock byte offsets within the wal-index, see https://sqlite.org/walformat.html
const (
	baseOffset = 120 // (22 + nLocks) * 4
	nLocks     = 8
	dmsOffset  = baseOffset + nLocks // 128

	WriteLock      = 0 // WAL_WRITE_LOCK
	CheckpointLock = 1 // WAL_CKPT_LOCK
	RecoverLock    = 2 // WAL_RECOVER_LOCK
)

// NReaders is WAL_NREADER: the number of reader-mark slots (and read-lock
// byte offsets) in the wal-index.
const NReaders = nLocks - 3

// ReadLock returns the lock index for reader slot i (0 <= i < NReaders).
func ReadLock(i int) int { return 3 + i }

// SharedMemory is a raw mmap of a SQLite -shm wal-index file
type SharedMemory struct {
	file    *os.File
	regions [][]byte
	logger  hclog.Logger
}

func Open(path string, logger hclog.Logger) (*SharedMemory, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	s := &SharedMemory{file: f, logger: logger}

	typ, err := lock.TestLock(f, dmsOffset, 1)
	if err != nil {
		f.Close()
		return nil, err
	}
	if typ == unix.F_WRLCK {
		f.Close()
		return nil, errors.New("dead man's switch held exclusively by another opener")
	}
	if typ == unix.F_UNLCK {
		if err := acquireDMSWithRetries(f, lock.WriteLock); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to claim dead man's switch: %w", err)
		}
		if err := f.Truncate(0); err != nil {
			f.Close()
			return nil, err
		}
		logger.Debug("opened wal-index as first opener; truncated stale content", "path", path)
	} else {
		logger.Debug("joined existing wal-index mapping", "path", path)
	}

	if err := acquireDMSWithRetries(f, lock.ReadLock); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to acquire shared dead man's switch lock: %w", err)
	}
	return s, nil
}

func (s *SharedMemory) Lock(index int) error {
	return lock.WriteLock(s.file, baseOffset+int64(index), 1, true)
}

func (s *SharedMemory) TryLock(index int) error {
	return lock.WriteLock(s.file, baseOffset+int64(index), 1, false)
}

func (s *SharedMemory) TryLockRange(index, n int) error {
	return lock.WriteLock(s.file, baseOffset+int64(index), int64(n), false)
}

func (s *SharedMemory) UnlockRange(index, n int) error {
	return lock.Unlock(s.file, baseOffset+int64(index), int64(n))
}

func (s *SharedMemory) RLock(index int) error {
	return lock.ReadLock(s.file, baseOffset+int64(index), 1, true)
}

func (s *SharedMemory) Unlock(index int) error {
	return lock.Unlock(s.file, baseOffset+int64(index), 1)
}

func (s *SharedMemory) GetRegion(id int) ([]byte, error) {
	for len(s.regions) <= id {
		if err := s.mapRegion(len(s.regions)); err != nil {
			return nil, err
		}
	}
	return s.regions[id], nil
}

func (s *SharedMemory) Close() error {
	for _, r := range s.regions {
		unix.Munmap(r)
	}
	s.regions = nil
	return s.file.Close()
}

func acquireDMSWithRetries(f *os.File, lockFn func(f *os.File, start, n int64, blocking bool) error) error {
	deadline := time.Now().Add(dmsLockRetryTimeout)
	for {
		err := lockFn(f, dmsOffset, 1, false)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(dmsLockRetryDelay)
	}
}

func (s *SharedMemory) mapRegion(id int) error {
	off := int64(id) * regionSize
	fi, err := s.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() < off+regionSize {
		if err := s.file.Truncate(off + regionSize); err != nil {
			return err
		}
	}
	region, err := unix.Mmap(int(s.file.Fd()), off, regionSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	s.regions = append(s.regions, region)
	return nil
}

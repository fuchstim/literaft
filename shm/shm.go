// Package shm gives the follower-apply path (see apply/) direct control
// over a SQLite -shm wal-index file: mmap its bytes and take the same OFD
// byte-range locks a real SQLite connection would, without going through
// sqlite3vfs.SharedMemory (which is deliberately opaque -- see
// docs/NCRUCES_NOTES.md §"Why the SHM must be vendored"). See README.md for
// what's adapted from upstream and why it isn't a literal copy.
package shm

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// regionSize is SQLite's fixed wal-index shared-memory granularity
// (WALINDEX_PGSZ): every mmap segment is exactly this many bytes.
const regionSize = 32768

// Lock byte offsets within the wal-index, copied from
// upstream/const.go.upstream's _SHM_BASE/_SHM_NLOCK/_SHM_DMS and confirmed
// against the locking-byte-offset table at
// https://sqlite.org/walformat.html.
const (
	baseOffset = 120 // (22 + nLocks) * 4
	nLocks     = 8
	dmsOffset  = baseOffset + nLocks // 128; ncruces' own shm-open handshake lock, not a SQLite-defined one

	WriteLock      = 0 // WAL_WRITE_LOCK
	CheckpointLock = 1 // WAL_CKPT_LOCK
	RecoverLock    = 2 // WAL_RECOVER_LOCK
)

// NReaders is WAL_NREADER: the number of reader-mark slots (and read-lock
// byte offsets) in the wal-index.
const NReaders = nLocks - 3

// ReadLock returns the lock index for reader slot i (0 <= i < NReaders).
func ReadLock(i int) int { return 3 + i }

// SharedMemory is a raw mmap of a SQLite -shm wal-index file, driven
// directly rather than through a SQLite connection. It takes the same OFD
// byte-range locks and the same "dead man's switch" open handshake as
// ncruces' own shm implementation, so it coordinates correctly with real
// SQLite connections mapping the same file (docs/NCRUCES_NOTES.md
// §"Coordination within one process").
type SharedMemory struct {
	file    *os.File
	regions [][]byte
}

// Open opens (creating if necessary) the wal-index file at path and
// performs the shm-open handshake: if no one else has it open, truncate
// and reinitialize it; otherwise join the existing mapping. Adapted from
// upstream/shm_ofd.go.upstream's shmOpen.
func Open(path string) (*SharedMemory, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	s := &SharedMemory{file: f}

	typ, err := testLock(f, dmsOffset, 1)
	if err != nil {
		f.Close()
		return nil, err
	}
	if typ == unix.F_WRLCK {
		f.Close()
		return nil, errors.New("shm: dead man's switch held exclusively by another opener")
	}
	if typ == unix.F_UNLCK {
		// We're the first opener: claim the switch exclusively, wipe any
		// stale content, then downgrade to the shared lock every opener
		// holds for as long as the shm is in use.
		if err := writeLock(f, dmsOffset, 1, true); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Truncate(0); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := readLock(f, dmsOffset, 1, true); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// Region returns the mapped bytes for wal-index page id (0-based), each
// regionSize bytes, growing the file and mapping new segments as needed.
// This mirrors SQLite's own incremental xShmMap semantics.
func (s *SharedMemory) Region(id int) ([]byte, error) {
	for len(s.regions) <= id {
		if err := s.mapRegion(len(s.regions)); err != nil {
			return nil, err
		}
	}
	return s.regions[id], nil
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

// Lock acquires an exclusive lock on the given lock index (WriteLock,
// CheckpointLock, RecoverLock, or ReadLock(i)), blocking until available.
func (s *SharedMemory) Lock(index int) error {
	return writeLock(s.file, baseOffset+int64(index), 1, true)
}

// TryLock is like Lock but returns immediately if the lock is unavailable.
func (s *SharedMemory) TryLock(index int) error {
	return writeLock(s.file, baseOffset+int64(index), 1, false)
}

// RLock acquires a shared lock on the given lock index, blocking until
// available.
func (s *SharedMemory) RLock(index int) error {
	return readLock(s.file, baseOffset+int64(index), 1, true)
}

// Unlock releases the lock on the given index.
func (s *SharedMemory) Unlock(index int) error {
	return unlock(s.file, baseOffset+int64(index), 1)
}

// Close unmaps all regions and closes the underlying file, releasing the
// dead man's switch lock.
func (s *SharedMemory) Close() error {
	for _, r := range s.regions {
		unix.Munmap(r)
	}
	s.regions = nil
	return s.file.Close()
}

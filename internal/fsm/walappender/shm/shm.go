package shm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/fuchstim/literaft/internal/lock"
	"github.com/fuchstim/literaft/internal/wal"
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
	lockBaseOffset = checkpointInfoOffset + checkpointInfoNReaders*4 + 4
	dmsOffset      = lockBaseOffset + checkpointInfoNReaders + 3 // 128

	WriteLock      = 0 // WAL_WRITE_LOCK
	CheckpointLock = 1 // WAL_CKPT_LOCK
	RecoverLock    = 2 // WAL_RECOVER_LOCK
)

const NReaders = checkpointInfoNReaders

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

// Try copy 0 first and verify its checksum. If its invalid, fallback to copy 1.
// If both are invalid, an uninitialized header is returned (IsInit() == false)
func (s *SharedMemory) ReadHeader() (Header, error) {
	r0, err := s.getRegion(0)
	if err != nil {
		return Header{}, fmt.Errorf("failed to get region 0: %w", err)
	}

	copy0 := slices.Clone(r0[0:headerCopySize])
	copy1 := slices.Clone(r0[headerCopySize : 2*headerCopySize])

	for _, c := range [][]byte{copy0, copy1} {
		checksum1, checksum2 := wal.ComputeChecksums(binary.LittleEndian, c[:40], 0, 0)
		hdr := Header(c)

		if checksum1 == hdr.Checksum1() && checksum2 == hdr.Checksum2() {
			return hdr, nil
		}
	}

	return Header{}, nil
}

func (s *SharedMemory) WriteHeader(h Header) error {
	r0, err := s.getRegion(0)
	if err != nil {
		return fmt.Errorf("failed to get region 0: %w", err)
	}

	h.UpdateChecksums()

	copy(r0[headerCopySize:2*headerCopySize], h[:])
	barrier()
	copy(r0[0:headerCopySize], h[:])

	return nil
}

func (s *SharedMemory) ReadCheckpointInfo() (CheckpointInfo, error) {
	r0, err := s.getRegion(0)
	if err != nil {
		return CheckpointInfo{}, fmt.Errorf("failed to get region 0: %w", err)
	}

	return readCheckpointInfo(r0), nil
}

func (s *SharedMemory) WriteCheckpointInfo(info CheckpointInfo) error {
	r0, err := s.getRegion(0)
	if err != nil {
		return fmt.Errorf("failed to get region 0: %w", err)
	}

	writeCheckpointInfo(info, r0)
	return nil
}

// AddFrame records in the wal-index that WAL frame `frameIdx` holds database page number `pgNo`.
func (s *SharedMemory) AddFrame(frameIdx, pgNo uint32) error {
	regionID := regionForFrame(frameIdx)
	region, err := s.getRegion(regionID)
	if err != nil {
		return fmt.Errorf("failed to get region %d for frame %d: %w", regionID, frameIdx, err)
	}

	pgNoOff, hashOff := hashTableOffsets(regionID)
	idx := int(frameIdx - frameZeroForRegion(regionID)) // 1-based position within this region's aPgno array

	if idx == 1 {
		// First entry in this segment: it may hold stale data from a
		// prior WAL epoch that reused this mapped region -- wipe the
		// whole segment before adding the new entry.
		clear(region[pgNoOff:])
	}

	binary.LittleEndian.PutUint32(region[pgNoOff+(idx-1)*4:], pgNo)
	for slot := hashSlotForPage(pgNo); ; slot = nextHashSlot(slot) {
		off := hashOff + slot*2
		if binary.LittleEndian.Uint16(region[off:]) == 0 {
			binary.LittleEndian.PutUint16(region[off:], uint16(idx))
			return nil
		}
	}
}

func (s *SharedMemory) Lock(index int) error {
	return lock.WriteLock(s.file, lockBaseOffset+int64(index), 1, true)
}

func (s *SharedMemory) TryLock(index int) error {
	return lock.WriteLock(s.file, lockBaseOffset+int64(index), 1, false)
}

func (s *SharedMemory) TryLockRange(index, n int) error {
	return lock.WriteLock(s.file, lockBaseOffset+int64(index), int64(n), false)
}

func (s *SharedMemory) UnlockRange(index, n int) error {
	return lock.Unlock(s.file, lockBaseOffset+int64(index), int64(n))
}

func (s *SharedMemory) RLock(index int) error {
	return lock.ReadLock(s.file, lockBaseOffset+int64(index), 1, true)
}

func (s *SharedMemory) Unlock(index int) error {
	return lock.Unlock(s.file, lockBaseOffset+int64(index), 1)
}

func (s *SharedMemory) Close() error {
	for _, r := range s.regions {
		unix.Munmap(r)
	}
	s.regions = nil
	return s.file.Close()
}

func (s *SharedMemory) getRegion(id int) ([]byte, error) {
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

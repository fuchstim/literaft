package walappender

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fuchstim/literaft/internal/fsm/walappender/shm"
	"github.com/fuchstim/literaft/internal/wal"
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3"
)

const lockPollInterval = time.Millisecond

type WALAppender struct {
	pageSize uint32
	db       *sqlite3.Conn
	f        *os.File
	shm      *shm.SharedMemory
	logger   hclog.Logger

	checkpointTicker         *time.Ticker
	checkpointThresholdPages int

	dirtyPageCount   int
	dirtyPageCountMu sync.Mutex

	// lockCh is a one-token semaphore fronting the OFD WAL_WRITE_LOCK. The OFD
	// lock excludes SQLite's connections and other processes, but two
	// acquisitions through this appender's single shm handle don't exclude
	// each other (same-OFD requests convert, and any unlock releases the
	// whole OFD's claim). This token restores intra-process exclusion once a
	// second in-process holder exists. Lock order: token, then OS lock.
	lockCh chan struct{}

	// *sqlite3.Conn is not safe for concurrent use, overlapping checkpoint calls
	// corrupt the database file
	checkpointMu sync.Mutex
	closed       bool
}

func Open(dbPath string, pageSize uint32, checkpointThresholdPages int, checkpointInterval time.Duration, logger hclog.Logger) (*WALAppender, error) {
	db, err := sqlite3.Open("file:" + dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at path `%s`: %w", dbPath, err)
	}

	// A *sqlite3.Conn that's never read anything
	// silently declines every WALCheckpoint call (nLog/nCkpt come back
	// -1/-1, no error). One throwaway read fixes this.
	if err := db.Exec("SELECT 1=1"); err != nil {
		return nil, errors.Join(db.Close(), fmt.Errorf("failed to prime checkpoint connection: %w", err))
	}

	f, err := os.OpenFile(dbPath+"-wal", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Join(db.Close(), fmt.Errorf("failed to open -wal file at path `%s`: %w", dbPath+"-wal", err))
	}

	sm, err := shm.Open(dbPath+"-shm", logger.Named("shm"))
	if err != nil {
		return nil, errors.Join(db.Close(), f.Close(), fmt.Errorf("failed to open -shm file at path `%s`: %w", dbPath+"-shm", err))
	}

	w := &WALAppender{
		pageSize:                 pageSize,
		db:                       db,
		f:                        f,
		shm:                      sm,
		logger:                   logger,
		checkpointThresholdPages: checkpointThresholdPages,
		lockCh:                   make(chan struct{}, 1),
	}
	w.lockCh <- struct{}{} // the single write-lock token starts available
	if err := w.maybeBootstrap(); err != nil {
		return nil, fmt.Errorf("failed to bootstrap WAL: %w", err)
	}

	if checkpointInterval > 0 {
		w.checkpointTicker = time.NewTicker(checkpointInterval)
		go w.runCheckpointer()
	}

	return w, nil
}

func (a *WALAppender) Close() error {
	if a.checkpointTicker != nil {
		a.checkpointTicker.Stop()
	}

	a.checkpointMu.Lock()
	a.closed = true
	a.checkpointMu.Unlock()

	return errors.Join(a.db.Close(), a.f.Close(), a.shm.Close())
}

type HeldLock struct {
	a        *WALAppender
	released bool
}

func (a *WALAppender) AcquireWriteLock(ctx context.Context) (*HeldLock, error) {
	select {
	case <-a.lockCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := a.lockOFD(ctx); err != nil {
		a.lockCh <- struct{}{}
		return nil, err
	}
	return &HeldLock{a: a}, nil
}

func (a *WALAppender) lockOFD(ctx context.Context) error {
	if ctx.Done() == nil {
		if err := a.shm.Lock(shm.WriteLock); err != nil {
			return fmt.Errorf("failed to acquire WAL_WRITE_LOCK: %w", err)
		}
		return nil
	}
	for {
		err := a.shm.TryLock(shm.WriteLock)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to acquire WAL_WRITE_LOCK before deadline: %w", ctx.Err())
		case <-time.After(lockPollInterval):
		}
	}
}

func (h *HeldLock) Release() {
	if h.released {
		return
	}
	h.released = true
	h.a.shm.Unlock(shm.WriteLock)
	h.a.lockCh <- struct{}{}
	h.a.maybeThresholdCheckpoint()
}

// AppendFrames appends `frames` to the local -wal while acquiring and
// releasing the write lock. afterCommit (optional) is executed under that lock
// immediately after mxFrame is published. `frames` checksums will be re-computed
// according to the current WAL state.
func (a *WALAppender) AppendFrames(frames []*wal.Frame, afterCommit func()) error {
	h, err := a.AcquireWriteLock(context.Background())
	if err != nil {
		return err
	}
	defer h.Release()

	return a.AppendFramesUnderLock(h, frames, afterCommit)
}

// AppendFramesUnderLock is identical to AppendFrames, but must be run
// under a HeldLock acquired from AcquireWriteLock.
func (a *WALAppender) AppendFramesUnderLock(h *HeldLock, frames []*wal.Frame, afterCommit func()) error {
	if h == nil || h.a != a || h.released {
		return fmt.Errorf("AppendFramesUnderLock called without a valid held lock for this appender")
	}
	return a.appendFramesLocked(frames, afterCommit)
}

func (a *WALAppender) appendFramesLocked(frames []*wal.Frame, afterCommit func()) error {
	hdr, err := a.shm.ReadHeader()
	if err != nil {
		return fmt.Errorf("failed to read wal-index header: %w", err)
	}

	hdr, err = a.rewindLogIfBackfilled(hdr)
	if err != nil {
		return fmt.Errorf("failed to rewind WAL log: %w", err)
	}

	currentFrameIdx := hdr.MaxFrame()
	currentOffset := wal.WALHeaderSize + int64(currentFrameIdx)*(wal.FrameHeaderSize+int64(a.pageSize))
	checksumEnc := binary.ByteOrder(binary.LittleEndian)
	if hdr.BigEndianChecksum() {
		checksumEnc = binary.BigEndian
	}
	lastFrameChecksum1, lastFrameChecksum2 := hdr.LastFrameChecksum1(), hdr.LastFrameChecksum2()

	for _, f := range frames {
		if len(f.Data) != int(a.pageSize) {
			return fmt.Errorf("frame for page %d is %d bytes, cluster page size is %d bytes",
				f.Header.PgNo(), len(f.Data), a.pageSize)
		}

		f.Header.SetSalt1(hdr.Salt1())
		f.Header.SetSalt2(hdr.Salt2())
		f.UpdateChecksums(checksumEnc, lastFrameChecksum1, lastFrameChecksum2)
		lastFrameChecksum1, lastFrameChecksum2 = f.Header.Checksum1(), f.Header.Checksum2()

		currentFrameIdx++
		if _, err := a.f.WriteAt(f.Header[:], currentOffset); err != nil {
			return fmt.Errorf("failed to write header for frame frame %d: %w", currentFrameIdx, err)
		}
		if _, err := a.f.WriteAt(f.Data, currentOffset+wal.FrameHeaderSize); err != nil {
			return fmt.Errorf("failed to write data for frame %d: %w", currentFrameIdx, err)
		}
		currentOffset += wal.FrameHeaderSize + int64(a.pageSize)

		if err := a.shm.AddFrame(currentFrameIdx, f.Header.PgNo()); err != nil {
			return fmt.Errorf("failed to add frame %d to wal-index: %w", currentFrameIdx, err)
		}

		a.dirtyPageCountMu.Lock()
		a.dirtyPageCount++
		a.dirtyPageCountMu.Unlock()

		if f.Header.NTruncate() != 0 {
			hdr.SetMaxFrame(currentFrameIdx)
			hdr.SetPageCount(f.Header.NTruncate())
			hdr.SetLastFrameChecksum1(lastFrameChecksum1)
			hdr.SetLastFrameChecksum2(lastFrameChecksum2)
			hdr.SetChangeCounter(hdr.ChangeCounter() + 1)

			if err := a.shm.WriteHeader(hdr); err != nil {
				return fmt.Errorf("failed to write wal-index header: %w", err)
			}

			a.logger.Debug("published wal-index header",
				"mxFrame", currentFrameIdx, "nPage", f.Header.NTruncate(), "frames", len(frames))
		}
	}

	if afterCommit != nil {
		afterCommit()
	}

	return nil
}

// rewindLogIfBackfilled checks if all WAL frames have been copied into the database
// and if no readers are currently using the WAL. If so, it rewinds the WAL to the beginning.
// Must be called while holding the WAL write lock.
func (a *WALAppender) rewindLogIfBackfilled(hdr shm.Header) (shm.Header, error) {
	if err := a.shm.TryLockRange(shm.ReadLock(1), shm.NReaders-1); err != nil {
		return hdr, nil // Some readers are still using the WAL, skip the rewind
	}
	defer a.shm.UnlockRange(shm.ReadLock(1), shm.NReaders-1)

	checkpointInfo, err := a.shm.ReadCheckpointInfo()
	if err != nil {
		return hdr, fmt.Errorf("failed to read checkpoint info: %w", err)
	}

	if hdr.MaxFrame() == 0 || checkpointInfo.NBackfill() < hdr.MaxFrame() {
		return hdr, nil // Not all frames have been backfilled, skip the rewind
	}

	walHdr, err := a.writeWALFileHeader()
	if err != nil {
		return hdr, fmt.Errorf("failed to write -wal file header: %w", err)
	}

	hdr.SetMaxFrame(0)
	hdr.SetPageCount(0)
	hdr.SetLastFrameChecksum1(walHdr.Checksum1())
	hdr.SetLastFrameChecksum2(walHdr.Checksum2())
	hdr.SetSalt1(walHdr.Salt1())
	hdr.SetSalt2(walHdr.Salt2())
	hdr.SetChangeCounter(hdr.ChangeCounter() + 1)

	if err := a.shm.WriteHeader(hdr); err != nil {
		return hdr, fmt.Errorf("failed to write wal-index header: %w", err)
	}

	checkpointInfo.ResetForRewind()
	if err := a.shm.WriteCheckpointInfo(checkpointInfo); err != nil {
		return hdr, fmt.Errorf("failed to write checkpoint info: %w", err)
	}

	a.logger.Debug("rewound backfilled WAL log to start")

	return hdr, nil
}

func (a *WALAppender) maybeBootstrap() error {
	hdr, err := a.shm.ReadHeader()
	if err != nil {
		return fmt.Errorf("failed to read wal-index header: %w", err)
	}

	if hdr.IsInit() { // Already bootstrapped
		return nil
	}

	fi, err := a.f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat -wal file: %w", err)
	}
	if fi.Size() > wal.WALHeaderSize {
		return fmt.Errorf("-wal file already has %d bytes but the wal-index is uninitialized; "+
			"recovery from an existing WAL is not supported", fi.Size())
	}

	walHdr, err := a.writeWALFileHeader()
	if err != nil {
		return fmt.Errorf("failed to initialize -wal file header: %w", err)
	}

	idxHdr := shm.InitHeader(a.pageSize, walHdr.Checksum1(), walHdr.Checksum2(), walHdr.Salt1(), walHdr.Salt2())
	if err := a.shm.WriteHeader(idxHdr); err != nil {
		return fmt.Errorf("failed to write wal-index header: %w", err)
	}

	return nil
}

func (a *WALAppender) writeWALFileHeader() (wal.WALHeader, error) {
	hdr, err := wal.InitHeader(a.pageSize)
	if err != nil {
		return wal.WALHeader{}, fmt.Errorf("failed to initialize -wal file header: %w", err)
	}
	if _, err := a.f.WriteAt(hdr[:], 0); err != nil {
		return wal.WALHeader{}, fmt.Errorf("failed to write -wal file header: %w", err)
	}

	return hdr, nil
}

func (a *WALAppender) runCheckpointer() {
	for range a.checkpointTicker.C {
		a.checkpoint()
	}
}

// maybeThresholdCheckpoint runs a passive checkpoint if dirtyPageCount >= checkpointThresholdPages.
// Must be run after WAL write lock is released, otherwise the checkpoint does nothing.
func (a *WALAppender) maybeThresholdCheckpoint() {
	a.dirtyPageCountMu.Lock()
	checkpoint := a.dirtyPageCount >= a.checkpointThresholdPages
	if checkpoint {
		a.dirtyPageCount = 0
	}
	a.dirtyPageCountMu.Unlock()

	if checkpoint {
		a.checkpoint()
	}
}

func (a *WALAppender) checkpoint() {
	a.checkpointMu.Lock()
	defer a.checkpointMu.Unlock()
	if a.closed {
		return
	}
	nLog, nCkpt, err := a.db.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE)
	if err != nil {
		a.logger.Debug("passive checkpoint failed", "error", err)
		return
	}
	a.logger.Debug("ran passive checkpoint", "walFrames", nLog, "checkpointed", nCkpt)
}

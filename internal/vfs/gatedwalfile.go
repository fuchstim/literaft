package vfs

import (
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/fuchstim/literaft/internal/wal"
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

type gatedWALFile struct {
	sqlite3vfs.File
	gate     Gate
	logger   hclog.Logger
	pageSize uint32

	// Frame header & data are written separately, so we need to track the header until the data arrives.
	pendingFrameHeader *wal.FrameHeader
	// Uncommitted frames captured in the current transaction
	currentTxFrames []*wal.Frame
	// Same as above, but indexed by the offset of the frame header in the WAL file
	currentTxFrameOffsets map[int64]*wal.Frame
	// Whether or not the current transaction has been committed. Used to determine the start of a new transaction
	currentTxCommitted bool
	// Highest offset seen in the WAL file. Used to detect WAL rewinds
	maxOffset int64
}

func newGatedWALFile(base sqlite3vfs.File, gate Gate, logger hclog.Logger) (*gatedWALFile, error) {
	headerBytes, pageSize := make([]byte, wal.WALHeaderSize), uint32(0)
	if n, err := base.ReadAt(headerBytes, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read WAL header: %w", err)
	} else if n == len(headerBytes) {
		h := wal.WALHeader(headerBytes)
		pageSize = h.PageSize()
	} // If n<headerBytes the WAL file might not be initialized yet. In that case we intercept the header write to parse the frame size

	return &gatedWALFile{
		File:     base,
		gate:     gate,
		logger:   logger,
		pageSize: pageSize,

		currentTxFrameOffsets: make(map[int64]*wal.Frame),
	}, nil
}

func (f *gatedWALFile) WriteAt(p []byte, off int64) (int, error) {
	if off == 0 {
		if f.pageSize == 0 && len(p) == wal.WALHeaderSize {
			h := wal.WALHeader(p)
			f.pageSize = h.PageSize()
		}

		return f.File.WriteAt(p, off)
	}

	if f.pageSize == 0 {
		return 0, fmt.Errorf("WAL page size unknown; cannot write frame at offset %d", off)
	}

	switch {
	case wal.IsFrameHeaderOffset(f.pageSize, off):
		return f.writeFrameHeader(p, off)
	case wal.IsFrameDataOffset(f.pageSize, off):
		return f.writeFrameData(p, off)

	default:
		return 0, fmt.Errorf("unexpected WAL write of length %d at offset %d (page size %d)", len(p), off, f.pageSize)
	}
}

func (f *gatedWALFile) writeFrameHeader(p []byte, off int64) (int, error) {
	if len(p) != wal.FrameHeaderSize {
		err := fmt.Errorf("unexpected WAL frame header write of length %d at offset %d (expected %d)", len(p), off, wal.FrameHeaderSize)
		return 0, sqlite3vfs.SystemError(err, sqlite3.IOERR_WRITE)
	}

	h := wal.FrameHeader(p)
	if f.currentTxCommitted {
		// WAL frame checksums for a committed transaction can be rewritten (same offset, same pgno)
		// The data stays unchanged, only the checksum bytes change.
		if pending, seen := f.currentTxFrameOffsets[off]; seen && pending.Header.PgNo() == h.PgNo() {
			pending.Header = &h

			return f.File.WriteAt(p, off)
		}
	}

	if f.currentTxCommitted || off <= f.maxOffset {
		// This is either a new transaction (currentTxCommitted) or a WAL rewind (off <= maxOffset).
		// In either case, the prior transaction's tracked history no longer applies.
		f.resetCurrentTx()
	}

	f.pendingFrameHeader = &h
	f.maxOffset = off

	// If this is not a commit frame, don't withhold
	if !h.IsCommit() {
		return f.File.WriteAt(p, off)
	}

	f.logger.Debug("withholding commit frame pending RAFT quorum",
		"offset", off, "pgno", h.PgNo, "nTruncate", h.NTruncate)

	return len(p), nil
}

func (f *gatedWALFile) writeFrameData(p []byte, off int64) (int, error) {
	if len(p) != int(f.pageSize) {
		err := fmt.Errorf("unexpected WAL frame data write of length %d at offset %d (expected %d)", len(p), off, f.pageSize)
		return 0, sqlite3vfs.SystemError(err, sqlite3.IOERR_WRITE)
	}

	if frame, seen := f.currentTxFrameOffsets[off-wal.FrameHeaderSize]; seen {
		// If the same page gets updated multiple times in the same transaction,
		// previous frames for that page can be overwritten with the new data.
		// The header remains the same, only the data part is updated.
		frame.Data = slices.Clone(p)
		return f.File.WriteAt(p, off)
	}

	if f.pendingFrameHeader == nil {
		err := fmt.Errorf("WAL frame data write at offset %d has no pending header", off)
		return 0, sqlite3vfs.SystemError(err, sqlite3.IOERR_WRITE)
	}

	pendingHeader := f.pendingFrameHeader
	f.pendingFrameHeader = nil

	frame := &wal.Frame{pendingHeader, slices.Clone(p)}
	f.currentTxFrames = append(f.currentTxFrames, frame)
	f.currentTxFrameOffsets[off-wal.FrameHeaderSize] = frame

	// If this is not a commit frame, don't withhold
	if !frame.Header.IsCommit() {
		return f.File.WriteAt(p, off)
	}

	frames, nTruncate := f.currentTxFrames, frame.Header.NTruncate()
	f.currentTxFrames = nil

	if err := f.gate.ProposeTransaction(frames); err != nil {
		f.logger.Info("gate rejected transaction; discarding withheld commit frame",
			"offset", off-wal.FrameHeaderSize, "frames", len(frames), "nTruncate", nTruncate, "error", err)

		f.resetCurrentTx()

		code := sqlite3.IOERR_WRITE
		if c, ok := ErrCode(err); ok && c != 0 {
			code = c
		}

		return 0, sqlite3vfs.SystemError(err, code)
	}

	f.currentTxCommitted = true
	f.logger.Debug("gate committed transaction; releasing withheld commit frame",
		"offset", off-wal.FrameHeaderSize, "frames", len(frames), "nTruncate", nTruncate)

	// Past this point the gate has committed the transaction cluster-wide,
	// and this node's own FSM.Apply has already consumed its skip marker
	// (hraft delivers each committed index exactly once). If flushing the
	// withheld commit frame now fails, the transaction is rolled back.
	// This node will never see the entry again, so it would silently and permanently
	// lack its own committed write. There is no way to recover from this, so we panic.
	if _, err := f.File.WriteAt(frame.Header[:], off-wal.FrameHeaderSize); err != nil {
		f.logger.Error("failed to flush committed commit-frame header after RAFT commit",
			"offset", off-wal.FrameHeaderSize, "error", err)
		panic(fmt.Sprintf("vfs: failed to flush committed commit-frame header at WAL offset %d after RAFT commit: %v", off-wal.FrameHeaderSize, err))
	}

	n2, err := f.File.WriteAt(frame.Data, off)
	if err != nil {
		f.logger.Error("failed to flush committed commit-frame data after RAFT commit",
			"offset", off, "error", err)
		panic(fmt.Sprintf("vfs: failed to flush committed commit-frame data at WAL offset %d after RAFT commit: %v", off, err))
	}

	return n2, nil
}

func (f *gatedWALFile) resetCurrentTx() {
	f.currentTxFrames = nil
	f.currentTxFrameOffsets = make(map[int64]*wal.Frame)
	f.currentTxCommitted = false
}

var (
	_ sqlite3vfs.File                   = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileUnwrap             = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileLockState          = (*gatedWALFile)(nil)
	_ sqlite3vfs.FilePersistWAL         = (*gatedWALFile)(nil)
	_ sqlite3vfs.FilePowersafeOverwrite = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileChunkSize          = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileSizeHint           = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileHasMoved           = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileOverwrite          = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileSync               = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileCommitPhaseTwo     = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileBatchAtomicWrite   = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileCheckpoint         = (*gatedWALFile)(nil)
	_ sqlite3vfs.FilePragma             = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileBusyHandler        = (*gatedWALFile)(nil)
	_ sqlite3vfs.FileSharedMemory       = (*gatedWALFile)(nil)
)

func (f *gatedWALFile) Unwrap() sqlite3vfs.File { return f.File }

func (f *gatedWALFile) LockState() sqlite3vfs.LockLevel {
	return vfsutil.WrapLockState(f.File)
}

func (f *gatedWALFile) PersistWAL() bool { return vfsutil.WrapPersistWAL(f.File) }
func (f *gatedWALFile) SetPersistWAL(keepWAL bool) {
	vfsutil.WrapSetPersistWAL(f.File, keepWAL)
}

func (f *gatedWALFile) PowersafeOverwrite() bool { return vfsutil.WrapPowersafeOverwrite(f.File) }
func (f *gatedWALFile) SetPowersafeOverwrite(psow bool) {
	vfsutil.WrapSetPowersafeOverwrite(f.File, psow)
}

func (f *gatedWALFile) ChunkSize(size int) { vfsutil.WrapChunkSize(f.File, size) }

func (f *gatedWALFile) SizeHint(size int64) error { return vfsutil.WrapSizeHint(f.File, size) }

func (f *gatedWALFile) HasMoved() (bool, error) { return vfsutil.WrapHasMoved(f.File) }

func (f *gatedWALFile) Overwrite() error { return vfsutil.WrapOverwrite(f.File) }

func (f *gatedWALFile) SyncSuper(super string) error { return vfsutil.WrapSyncSuper(f.File, super) }

func (f *gatedWALFile) CommitPhaseTwo() error { return vfsutil.WrapCommitPhaseTwo(f.File) }

func (f *gatedWALFile) BeginAtomicWrite() error    { return vfsutil.WrapBeginAtomicWrite(f.File) }
func (f *gatedWALFile) CommitAtomicWrite() error   { return vfsutil.WrapCommitAtomicWrite(f.File) }
func (f *gatedWALFile) RollbackAtomicWrite() error { return vfsutil.WrapRollbackAtomicWrite(f.File) }

func (f *gatedWALFile) CheckpointStart() { vfsutil.WrapCheckpointStart(f.File) }
func (f *gatedWALFile) CheckpointDone()  { vfsutil.WrapCheckpointDone(f.File) }

func (f *gatedWALFile) Pragma(name, value string) (string, error) {
	return vfsutil.WrapPragma(f.File, name, value)
}

func (f *gatedWALFile) BusyHandler(handler func() bool) { vfsutil.WrapBusyHandler(f.File, handler) }

func (f *gatedWALFile) SharedMemory() sqlite3vfs.SharedMemory {
	return vfsutil.WrapSharedMemory(f.File)
}

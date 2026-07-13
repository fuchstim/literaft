package vfs

import (
	"errors"
	"fmt"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

// File wraps a base sqlite3vfs.File, tagging it with its FileType.
//
// Required File methods (Close/ReadAt/Truncate/Sync/Size/Lock/Unlock/
// CheckReservedLock/SectorSize/DeviceCharacteristics) are promoted unchanged
// from the embedded base. WriteAt is overridden to intercept the commit
// frame on the WAL file.
type File struct {
	sqlite3vfs.File
	kind FileType

	// WAL commit-frame interception (FileTypeWAL only). pending holds a
	// frame header already parsed but not yet paired with its page-image
	// write; capture accumulates (pgno, page) pairs for the write
	// transaction currently in flight on this file.
	gate    Gate
	pending *pendingFrame
	capture []*Frame

	// pageSize is the real, cluster-wide fixed SQLite page size (never 0).
	// It's used both to compute frame-header offsets and to validate a
	// captured frame's page-image length. A wrong value here doesn't just
	// weaken the length check -- it desyncs offset computation, silently
	// corrupting frame parsing for every write after that point.
	pageSize uint32

	// headerPgno and dataOffsets record, for the transaction currently
	// accumulating in capture, which WAL offsets already hold a captured
	// frame's header (offset -> pgno) or page image (offset -> index into
	// capture). SQLite can revisit both after a spill: a re-dirtied
	// already-spilled page is overwritten in place with a bare page-only
	// write (no header), and committing can rewrite every affected frame's
	// header a second time -- including the commit frame's own -- purely
	// to fix the cumulative checksum chain, with no paired data write.
	// Both must be recognized as revisits of an already-captured frame,
	// not new ones, or the transaction ends up under-captured, or a
	// genuine commit frame gets mistaken for a checksum-only rewrite and
	// never reaches the gate.
	//
	// Both maps hold only the in-flight (or just-finished) transaction's
	// own offsets, cleared on the first header write that no longer fits:
	// a fresh offset once txnDone (the checksum rewrite, if any, already
	// ran), or any offset <= maxOffset (a checkpoint restart, or a
	// transaction that spilled frames and rolled back without ever setting
	// txnDone).
	//
	// An offset+pgno match only proves a genuine checksum rewrite -- as
	// opposed to a new frame coincidentally landing on a stale offset --
	// while txnDone is still true, since that rewrite can only happen
	// synchronously right after this transaction's own commit frame was
	// processed. Once txnDone is false, any offset match is coincidence,
	// not evidence of a rewrite. This is why a failed gate proposal must
	// reset both maps and txnDone immediately, not just on the next header
	// write: a rejected commit's offsets are exactly as stale as a
	// rolled-back transaction's.
	headerPgno  map[int64]uint32
	dataOffsets map[int64]int
	maxOffset   int64
	txnDone     bool
}

// pendingFrame is a frame header seen by writeFrameHeader, held until the
// paired page-image write arrives at writeFrameData.
type pendingFrame struct {
	header    frameHeader
	headerRaw [frameHeaderSize]byte
	offset    int64
}

func wrapFile(base sqlite3vfs.File, kind FileType, gate Gate, pageSize uint32) *File {
	return &File{File: base, kind: kind, gate: gate, pageSize: pageSize}
}

// WriteAt implements sqlite3vfs.File. On the WAL file it intercepts the
// commit frame; every other file, and every non-frame WAL write, passes
// straight through.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if f.kind != FileTypeWAL {
		return f.File.WriteAt(p, off)
	}

	if off == 0 {
		// Don't intercept WAL header
		return f.File.WriteAt(p, off)
	}

	switch {
	case f.pending != nil:
		return f.writeFrameData(p, off)
	case f.isFrameHeaderOffset(off):
		h := parseFrameHeader(p)
		if f.txnDone {
			// A rewrite of this transaction's own frame (same offset,
			// same pgno) can only be genuine while this window is open;
			// once closed, a matching offset+pgno is coincidence.
			if pgno, seen := f.headerPgno[off]; seen && pgno == h.pgno {
				// The checksum bytes differ, pgno and commit marker don't:
				// write through as-is, without restarting the header/data
				// pairing state machine or re-proposing to the gate.
				return f.File.WriteAt(p, off)
			}
		}
		if f.txnDone || off <= f.maxOffset {
			// Either the prior transaction's checksum-rewrite window is
			// closed (this must be a new transaction), or the WAL rewound
			// (a checkpoint restart, or a rolled-back spill that never
			// set txnDone). Either way this offset's tracked history no
			// longer applies.
			f.headerPgno = nil
			f.dataOffsets = nil
			f.capture = nil
			f.txnDone = false
		}
		f.maxOffset = off
		return f.writeFrameHeader(h, p, off)
	default:
		if idx, seen := f.dataOffsets[off]; seen && idx < len(f.capture) {
			// Same-transaction page re-dirty after an earlier spill: a
			// bare page-sized overwrite of an already-captured frame's
			// data, no header rewrite. Update that frame's captured image
			// in place instead of silently letting it pass through
			// uncaptured.
			f.capture[idx].Page = append([]byte(nil), p...)
		}
		return f.File.WriteAt(p, off)
	}
}

// isFrameHeaderOffset reports whether off is where a frame header must
// start, per the WAL's fixed frame layout: walHeaderSize +
// n*(frameHeaderSize+page size), n = 0, 1, 2, etc.
func (f *File) isFrameHeaderOffset(off int64) bool {
	return off >= walHeaderSize && (off-walHeaderSize)%(frameHeaderSize+int64(f.pageSize)) == 0
}

// writeFrameHeader records a just-seen, already-parsed frame header. A
// non-commit frame is written straight through immediately. The
// commit frame is held back until writeFrameData resolves the gate.
func (f *File) writeFrameHeader(h frameHeader, p []byte, off int64) (int, error) {
	if !h.isCommit() {
		if n, err := f.File.WriteAt(p, off); err != nil {
			return n, err
		}
	}

	if f.headerPgno == nil {
		f.headerPgno = make(map[int64]uint32)
	}
	f.headerPgno[off] = h.pgno

	pending := &pendingFrame{header: h, offset: off}
	copy(pending.headerRaw[:], p)
	f.pending = pending
	return len(p), nil
}

// writeFrameData completes the pending frame with its page image. For a
// non-commit frame it just passes the write through and extends the
// capture buffer. For the commit frame it proposes the whole captured
// transaction to the gate, then either releases the withheld header and
// this write to disk, or, on gate failure, discards both -- so a rejected
// transaction never leaves a valid commit frame on disk.
func (f *File) writeFrameData(p []byte, off int64) (int, error) {
	pending := f.pending
	f.pending = nil

	if len(p) != int(f.pageSize) {
		// This write is never captured or proposed, so leaving
		// txnDone/headerPgno stale would let a same-offset retry be
		// mistaken for a checksum rewrite and bypass the gate.
		f.headerPgno = nil
		f.dataOffsets = nil
		f.txnDone = false
		err := fmt.Errorf("frame page image is %d bytes, want configured page size %d", len(p), f.pageSize)
		return 0, sqlite3vfs.SystemError(err, sqlite3.IOERR_WRITE)
	}

	if f.dataOffsets == nil {
		f.dataOffsets = make(map[int64]int)
	}
	f.dataOffsets[off] = len(f.capture)
	f.capture = append(f.capture, &Frame{
		Pgno: pending.header.pgno,
		Page: append([]byte(nil), p...),
	})

	if !pending.header.isCommit() {
		return f.File.WriteAt(p, off)
	}

	frames, nTruncate := f.capture, pending.header.nTruncate
	f.capture = nil

	if err := f.gate.ProposeTransaction(frames, nTruncate); err != nil {
		// Rejected: this transaction never committed, so no checksum
		// rewrite window opens for it. headerPgno/dataOffsets must not
		// survive into the next write, or a retry landing on the same
		// offset+pgno could pass straight through as a same-transaction
		// rewrite -- bypassing both capture and the gate, which for a
		// follower-local retry means silent acceptance with zero RAFT
		// involvement.
		f.headerPgno = nil
		f.dataOffsets = nil
		f.txnDone = false

		code := sqlite3.IOERR_WRITE
		gateErr := &gateError{}
		if errors.As(err, &gateErr) {
			if gateErr.code != 0 {
				code = gateErr.code
			}
		}

		// Wrap, don't discard: err may carry a leader-redirect hint or
		// retriability that a bare result code would lose. This is also
		// not a fully reliable channel -- SQLite's automatic rollback
		// after a failed commit can trigger further VFS calls that
		// overwrite it before it reaches the caller.
		return 0, sqlite3vfs.SystemError(err, code)
	}

	// headerPgno/dataOffsets stay alive: a checksum rewrite, if one
	// happens, targets these exact offsets next, still within this same
	// WriteAt sequence. txnDone marks that the next unseen (or
	// pgno-mismatched) header offset belongs to a new transaction.
	f.txnDone = true

	// Past this point the gate has committed the transaction cluster-wide,
	// and this node's own FSM.Apply has already consumed its skip marker
	// (hraft delivers each committed index exactly once). If flushing the
	// withheld commit frame now fails, SQLite rolls the transaction back
	// locally and mxFrame never advances -- but the entry is committed for
	// the cluster and will never be redelivered to this node's Apply, so it
	// would silently and permanently lack its own committed write while
	// continuing to serve reads and propose new writes on the deficient
	// state. There is no local recovery: fail fatally, matching FSM.Apply's
	// contract, so the process restarts and reconverges via hraft's
	// snapshot-restore + log replay (both carry full page images).
	//
	// SQLite's own subsequent publish (growing the wal-index in
	// walIndexAppend, then advancing mxFrame in walIndexWriteHdr) can fail
	// the same way, but runs inside the engine through the opaque
	// SharedMemory interface with no VFS callback at that point, so it can't
	// be escalated here; those paths are memory writes into an
	// already-mapped shm and a boundary-only mmap extend, far less likely
	// than a disk write.
	if _, err := f.File.WriteAt(pending.headerRaw[:], pending.offset); err != nil {
		panic(fmt.Sprintf("vfs: failed to flush committed commit-frame header at WAL offset %d after RAFT commit: %v", pending.offset, err))
	}
	n, err := f.File.WriteAt(p, off)
	if err != nil {
		panic(fmt.Sprintf("vfs: failed to flush committed commit-frame page image at WAL offset %d after RAFT commit: %v", off, err))
	}
	return n, nil
}

var (
	_ sqlite3vfs.File                   = (*File)(nil)
	_ sqlite3vfs.FileUnwrap             = (*File)(nil)
	_ sqlite3vfs.FileLockState          = (*File)(nil)
	_ sqlite3vfs.FilePersistWAL         = (*File)(nil)
	_ sqlite3vfs.FilePowersafeOverwrite = (*File)(nil)
	_ sqlite3vfs.FileChunkSize          = (*File)(nil)
	_ sqlite3vfs.FileSizeHint           = (*File)(nil)
	_ sqlite3vfs.FileHasMoved           = (*File)(nil)
	_ sqlite3vfs.FileOverwrite          = (*File)(nil)
	_ sqlite3vfs.FileSync               = (*File)(nil)
	_ sqlite3vfs.FileCommitPhaseTwo     = (*File)(nil)
	_ sqlite3vfs.FileBatchAtomicWrite   = (*File)(nil)
	_ sqlite3vfs.FileCheckpoint         = (*File)(nil)
	_ sqlite3vfs.FilePragma             = (*File)(nil)
	_ sqlite3vfs.FileBusyHandler        = (*File)(nil)
	_ sqlite3vfs.FileSharedMemory       = (*File)(nil)
)

func (f *File) Unwrap() sqlite3vfs.File { return f.File }

func (f *File) LockState() sqlite3vfs.LockLevel {
	return vfsutil.WrapLockState(f.File)
}

func (f *File) PersistWAL() bool { return vfsutil.WrapPersistWAL(f.File) }
func (f *File) SetPersistWAL(keepWAL bool) {
	vfsutil.WrapSetPersistWAL(f.File, keepWAL)
}

func (f *File) PowersafeOverwrite() bool { return vfsutil.WrapPowersafeOverwrite(f.File) }
func (f *File) SetPowersafeOverwrite(psow bool) {
	vfsutil.WrapSetPowersafeOverwrite(f.File, psow)
}

func (f *File) ChunkSize(size int) { vfsutil.WrapChunkSize(f.File, size) }

func (f *File) SizeHint(size int64) error { return vfsutil.WrapSizeHint(f.File, size) }

func (f *File) HasMoved() (bool, error) { return vfsutil.WrapHasMoved(f.File) }

func (f *File) Overwrite() error { return vfsutil.WrapOverwrite(f.File) }

func (f *File) SyncSuper(super string) error { return vfsutil.WrapSyncSuper(f.File, super) }

func (f *File) CommitPhaseTwo() error { return vfsutil.WrapCommitPhaseTwo(f.File) }

func (f *File) BeginAtomicWrite() error    { return vfsutil.WrapBeginAtomicWrite(f.File) }
func (f *File) CommitAtomicWrite() error   { return vfsutil.WrapCommitAtomicWrite(f.File) }
func (f *File) RollbackAtomicWrite() error { return vfsutil.WrapRollbackAtomicWrite(f.File) }

func (f *File) CheckpointStart() { vfsutil.WrapCheckpointStart(f.File) }
func (f *File) CheckpointDone()  { vfsutil.WrapCheckpointDone(f.File) }

func (f *File) Pragma(name, value string) (string, error) {
	return vfsutil.WrapPragma(f.File, name, value)
}

func (f *File) BusyHandler(handler func() bool) { vfsutil.WrapBusyHandler(f.File, handler) }

func (f *File) SharedMemory() sqlite3vfs.SharedMemory {
	return vfsutil.WrapSharedMemory(f.File)
}

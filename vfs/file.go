package vfs

import (
	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

// File wraps a base sqlite3vfs.File, tagging it with its FileType.
//
// Required File methods (Close/ReadAt/Truncate/Sync/Size/Lock/Unlock/
// CheckReservedLock/SectorSize/DeviceCharacteristics) are promoted unchanged
// from the embedded base. WriteAt is overridden below to intercept the
// commit frame on the WAL file (docs/DESIGN.md §write path). The optional
// capability interfaces don't promote through an embedded interface, so
// each is forwarded explicitly below via vfsutil, mirroring the wrapping
// pattern used by ncruces' own vfs/adiantum and vfs/xts.
type File struct {
	sqlite3vfs.File
	kind FileType

	// WAL commit-frame interception (FileTypeWAL only). pending holds a
	// frame header already parsed but not yet paired with its page-image
	// write; capture accumulates (pgno, page) pairs for the write
	// transaction currently in flight on this file.
	gate    Gate
	pending *pendingFrame
	capture []Frame

	// headerOffsets and dataOffsets record, for the transaction currently
	// accumulating in capture, which WAL offsets already hold a captured
	// frame's header and page-image respectively (data offset -> index
	// into capture). wal.c's walFrames() can revisit both after an
	// earlier spill: re-dirtying an already-spilled page overwrites its
	// data in place with a bare page-sized write (no header, since the
	// header doesn't change), and committing such a transaction runs
	// walRewriteChecksums(), which rewrites every affected frame's header
	// -- including the just-written commit frame's own -- a second time
	// with no paired data write, purely to fix the cumulative checksum
	// chain. Both must be recognized as revisits of an already-captured
	// frame, not new ones: missing the first silently under-captures the
	// transaction, and missing the second desyncs the header/data pairing
	// state machine, corrupting whatever write follows (docs/WAL_FORMAT.md,
	// wal.c walFrames/walRewriteChecksums).
	//
	// Both maps hold only the in-flight transaction's own offsets, so a
	// header at a fresh (never-yet-recorded) offset always starts a new
	// frame in this transaction; that offset is set to be reused after
	// commit, but not before a genuinely new header proves the previous
	// transaction's rewrite phase (which never touches offsets beyond its
	// own commit frame) is over -- cleared lazily on that write. maxOffset
	// also lets a header reused at or below any previously-seen offset
	// (WAL checkpoint restart, or a rolled-back transaction's frames being
	// reused) force the same reset, since that can't otherwise be told
	// apart from an in-transaction revisit.
	headerOffsets map[int64]struct{}
	dataOffsets   map[int64]int
	maxOffset     int64
	txnDone       bool
}

// pendingFrame is a frame header seen by writeFrameHeader, held until the
// paired page-image write arrives at writeFrameData.
type pendingFrame struct {
	header    frameHeader
	headerRaw [frameHeaderSize]byte
	offset    int64
}

func wrapFile(base sqlite3vfs.File, kind FileType, gate Gate) *File {
	return &File{File: base, kind: kind, gate: gate}
}

// Kind reports which SQLite file this wraps.
func (f *File) Kind() FileType { return f.kind }

// WriteAt implements sqlite3vfs.File. On the WAL file it intercepts the
// commit frame; every other file, and every non-frame WAL write, passes
// straight through (docs/DESIGN.md §write path, docs/WAL_FORMAT.md).
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if f.kind != FileTypeWAL {
		return f.File.WriteAt(p, off)
	}

	switch {
	case f.pending != nil:
		return f.writeFrameData(p, off)
	case len(p) == frameHeaderSize:
		if _, seen := f.headerOffsets[off]; seen {
			// walRewriteChecksums rewriting an already-captured frame's
			// header to fix the checksum chain: pgno and commit marker
			// are unchanged (re-read from disk), only the checksum bytes
			// differ. Not a new frame -- write through as-is, without
			// restarting the header/data pairing state machine or
			// re-proposing to the gate.
			return f.File.WriteAt(p, off)
		}
		if f.txnDone || off <= f.maxOffset {
			// Either the previous transaction's commit-frame pairing
			// already resolved (so this offset, being unseen, must start
			// a new transaction -- its own rewrite phase, if any, runs
			// synchronously before any new transaction's frames), or the
			// WAL rewound (checkpoint restart, or a rolled-back
			// transaction's offsets reused). Either way this offset's
			// history no longer applies.
			f.headerOffsets = nil
			f.dataOffsets = nil
			f.txnDone = false
		}
		f.maxOffset = off
		return f.writeFrameHeader(p, off)
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

// writeFrameHeader records a just-seen frame header. A non-commit frame is
// written straight through immediately (docs/DESIGN.md §write path step 2,
// non-commit frames are safe to write through). The commit frame is held
// back until writeFrameData resolves the gate.
func (f *File) writeFrameHeader(p []byte, off int64) (int, error) {
	h := parseFrameHeader(p)

	if !h.isCommit() {
		if n, err := f.File.WriteAt(p, off); err != nil {
			return n, err
		}
	}

	if f.headerOffsets == nil {
		f.headerOffsets = make(map[int64]struct{})
	}
	f.headerOffsets[off] = struct{}{}

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
// transaction never leaves a valid commit frame on disk (docs/DESIGN.md
// §write path steps 3-5).
func (f *File) writeFrameData(p []byte, off int64) (int, error) {
	pending := f.pending
	f.pending = nil

	if f.dataOffsets == nil {
		f.dataOffsets = make(map[int64]int)
	}
	f.dataOffsets[off] = len(f.capture)
	f.capture = append(f.capture, Frame{
		Pgno: pending.header.pgno,
		Page: append([]byte(nil), p...),
	})

	if !pending.header.isCommit() {
		return f.File.WriteAt(p, off)
	}

	entry := Entry{Frames: f.capture, NTruncate: pending.header.nTruncate}
	f.capture = nil
	// headerOffsets/dataOffsets stay alive: walRewriteChecksums, if it
	// runs, rewrites frame headers at these exact offsets next, still
	// inside this same WriteAt sequence. txnDone marks that the next
	// unseen header offset belongs to a new transaction, safe to clear
	// them for.
	f.txnDone = true

	if err := f.gate.Propose(entry); err != nil {
		return 0, sqlite3.IOERR_WRITE
	}

	if _, err := f.File.WriteAt(pending.headerRaw[:], pending.offset); err != nil {
		return 0, err
	}
	return f.File.WriteAt(p, off)
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

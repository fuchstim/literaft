package vfs

import (
	"fmt"

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

	// pageSize enforces CLAUDE.md's "fixed cluster-wide page size" on
	// captured page images, catching a mismatch on the leader before it's
	// ever proposed to RAFT rather than only after a follower's
	// apply.Applier.Apply rejects it post-commit. 0 disables the check
	// (M0-M2's default registration, and any test not opting in).
	pageSize uint32

	// walPageSize is the page size actually in effect for this WAL, used to
	// tell a frame header write from a page-image write by offset alone
	// (walHeaderSize + n*(frameHeaderSize+walPageSize) is always a header).
	// Unlike pageSize above it's never optional: it's read from the WAL
	// header itself (docs/WAL_FORMAT.md) the first time it's needed, either
	// because WriteAt just saw offset 0 written, or, lazily, straight off
	// disk in isFrameHeaderOffset if this handle's own WriteAt never saw
	// that write -- e.g. a connection reopening a WAL-mode db whose WAL a
	// prior connection left non-empty on close. 0 until known.
	walPageSize uint32

	// headerPgno and dataOffsets record, for the transaction currently
	// accumulating in capture, which WAL offsets already hold a captured
	// frame's header (offset -> that frame's pgno) and page-image (offset
	// -> index into capture) respectively. wal.c's walFrames() can revisit
	// both after an earlier spill: re-dirtying an already-spilled page
	// overwrites its data in place with a bare page-sized write (no
	// header, since the header doesn't change), and committing such a
	// transaction runs walRewriteChecksums(), which rewrites every
	// affected frame's header -- including the just-written commit
	// frame's own -- a second time with no paired data write, purely to
	// fix the cumulative checksum chain. Both must be recognized as
	// revisits of an already-captured frame, not new ones: missing the
	// first silently under-captures the transaction, and missing the
	// second desyncs the header/data pairing state machine so badly that
	// a genuine commit frame can be mistaken for a checksum-only rewrite
	// and never reach the gate at all (docs/WAL_FORMAT.md, wal.c
	// walFrames/walRewriteChecksums).
	//
	// Both maps hold only the in-flight (or just-finished, mid-rewrite)
	// transaction's own offsets, cleared lazily on the first header write
	// that proves they no longer apply -- either a fresh offset once
	// txnDone (walRewriteChecksums, if any, ran synchronously and is
	// over), or any offset <= maxOffset (WAL checkpoint restart, or a
	// transaction that spilled frames and then rolled back without ever
	// setting txnDone: sqlite3WalUndo only reverts mxFrame back to where
	// this connection's write transaction began, so the next
	// transaction's first frame can land exactly on the aborted one's
	// first frame's offset, not before it).
	//
	// A recorded offset+pgno match only proves a genuine walRewriteChecksums
	// rewrite -- as opposed to a new frame coincidentally landing on an old,
	// stale offset -- while txnDone is still true, since that rewrite can
	// only happen synchronously right after this same transaction's own
	// commit frame was processed (wal.c only calls it from within
	// walFrames(), guarded by isCommit). Once txnDone is false, either the
	// rewrite window is over (fresh offset, unrelated to any past pgno) or
	// there never was one to begin with (rollback), so any offset match is
	// coincidence, not evidence of a rewrite. This is why writeFrameData
	// must reset both maps and txnDone the instant gate.Propose fails,
	// rather than only on the next header write: a rejected commit's
	// offsets (and possibly pgnos, by coincidence with a same-shape retry)
	// are exactly as stale as a rolled-back transaction's, and the
	// walRewriteChecksums window never opens for a transaction that never
	// committed.
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
// straight through (docs/DESIGN.md §write path, docs/WAL_FORMAT.md).
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if f.kind != FileTypeWAL {
		return f.File.WriteAt(p, off)
	}

	if off == 0 {
		// The WAL header (docs/WAL_FORMAT.md) -- the only thing ever
		// written at offset 0 of a WAL file. Captured, never intercepted:
		// it's what isFrameHeaderOffset needs to locate frame boundaries by
		// offset for the rest of this WAL epoch.
		if len(p) >= 12 {
			f.walPageSize = walHeaderPageSize(p)
		}
		return f.File.WriteAt(p, off)
	}

	switch {
	case f.pending != nil:
		return f.writeFrameData(p, off)
	case f.isFrameHeaderOffset(off):
		h := parseFrameHeader(p)
		if f.txnDone {
			// walRewriteChecksums only ever runs synchronously right
			// after this connection's own commit frame was processed, so
			// a rewrite of one of its frames (same offset, same pgno) can
			// only be genuine while that window is still open. Outside
			// it, a matching offset+pgno is coincidence, not a rewrite --
			// see below.
			if pgno, seen := f.headerPgno[off]; seen && pgno == h.pgno {
				// The checksum bytes differ, pgno and commit marker don't:
				// write through as-is, without restarting the header/data
				// pairing state machine or re-proposing to the gate.
				return f.File.WriteAt(p, off)
			}
		}
		if f.txnDone || off <= f.maxOffset {
			// Either the previous transaction's commit-frame pairing
			// already resolved and this write is no longer one of its
			// checksum rewrites (so it must start a new transaction --
			// walRewriteChecksums never touches offsets beyond its own
			// commit frame), or the WAL rewound (checkpoint restart), or a
			// transaction that spilled frames and then rolled back
			// without ever setting txnDone left this offset (and possibly
			// this exact pgno, by coincidence) tracked with nothing to
			// legitimately rewrite it. In every case this offset's
			// history no longer applies.
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
// n*(frameHeaderSize+page size), n = 0, 1, 2, ... (docs/WAL_FORMAT.md). It
// resolves walPageSize lazily via a direct read of the underlying file's own
// header if this handle's WriteAt has never seen offset 0 written -- true
// for the very first WAL write of a freshly created/reset WAL, but not for a
// connection that reopens a WAL a prior connection left non-empty.
func (f *File) isFrameHeaderOffset(off int64) bool {
	if f.walPageSize == 0 {
		var hdr [12]byte
		if _, err := f.File.ReadAt(hdr[:], 0); err != nil {
			return false
		}
		f.walPageSize = walHeaderPageSize(hdr[:])
	}
	return off >= walHeaderSize && (off-walHeaderSize)%(frameHeaderSize+int64(f.walPageSize)) == 0
}

// writeFrameHeader records a just-seen, already-parsed frame header. A
// non-commit frame is written straight through immediately (docs/DESIGN.md
// §write path step 2, non-commit frames are safe to write through). The
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
// transaction never leaves a valid commit frame on disk (docs/DESIGN.md
// §write path steps 3-5).
func (f *File) writeFrameData(p []byte, off int64) (int, error) {
	pending := f.pending
	f.pending = nil

	if f.pageSize != 0 && len(p) != int(f.pageSize) {
		// Same reset as a gate rejection (writeFrameData's Propose-error
		// branch below): this write is never captured or proposed, so
		// leaving txnDone/headerPgno stale would expose the same
		// same-offset-retry bypass fixed there.
		f.headerPgno = nil
		f.dataOffsets = nil
		f.txnDone = false
		err := fmt.Errorf("literaft: frame page image is %d bytes, want configured page size %d", len(p), f.pageSize)
		return 0, sqlite3vfs.SystemError(err, sqlite3.IOERR_WRITE)
	}

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

	if err := f.gate.Propose(entry); err != nil {
		// Rejected: no walRewriteChecksums window opens for a transaction
		// that never committed, so headerPgno/dataOffsets must not survive
		// into the next write. Leaving txnDone true (and the old offsets
		// keyed by their now-stale pgnos) would let a retry that happens to
		// land on the same WAL offset with the same pgno -- exactly what
		// docs/DESIGN.md promises happens on retry, since mxFrame never
		// moved -- be mistaken for a same-transaction checksum rewrite and
		// written straight through, bypassing capture and the gate
		// entirely. That's not just a leader-side gate bypass: since the
		// stale-match branch never calls gate.Propose again, it also skips
		// the leader check, so a retried follower-local write could be
		// silently accepted with zero RAFT involvement (violates
		// ADR-004/ADR-007's writes-are-leader-only invariant).
		f.headerPgno = nil
		f.dataOffsets = nil
		f.txnDone = false
		// Wrap, don't discard: gate.Propose can return a *NotLeaderError
		// (with a leader-redirect hint, docs/DESIGN.md's client-redirect
		// flow) or a CatchingUpError, and a bare result code would lose that
		// distinction. sqlite3vfs.SystemError attaches err so it's reachable
		// via errors.As from whatever wraps this return value -- but it is
		// NOT a reliable channel by itself: SQLite's own automatic rollback
		// after a failed commit makes further, successful VFS calls before
		// conn.Exec/Stmt.Step returns to Go, and each one clears the single
		// slot this detail travels through (ncruces' wrp.SysError), win or
		// lose. Confirmed empirically -- a real Exec-driven commit failure
		// reliably loses it before the caller ever sees it. Gate.LastRejection
		// is the mechanism a caller should actually use to recover which
		// error this was; this wrap is attempted anyway since it's free and
		// occasionally survives (e.g. paths that don't trigger an automatic
		// rollback), never because it can be relied on alone.
		return 0, sqlite3vfs.SystemError(err, sqlite3.IOERR_WRITE)
	}

	// headerPgno/dataOffsets stay alive: walRewriteChecksums, if it runs,
	// rewrites frame headers at these exact offsets next, still inside
	// this same WriteAt sequence. txnDone marks that the next unseen (or
	// pgno-mismatched) header offset belongs to a new transaction, safe
	// to clear them for.
	f.txnDone = true

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

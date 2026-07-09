package walappender

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/fuchstim/literaft/internal/fsm/walappender/shm"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/ncruces/go-sqlite3"
)

const (
	walHeaderSize   = 32
	frameHeaderSize = 24
	walMagicLE      = 0x377f0682 // low bit 0: checksums are little-endian on this (wasm) engine
	walMaxVersion   = 3007000
)

type WALAppender struct {
	pageSize uint32
	db       *sqlite3.Conn
	f        *os.File
	shm      *shm.SharedMemory

	checkpointTicker         *time.Ticker
	checkpointThresholdPages int
	dirtyPageCount           int
}

// Open opens (creating if necessary) the -wal and -shm files alongside the
// database at dbPath. pageSize is the cluster-wide fixed page size.
// A passive checkpoint is performed after checkpointThresholdPages have been committed to the WAL,
// and on every checkpointInterval.
//
// WALAppender opens and maintains a single DB connection to prevent the WAL/SHM from being deleted.
// This connection is also used to perform passive checkpoints, preventing unbounded WAL growth on
// followers that are not performing any writes (outside of WALAppender).
//
// A wal-index header with pageSize == 0 is treated as uninitialized (see
// bootstrap) even though it otherwise looks structurally valid: real
// SQLite can leave exactly this behind for a WAL-mode db that's had
// journal_mode=WAL enabled but never had an actual write transaction, and
// trusting it would chain every frame's checksum from a salt real readers
// never see written into the -wal file's own header.
func Open(dbPath string, pageSize uint32, checkpointThresholdPages int, checkpointInterval time.Duration) (*WALAppender, error) {
	db, err := sqlite3.Open("file:" + dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at path `%s`: %w", dbPath, err)
	}

	f, err := os.OpenFile(dbPath+"-wal", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	sm, err := shm.Open(dbPath + "-shm")
	if err != nil {
		f.Close()
		return nil, err
	}

	w := &WALAppender{
		pageSize:                 pageSize,
		db:                       db,
		f:                        f,
		shm:                      sm,
		checkpointThresholdPages: checkpointThresholdPages,
	}
	if err := w.maybeBootstrap(); err != nil {
		return nil, fmt.Errorf("failed to bootstrap WAL: %w", err)
	}
	go w.runCheckpointer(checkpointInterval)

	return w, nil
}

func (a *WALAppender) Close() error {
	if a.checkpointTicker != nil {
		a.checkpointTicker.Stop()
	}

	return errors.Join(a.db.Close(), a.f.Close(), a.shm.Close())
}

// AppendEntry appends e's frames to the local -wal
func (a *WALAppender) AppendEntry(e *raftproto.Entry) error {
	frames := make([]*Frame, len(e.Frames))
	for i, f := range e.Frames {
		var nTruncate uint32
		if i == len(e.Frames)-1 {
			nTruncate = e.NTruncate
		}
		frames[i] = NewFrame(f.Pgno, nTruncate, f.Page)
	}

	return a.AppendFrames(frames)
}

// AppendFrames appends fs to the local -wal
// (computing this node's own running checksums, chained
// from whatever's already there), updates the wal-index page-map hash
// slots, and finally advances mxFrame with the tear-safe
// two-copy header write. AppendFrames
// takes WAL_WRITE_LOCK for its own duration and releases it before
// returning, whether it succeeds or fails.
func (a *WALAppender) AppendFrames(fs []*Frame) error {
	if err := a.shm.Lock(shm.WriteLock); err != nil {
		return fmt.Errorf("failed to acquire WAL_WRITE_LOCK: %w", err)
	}
	defer a.shm.Unlock(shm.WriteLock)

	region0, err := a.shm.Region(0)
	if err != nil {
		return fmt.Errorf("failed to map wal-index header page: %w", err)
	}

	hdr, ok := readWALIndexHeader(region0)
	if !ok {
		return fmt.Errorf("failed to read wal-index header page")
	}

	frame := hdr.maxFrame
	offset := walHeaderSize + int64(frame)*(frameHeaderSize+int64(a.pageSize))
	cksum := hdr.frameCksum

	for _, f := range fs {
		if len(f.page) != int(a.pageSize) {
			return fmt.Errorf("frame for page %d is %d bytes, cluster page size is %d bytes",
				f.pgNo, len(f.page), a.pageSize)
		}

		var fh [frameHeaderSize]byte
		fh, cksum = f.encodeHeader(hdr.salt, cksum)

		frame++
		if _, err := a.f.WriteAt(fh[:], offset); err != nil {
			return fmt.Errorf("failed to write header for frame frame %d: %w", frame, err)
		}
		if _, err := a.f.WriteAt(f.page, offset+frameHeaderSize); err != nil {
			return fmt.Errorf("failed to write data for frame %d: %w", frame, err)
		}
		offset += frameHeaderSize + int64(a.pageSize)

		region, err := a.shm.Region(framePage(frame))
		if err != nil {
			return fmt.Errorf("failed to map wal-index page for frame %d: %w", frame, err)
		}
		addFrameToWALIndex(region, frame, f.pgNo)

		a.dirtyPageCount++

		if f.nTruncate != 0 {
			hdr.maxFrame = frame
			hdr.nPage = f.nTruncate
			hdr.frameCksum = cksum
			hdr.change++
			writeWALIndexHeader(region0, hdr)

			if a.dirtyPageCount >= a.checkpointThresholdPages {
				a.checkpoint()
			}
		}
	}

	return nil
}

// maybeBootstrap initializes the -wal file and wal-index header if they aren't already initialized.
// It returns an error if the -wal file already has content but the wal-index is uninitialized,
// since recovery from an existing WAL isn't implemented yet.
func (a *WALAppender) maybeBootstrap() error {
	region0, err := a.shm.Region(0)
	if err != nil {
		return fmt.Errorf("failed to map wal-index header page: %w", err)
	}

	hdr, ok := readWALIndexHeader(region0)
	// A WAL-mode db that's had journal_mode=WAL enabled but never had an
	// actual write transaction can leave a structurally valid, "init"
	// wal-index header behind (real SQLite's own doing, establishing the
	// header lazily on connect) whose pageSize/salt were never actually
	// populated -- a real header with none of the real content, distinct
	// from readHeader's own "freshly zeroed region" case. Trusting it as-is
	// would make every applied frame carry a zero salt while the -wal
	// file's own on-disk header is never written (bootstrap never runs),
	// which readers reject outright. Treat it the same as uninitialized.
	if ok && hdr.pageSize == 0 {
		ok = false
	}

	if ok { // Already bootstrapped
		return nil
	}

	fi, err := a.f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat -wal file: %w", err)
	}
	if fi.Size() != 0 {
		return fmt.Errorf("-wal file already has %d bytes but the wal-index is uninitialized; "+
			"recovery from an existing WAL isn't implemented yet", fi.Size())
	}

	var salt [saltSize]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return fmt.Errorf("failed to generate random salt for WAL header: %w", err)
	}

	var walHdr [walHeaderSize]byte
	binary.BigEndian.PutUint32(walHdr[0:4], walMagicLE)
	binary.BigEndian.PutUint32(walHdr[4:8], walMaxVersion)
	binary.BigEndian.PutUint32(walHdr[8:12], a.pageSize)
	// bytes 12:16 (checkpoint sequence number) start at 0.
	copy(walHdr[16:24], salt[:])

	s0, s1 := checksum(0, 0, walHdr[:24])
	binary.BigEndian.PutUint32(walHdr[24:28], s0)
	binary.BigEndian.PutUint32(walHdr[28:32], s1)
	if _, err := a.f.WriteAt(walHdr[:], 0); err != nil {
		return fmt.Errorf("failed to write -wal file header: %w", err)
	}

	pageSize16 := uint16(a.pageSize)
	if a.pageSize == 65536 {
		pageSize16 = 1 // SQLite's szPage field encodes 64K pages as 1.
	}

	h := walIndexHeader{
		version:     walMaxVersion,
		init:        true,
		bigEndCksum: false,
		pageSize:    pageSize16,
		maxFrame:    0,
		nPage:       0,
		frameCksum:  [2]uint32{s0, s1}, // frame 1's checksum chains from the WAL header's own checksum
		salt:        salt,
	}
	writeWALIndexHeader(region0, h)

	return nil
}

func (a *WALAppender) runCheckpointer(interval time.Duration) {
	if interval <= 0 {
		return
	}

	a.checkpointTicker = time.NewTicker(interval)
	for range a.checkpointTicker.C {
		a.checkpoint()
	}
}

func (a *WALAppender) checkpoint() {
	a.db.WALCheckpoint("main", sqlite3.CHECKPOINT_PASSIVE)
}

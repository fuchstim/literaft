// Package apply materializes RAFT log entries captured by vfs's
// commit-frame gate into a follower's local -wal file and wal-index,
// without going through a SQLite connection at all (docs/DESIGN.md
// §follower-apply). It's the most format-sensitive code in the repo: get a
// byte offset wrong here and every reader -- including external,
// unmodified SQLite processes -- sees a corrupt or torn database. See
// README.md for how the format was derived and verified.
package apply

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/fuchstim/literaft/shm"
	raftvfs "github.com/fuchstim/literaft/vfs"
)

const (
	walHeaderSize   = 32
	frameHeaderSize = 24
	walMagicLE      = 0x377f0682 // low bit 0: checksums are little-endian on this (wasm) engine
	walMaxVersion   = 3007000
)

// Applier materializes captured entries into one follower database's local
// -wal file and wal-index. It holds the wal-index mapped and the -wal file
// open across calls, mirroring a long-lived follower process. Not safe for
// concurrent use: entries must be applied strictly in order (ADR-003).
type Applier struct {
	pageSize uint32
	wal      *os.File
	shm      *shm.SharedMemory
}

// Open opens (creating if necessary) the -wal and -shm files alongside the
// database at dbPath. pageSize is the cluster-wide fixed page size
// (CLAUDE.md invariant): every applied frame's page image must be exactly
// this many bytes.
func Open(dbPath string, pageSize uint32) (*Applier, error) {
	wal, err := os.OpenFile(dbPath+"-wal", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	sm, err := shm.Open(dbPath + "-shm")
	if err != nil {
		wal.Close()
		return nil, err
	}
	return &Applier{pageSize: pageSize, wal: wal, shm: sm}, nil
}

// Close closes the -wal file and unmaps the wal-index.
func (a *Applier) Close() error {
	shmErr := a.shm.Close()
	walErr := a.wal.Close()
	if shmErr != nil {
		return shmErr
	}
	return walErr
}

// Apply materializes one captured write transaction: it appends e's frames
// to the local -wal (computing this node's own running checksums, chained
// from whatever's already there), updates the wal-index page-map hash
// slots for each, and finally advances mxFrame with the tear-safe
// two-copy header write (docs/DESIGN.md §follower-apply steps 1-5). Apply
// takes WAL_WRITE_LOCK for its own duration and releases it before
// returning, whether it succeeds or fails.
//
// A wal-index header with pageSize == 0 is treated as uninitialized (see
// bootstrap) even though it otherwise looks structurally valid: real
// SQLite can leave exactly this behind for a WAL-mode db that's had
// journal_mode=WAL enabled but never had an actual write transaction, and
// trusting it would chain every frame's checksum from a salt real readers
// never see written into the -wal file's own header.
func (a *Applier) Apply(e raftvfs.Entry) error {
	if err := a.shm.Lock(shm.WriteLock); err != nil {
		return fmt.Errorf("apply: acquiring WAL_WRITE_LOCK: %w", err)
	}
	defer a.shm.Unlock(shm.WriteLock)

	region0, err := a.shm.Region(0)
	if err != nil {
		return fmt.Errorf("apply: mapping wal-index header page: %w", err)
	}

	hdr, ok := readHeader(region0)
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
	if !ok {
		hdr, err = a.bootstrap()
		if err != nil {
			return fmt.Errorf("apply: bootstrapping wal: %w", err)
		}
	}

	frame := hdr.maxFrame
	offset := walHeaderSize + int64(frame)*(frameHeaderSize+int64(a.pageSize))
	cksum := hdr.frameCksum

	for i, f := range e.Frames {
		if len(f.Page) != int(a.pageSize) {
			return fmt.Errorf("apply: frame for page %d is %d bytes, want the cluster page size %d",
				f.Pgno, len(f.Page), a.pageSize)
		}

		var nTruncate uint32
		if i == len(e.Frames)-1 {
			nTruncate = e.NTruncate
		}

		var fh [frameHeaderSize]byte
		fh, cksum = encodeFrame(f.Pgno, nTruncate, f.Page, hdr.salt, cksum)

		frame++
		if _, err := a.wal.WriteAt(fh[:], offset); err != nil {
			return fmt.Errorf("apply: writing frame %d header: %w", frame, err)
		}
		if _, err := a.wal.WriteAt(f.Page, offset+frameHeaderSize); err != nil {
			return fmt.Errorf("apply: writing frame %d page image: %w", frame, err)
		}
		offset += frameHeaderSize + int64(a.pageSize)

		region, err := a.shm.Region(framePage(frame))
		if err != nil {
			return fmt.Errorf("apply: mapping wal-index page for frame %d: %w", frame, err)
		}
		insertFrame(region, frame, f.Pgno)
	}

	// Local WAL fsync is skippable: durability comes from the RAFT log
	// quorum, and a crashed node rebuilds its WAL tail from the log via
	// persisted lastApplied (docs/DECISIONS.md ADR-006).
	hdr.maxFrame = frame
	hdr.nPage = e.NTruncate
	hdr.frameCksum = cksum
	hdr.change++
	writeHeader(region0, hdr)
	return nil
}

// bootstrap writes a fresh WAL file header and returns the wal-index
// header a first Apply call should build on. Only valid when the -shm
// wal-index is uninitialized; if the -wal file already has content in
// that state, this is a crash-recovery scenario apply/ doesn't yet handle
// and bootstrap refuses rather than risk clobbering it.
func (a *Applier) bootstrap() (header, error) {
	fi, err := a.wal.Stat()
	if err != nil {
		return header{}, err
	}
	if fi.Size() != 0 {
		return header{}, fmt.Errorf("-wal file already has %d bytes but the wal-index is uninitialized; "+
			"recovery from an existing WAL isn't implemented yet", fi.Size())
	}

	var salt [saltSize]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return header{}, err
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
	if _, err := a.wal.WriteAt(walHdr[:], 0); err != nil {
		return header{}, err
	}

	pageSize16 := uint16(a.pageSize)
	if a.pageSize == 65536 {
		pageSize16 = 1 // SQLite's szPage field encodes 64K pages as 1.
	}
	return header{
		version:     walMaxVersion,
		init:        true,
		bigEndCksum: false,
		pageSize:    pageSize16,
		maxFrame:    0,
		nPage:       0,
		frameCksum:  [2]uint32{s0, s1}, // frame 1's checksum chains from the WAL header's own checksum
		salt:        salt,
	}, nil
}

// encodeFrame builds a 24-byte WAL frame header for page pgno (wal.c's
// walEncodeFrame) and returns the running checksum after it, chaining from
// seed. nTruncate is the post-commit database size in pages, or 0 if this
// isn't the commit frame.
func encodeFrame(pgno, nTruncate uint32, page []byte, salt [saltSize]byte, seed [2]uint32) ([frameHeaderSize]byte, [2]uint32) {
	var fh [frameHeaderSize]byte
	binary.BigEndian.PutUint32(fh[0:4], pgno)
	binary.BigEndian.PutUint32(fh[4:8], nTruncate)
	copy(fh[8:16], salt[:])

	s0, s1 := checksum(seed[0], seed[1], fh[:8])
	s0, s1 = checksum(s0, s1, page)
	binary.BigEndian.PutUint32(fh[16:20], s0)
	binary.BigEndian.PutUint32(fh[20:24], s1)
	return fh, [2]uint32{s0, s1}
}

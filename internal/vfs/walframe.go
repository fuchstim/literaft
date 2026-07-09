package vfs

import "encoding/binary"

const (
	// walHeaderSize is the fixed size of the WAL file header at offset 0
	// (docs/WAL_FORMAT.md). Every frame starts at
	// walHeaderSize + n*(frameHeaderSize+page size), which is what lets
	// WriteAt tell a frame header write from a page-image write by offset
	// alone, without depending on which write came before it.
	walHeaderSize = 32

	// frameHeaderSize is the fixed size of a WAL frame header, in bytes
	// (docs/WAL_FORMAT.md). SQLite's minimum page size (512) rules out any
	// ambiguity between a frame header write, the 32-byte WAL header, and a
	// frame's page-image write.
	frameHeaderSize = 24
)

// frameHeader is the parsed form of a WAL frame header. Salts and
// checksums aren't decoded: the commit-frame gate never needs to recompute
// them, it only ever replays the exact bytes SQLite already wrote.
type frameHeader struct {
	pgno      uint32
	nTruncate uint32 // post-commit db size in pages; 0 unless this is the commit frame
}

// parseFrameHeader decodes a 24-byte WAL frame header. All WAL fields are
// big-endian (docs/WAL_FORMAT.md).
func parseFrameHeader(b []byte) frameHeader {
	return frameHeader{
		pgno:      binary.BigEndian.Uint32(b[0:4]),
		nTruncate: binary.BigEndian.Uint32(b[4:8]),
	}
}

// walHeaderPageSize reads the page-size field (bytes 8-11) of a WAL file
// header. All WAL fields are big-endian (docs/WAL_FORMAT.md).
func walHeaderPageSize(b []byte) uint32 {
	return binary.BigEndian.Uint32(b[8:12])
}

// isCommit reports whether this frame is the transaction's commit frame:
// bytes 4-7 (db size after commit) are non-zero only on a commit frame.
func (h frameHeader) isCommit() bool { return h.nTruncate != 0 }

package vfs

import "encoding/binary"

// frameHeaderSize is the fixed size of a WAL frame header, in bytes
// (docs/WAL_FORMAT.md). SQLite's minimum page size (512) rules out any
// ambiguity between a frame header write, the 32-byte WAL header, and a
// frame's page-image write.
const frameHeaderSize = 24

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

// isCommit reports whether this frame is the transaction's commit frame:
// bytes 4-7 (db size after commit) are non-zero only on a commit frame.
func (h frameHeader) isCommit() bool { return h.nTruncate != 0 }

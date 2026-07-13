package snapshotter

import (
	"encoding/binary"
	"fmt"
	"io"
)

// The snapshot stream carries a small fixed header ahead of the raw database
// bytes so a restoring node can recover the snapshot's raft index (raft does
// not otherwise pass it to a restore). snapshotMagic is deliberately not a
// valid page-1 prefix -- a SQLite page 1 always begins with the 16 bytes
// "SQLite format 3\0" -- so an old, headerless snapshot is rejected rather
// than misread as a page.
const (
	snapshotMagicSize = 16
	snapshotVersion   = uint32(1)
	// headerSize is magic(16) + version(4) + raft index(8).
	headerSize = snapshotMagicSize + 4 + 8
)

var snapshotMagic = [snapshotMagicSize]byte{'L', 'I', 'T', 'E', 'R', 'A', 'F', 'T', '-', 'S', 'N', 'A', 'P', 0, 0, 0}

// encodeHeader builds the fixed snapshot-stream header for a snapshot taken at
// raft index.
func encodeHeader(index uint64) [headerSize]byte {
	var b [headerSize]byte
	copy(b[0:snapshotMagicSize], snapshotMagic[:])
	binary.BigEndian.PutUint32(b[snapshotMagicSize:snapshotMagicSize+4], snapshotVersion)
	binary.BigEndian.PutUint64(b[snapshotMagicSize+4:headerSize], index)
	return b
}

// decodeHeader reads and validates the fixed header from the front of r,
// returning the snapshot's raft index. A missing/short header, a wrong magic
// (including an old headerless snapshot starting with a page-1 prefix), or an
// unrecognized version are all rejected.
func decodeHeader(r io.Reader) (uint64, error) {
	var b [headerSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, fmt.Errorf("failed to read snapshot header: %w", err)
	}
	if [snapshotMagicSize]byte(b[0:snapshotMagicSize]) != snapshotMagic {
		return 0, fmt.Errorf("snapshot is missing the literaft header (old headerless snapshots are not supported)")
	}
	version := binary.BigEndian.Uint32(b[snapshotMagicSize : snapshotMagicSize+4])
	if version != snapshotVersion {
		return 0, fmt.Errorf("unsupported snapshot header version %d (this build writes version %d)", version, snapshotVersion)
	}
	return binary.BigEndian.Uint64(b[snapshotMagicSize+4 : headerSize]), nil
}

package raftproto

import (
	"encoding/binary"
	"fmt"

	"github.com/fuchstim/literaft/internal/vfs"
)

// Entry is a whole write transaction's captured frames, proposed to a Gate
// as a unit when its commit frame is withheld.
type Entry struct {
	NodeID    string
	Frames    []*vfs.Frame
	NTruncate uint32 // post-commit database size in pages
}

// Encode serializes e for a raft.Log's Data field: this is our own
// wire format, unrelated to the WAL's on-disk format -- just a length-prefixed list of (pgno, page) pairs
// plus nTruncate, big-endian for consistency with the rest of the repo's
// manual binary encodings.
func (e *Entry) Encode() []byte {
	size := 4 + len(e.NodeID) + 4 // node id length + node id + frame count
	for _, f := range e.Frames {
		size += 4 + 4 + len(f.Page) // pgno + page length + page
	}
	size += 4 // nTruncate

	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint32(buf[off:], uint32(len(e.NodeID)))
	off += 4
	off += copy(buf[off:], e.NodeID)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(e.Frames)))
	off += 4
	for _, f := range e.Frames {
		binary.BigEndian.PutUint32(buf[off:], f.Pgno)
		off += 4
		binary.BigEndian.PutUint32(buf[off:], uint32(len(f.Page)))
		off += 4
		off += copy(buf[off:], f.Page)
	}
	binary.BigEndian.PutUint32(buf[off:], e.NTruncate)
	off += 4

	return buf[:off]
}

// DecodeEntry is the inverse of Entry.Encode. It returns an error on
// truncated or malformed input rather than panicking, since the bytes cross
// a raft.Log boundary.
func DecodeEntry(b []byte) (*Entry, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("entry too short (%d bytes) to hold node id length", len(b))
	}
	idlen := binary.BigEndian.Uint32(b[0:4])
	off := 4
	if len(b)-off < int(idlen) {
		return nil, fmt.Errorf("entry too short (%d bytes) to hold node id of length %d", len(b), idlen)
	}
	nodeID := string(b[off : off+int(idlen)])
	off += int(idlen)

	if len(b)-off < 4 {
		return nil, fmt.Errorf("entry too short (%d bytes) to hold a frame count", len(b))
	}
	count := binary.BigEndian.Uint32(b[off:])
	off += 4

	// Bound count against the buffer before trusting it as a make() capacity:
	// every frame needs at least 8 bytes (pgno + page length), so a corrupted
	// or truncated entry claiming far more frames than b could possibly hold
	// must fail cleanly here rather than attempt a multi-gigabyte allocation.
	if maxFrames := uint32(len(b)-off) / 8; count > maxFrames {
		return nil, fmt.Errorf("entry too short (%d bytes) to hold %d frames", len(b), count)
	}

	frames := make([]*vfs.Frame, 0, count)
	for i := range count {
		if len(b)-off < 8 {
			return nil, fmt.Errorf("entry truncated reading frame %d header", i)
		}
		pgno := binary.BigEndian.Uint32(b[off:])
		off += 4
		pageLen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if uint32(len(b)-off) < pageLen {
			return nil, fmt.Errorf("entry truncated reading frame %d page (want %d bytes)", i, pageLen)
		}
		page := append([]byte(nil), b[off:off+int(pageLen)]...)
		off += int(pageLen)
		frames = append(frames, &vfs.Frame{Pgno: pgno, Page: page})
	}

	if len(b)-off < 4 {
		return nil, fmt.Errorf("entry truncated reading nTruncate")
	}
	nTruncate := binary.BigEndian.Uint32(b[off:])
	off += 4

	if off != len(b) {
		return nil, fmt.Errorf("entry has %d trailing bytes after nTruncate", len(b)-off)
	}

	return &Entry{NodeID: nodeID, Frames: frames, NTruncate: nTruncate}, nil
}

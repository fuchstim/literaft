// Package raft is the thin adapter between vfs.Gate/vfs.Entry and a real
// hashicorp/raft cluster. The hashicorp/raft import is
// aliased to hraft throughout so this package can keep the name CLAUDE.md's
// repo layout gives it ("/raft/ - thin adapter over the chosen RAFT
// library") without shadowing.
package raft

import (
	"encoding/binary"
	"fmt"

	"github.com/fuchstim/literaft/vfs"
)

// EncodeEntry serializes e for a hraft.Log's Data field: this is our own
// wire format, unrelated to the WAL's on-disk format (docs/DESIGN.md §RAFT
// log entry format) -- just a length-prefixed list of (pgno, page) pairs
// plus nTruncate, big-endian for consistency with the rest of the repo's
// manual binary encodings. No client-request-ID field: that's tied to the
// deferred forwarding/OCC work (docs/DECISIONS.md ADR-008/009) and has
// nothing to dedup until forwarding exists.
func EncodeEntry(e vfs.Entry) []byte {
	size := 4 // frame count
	for _, f := range e.Frames {
		size += 4 + 4 + len(f.Page) // pgno + page length + page
	}
	size += 4 // nTruncate

	buf := make([]byte, size)
	off := 0
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

// DecodeEntry is the inverse of EncodeEntry. It returns an error on
// truncated or malformed input rather than panicking, since the bytes cross
// a hraft.Log boundary.
func DecodeEntry(b []byte) (vfs.Entry, error) {
	if len(b) < 4 {
		return vfs.Entry{}, fmt.Errorf("raft: entry too short (%d bytes) to hold a frame count", len(b))
	}
	count := binary.BigEndian.Uint32(b[0:4])
	off := 4

	// Bound count against the buffer before trusting it as a make() capacity:
	// every frame needs at least 8 bytes (pgno + page length), so a corrupted
	// or truncated entry claiming far more frames than b could possibly hold
	// must fail cleanly here rather than attempt a multi-gigabyte allocation.
	if maxFrames := uint32(len(b)-off) / 8; count > maxFrames {
		return vfs.Entry{}, fmt.Errorf("raft: entry too short (%d bytes) to hold %d frames", len(b), count)
	}

	frames := make([]vfs.Frame, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(b)-off < 8 {
			return vfs.Entry{}, fmt.Errorf("raft: entry truncated reading frame %d header", i)
		}
		pgno := binary.BigEndian.Uint32(b[off:])
		off += 4
		pageLen := binary.BigEndian.Uint32(b[off:])
		off += 4
		if uint32(len(b)-off) < pageLen {
			return vfs.Entry{}, fmt.Errorf("raft: entry truncated reading frame %d page (want %d bytes)", i, pageLen)
		}
		page := append([]byte(nil), b[off:off+int(pageLen)]...)
		off += int(pageLen)
		frames = append(frames, vfs.Frame{Pgno: pgno, Page: page})
	}

	if len(b)-off < 4 {
		return vfs.Entry{}, fmt.Errorf("raft: entry truncated reading nTruncate")
	}
	nTruncate := binary.BigEndian.Uint32(b[off:])
	off += 4

	if off != len(b) {
		return vfs.Entry{}, fmt.Errorf("raft: entry has %d trailing bytes after nTruncate", len(b)-off)
	}

	return vfs.Entry{Frames: frames, NTruncate: nTruncate}, nil
}

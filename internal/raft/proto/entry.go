package raftproto

//go:generate buf generate

import (
	"fmt"

	"github.com/fuchstim/literaft/internal/vfs"
	"google.golang.org/protobuf/proto"
)

// Entry is a whole write transaction's captured frames, proposed to a Gate
// as a unit when its commit frame is withheld.
type Entry struct {
	NodeID    string
	Frames    []*vfs.Frame
	NTruncate uint32 // post-commit database size in pages
}

// Encode serializes e for a raft.Log's Data field via EntryData, the
// generated protobuf wire format: this is our own wire format, unrelated to
// the WAL's on-disk format.
func (e *Entry) Encode() []byte {
	data := &EntryData{
		NodeId:    e.NodeID,
		Frames:    make([]*FrameData, len(e.Frames)),
		NTruncate: e.NTruncate,
	}
	for i, f := range e.Frames {
		data.Frames[i] = &FrameData{Pgno: f.Pgno, Page: f.Page}
	}

	b, err := proto.Marshal(data)
	if err != nil {
		// data only has scalar/bytes/repeated-message fields with no
		// invariants proto.Marshal enforces beyond valid UTF-8 in NodeID,
		// so a failure here means a node ID isn't valid UTF-8 -- a caller
		// bug, not a condition to recover from.
		panic(fmt.Sprintf("failed to marshal entry: %v", err))
	}

	return b
}

// DecodeEntry is the inverse of Entry.Encode. It returns an error on
// malformed input rather than panicking, since the bytes cross a raft.Log
// boundary.
func DecodeEntry(b []byte) (*Entry, error) {
	data := &EntryData{}
	if err := proto.Unmarshal(b, data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
	}

	frames := make([]*vfs.Frame, len(data.Frames))
	for i, f := range data.Frames {
		frames[i] = &vfs.Frame{Pgno: f.Pgno, Page: f.Page}
	}

	return &Entry{NodeID: data.NodeId, Frames: frames, NTruncate: data.NTruncate}, nil
}

package snapshotter

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	SnapshotHeaderSize = 16 // magic(4) + version(4) + last applied index(8)

	currentMagic   = 0x4C524654 // "LRFT"
	currentVersion = 1
)

type SnapshotHeader struct {
	Magic            uint32
	Version          uint32
	LastAppliedIndex uint64
}

func NewSnapshotHeader(lastAppliedIndex uint64) *SnapshotHeader {
	return &SnapshotHeader{
		Magic:            currentMagic,
		Version:          currentVersion,
		LastAppliedIndex: lastAppliedIndex,
	}
}

func DecodeHeader(b []byte) (*SnapshotHeader, error) {
	if len(b) < SnapshotHeaderSize {
		return nil, fmt.Errorf("%w: snapshot header too short: got %d bytes, want %d", io.ErrUnexpectedEOF, len(b), SnapshotHeaderSize)
	}

	magic := binary.BigEndian.Uint32(b[0:4])
	if magic != currentMagic {
		return nil, fmt.Errorf("invalid snapshot magic: got 0x%X, want 0x%X", magic, currentMagic)
	}

	version := binary.BigEndian.Uint32(b[4:8])
	if version != currentVersion {
		return nil, fmt.Errorf("unsupported snapshot version: got %d, want %d", version, currentVersion)
	}

	lastAppliedIndex := binary.BigEndian.Uint64(b[8:16])

	return &SnapshotHeader{
		Magic:            magic,
		Version:          version,
		LastAppliedIndex: lastAppliedIndex,
	}, nil
}

func (h *SnapshotHeader) Bytes() []byte {
	b := make([]byte, SnapshotHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], h.Magic)
	binary.BigEndian.PutUint32(b[4:8], h.Version)
	binary.BigEndian.PutUint64(b[8:16], h.LastAppliedIndex)

	return b
}

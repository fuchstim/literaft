package snapshotter

import (
	"encoding/binary"
	"fmt"
)

const (
	SnapshotHeaderSize = 16 // magic(4) + version(4) + last applied index(8)

	currentMagic   = 0x4C524654 // "LRFT"
	currentVersion = 1
)

type SnapshotHeader [SnapshotHeaderSize]byte

func NewSnapshotHeader(lastAppliedIndex uint64) SnapshotHeader {
	var h SnapshotHeader
	h.SetMagic(currentMagic)
	h.SetVersion(currentVersion)
	h.SetLastAppliedIndex(lastAppliedIndex)
	return h
}

func (h SnapshotHeader) Magic() uint32      { return binary.BigEndian.Uint32(h[0:4]) }
func (h *SnapshotHeader) SetMagic(m uint32) { binary.BigEndian.PutUint32(h[0:4], m) }

func (h SnapshotHeader) Version() uint32      { return binary.BigEndian.Uint32(h[4:8]) }
func (h *SnapshotHeader) SetVersion(v uint32) { binary.BigEndian.PutUint32(h[4:8], v) }

func (h SnapshotHeader) LastAppliedIndex() uint64      { return binary.BigEndian.Uint64(h[8:16]) }
func (h *SnapshotHeader) SetLastAppliedIndex(i uint64) { binary.BigEndian.PutUint64(h[8:16], i) }

func (h SnapshotHeader) Validate() error {
	if magic := h.Magic(); magic != currentMagic {
		return fmt.Errorf("invalid snapshot magic: got 0x%X, want 0x%X", magic, currentMagic)
	}

	if version := h.Version(); version != currentVersion {
		return fmt.Errorf("unsupported snapshot version: got %d, want %d", version, currentVersion)
	}

	return nil
}

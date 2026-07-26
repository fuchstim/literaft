package wal

import "encoding/binary"

const WALHeaderSize = 32

type WALHeader [WALHeaderSize]byte

func (h WALHeader) Magic() uint32      { return binary.BigEndian.Uint32(h[0:4]) }
func (h *WALHeader) SetMagic(m uint32) { binary.BigEndian.PutUint32(h[0:4], m) }

func (h WALHeader) Version() uint32      { return binary.BigEndian.Uint32(h[4:8]) }
func (h *WALHeader) SetVersion(v uint32) { binary.BigEndian.PutUint32(h[4:8], v) }

func (h WALHeader) PageSize() uint32 {
	if ps := binary.BigEndian.Uint32(h[8:12]); ps == 1 {
		// SQLite's szPage field encodes 64K pages as 1.
		return 65536
	} else {
		return ps
	}
}
func (h *WALHeader) SetPageSize(p uint32) {
	if p == 65536 {
		// SQLite's szPage field encodes 64K pages as 1.
		p = 1
	}
	binary.BigEndian.PutUint32(h[8:12], p)
}

func (h WALHeader) CheckpointSeq() uint32      { return binary.BigEndian.Uint32(h[12:16]) }
func (h *WALHeader) SetCheckpointSeq(s uint32) { binary.BigEndian.PutUint32(h[12:16], s) }

func (h WALHeader) Salt1() uint32      { return binary.BigEndian.Uint32(h[16:20]) }
func (h *WALHeader) SetSalt1(s uint32) { binary.BigEndian.PutUint32(h[16:20], s) }

func (h WALHeader) Salt2() uint32      { return binary.BigEndian.Uint32(h[20:24]) }
func (h *WALHeader) SetSalt2(s uint32) { binary.BigEndian.PutUint32(h[20:24], s) }

func (h WALHeader) Checksum1() uint32      { return binary.BigEndian.Uint32(h[24:28]) }
func (h *WALHeader) SetChecksum1(c uint32) { binary.BigEndian.PutUint32(h[24:28], c) }

func (h WALHeader) Checksum2() uint32      { return binary.BigEndian.Uint32(h[28:32]) }
func (h *WALHeader) SetChecksum2(c uint32) { binary.BigEndian.PutUint32(h[28:32], c) }

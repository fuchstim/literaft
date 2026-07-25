package wal

import "encoding/binary"

const WALHeaderSize = 32

type WALHeader struct {
	Magic                uint32
	Version              uint32
	PageSize             uint32
	CheckpointSeq        uint32
	Salt1, Salt2         uint32
	Checksum1, Checksum2 uint32
}

func ParseWALHeader(b []byte) *WALHeader {
	return &WALHeader{
		Magic:         binary.BigEndian.Uint32(b[0:4]),
		Version:       binary.BigEndian.Uint32(b[4:8]),
		PageSize:      binary.BigEndian.Uint32(b[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(b[12:16]),
		Salt1:         binary.BigEndian.Uint32(b[16:20]),
		Salt2:         binary.BigEndian.Uint32(b[20:24]),
		Checksum1:     binary.BigEndian.Uint32(b[24:28]),
		Checksum2:     binary.BigEndian.Uint32(b[28:32]),
	}
}

func (h *WALHeader) Bytes() []byte {
	b := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], h.Magic)
	binary.BigEndian.PutUint32(b[4:8], h.Version)
	binary.BigEndian.PutUint32(b[8:12], h.PageSize)
	binary.BigEndian.PutUint32(b[12:16], h.CheckpointSeq)
	binary.BigEndian.PutUint32(b[16:20], h.Salt1)
	binary.BigEndian.PutUint32(b[20:24], h.Salt2)
	binary.BigEndian.PutUint32(b[24:28], h.Checksum1)
	binary.BigEndian.PutUint32(b[28:32], h.Checksum2)

	return b
}

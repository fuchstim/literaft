package wal

import "encoding/binary"

const FrameHeaderSize = 24

type FrameHeader struct {
	PgNo                 uint32
	NTruncate            uint32
	Salt1, Salt2         uint32
	Checksum1, Checksum2 uint32
}

func ParseFrameHeader(b []byte) *FrameHeader {
	return &FrameHeader{
		PgNo:      binary.BigEndian.Uint32(b[0:4]),
		NTruncate: binary.BigEndian.Uint32(b[4:8]),
		Salt1:     binary.BigEndian.Uint32(b[8:12]),
		Salt2:     binary.BigEndian.Uint32(b[12:16]),
		Checksum1: binary.BigEndian.Uint32(b[16:20]),
		Checksum2: binary.BigEndian.Uint32(b[20:24]),
	}
}

func (h *FrameHeader) Bytes() []byte {
	b := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], h.PgNo)
	binary.BigEndian.PutUint32(b[4:8], h.NTruncate)
	binary.BigEndian.PutUint32(b[8:12], h.Salt1)
	binary.BigEndian.PutUint32(b[12:16], h.Salt2)
	binary.BigEndian.PutUint32(b[16:20], h.Checksum1)
	binary.BigEndian.PutUint32(b[20:24], h.Checksum2)

	return b
}

func (h FrameHeader) IsCommit() bool { return h.NTruncate != 0 }

func IsFrameHeaderOffset(pageSize uint32, offset int64) bool {
	return offset >= WALHeaderSize && (offset-WALHeaderSize)%(FrameHeaderSize+int64(pageSize)) == 0
}

func IsFrameDataOffset(pageSize uint32, offset int64) bool {
	return offset >= WALHeaderSize+FrameHeaderSize && (offset-WALHeaderSize-FrameHeaderSize)%(FrameHeaderSize+int64(pageSize)) == 0
}

type Frame struct {
	Header *FrameHeader
	Data   []byte
}

func (f Frame) Bytes() []byte {
	b := make([]byte, FrameHeaderSize+len(f.Data))
	copy(b[0:FrameHeaderSize], f.Header.Bytes())
	copy(b[FrameHeaderSize:], f.Data)

	return b
}

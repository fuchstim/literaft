package wal

import "encoding/binary"

const FrameHeaderSize = 24

type Frame struct {
	Header FrameHeader
	Data   []byte
}

type FrameHeader [FrameHeaderSize]byte

func (h FrameHeader) PgNo() uint32      { return binary.BigEndian.Uint32(h[0:4]) }
func (h *FrameHeader) SetPgNo(p uint32) { binary.BigEndian.PutUint32(h[0:4], p) }

func (h FrameHeader) NTruncate() uint32      { return binary.BigEndian.Uint32(h[4:8]) }
func (h *FrameHeader) SetNTruncate(n uint32) { binary.BigEndian.PutUint32(h[4:8], n) }

func (h FrameHeader) Salt1() uint32      { return binary.BigEndian.Uint32(h[8:12]) }
func (h *FrameHeader) SetSalt1(s uint32) { binary.BigEndian.PutUint32(h[8:12], s) }

func (h FrameHeader) Salt2() uint32      { return binary.BigEndian.Uint32(h[12:16]) }
func (h *FrameHeader) SetSalt2(s uint32) { binary.BigEndian.PutUint32(h[12:16], s) }

func (h FrameHeader) Checksum1() uint32      { return binary.BigEndian.Uint32(h[16:20]) }
func (h *FrameHeader) SetChecksum1(c uint32) { binary.BigEndian.PutUint32(h[16:20], c) }

func (h FrameHeader) Checksum2() uint32      { return binary.BigEndian.Uint32(h[20:24]) }
func (h *FrameHeader) SetChecksum2(c uint32) { binary.BigEndian.PutUint32(h[20:24], c) }

func (h FrameHeader) IsCommit() bool { return h.NTruncate() != 0 }

func IsFrameHeaderOffset(pageSize uint32, offset int64) bool {
	return offset >= WALHeaderSize && (offset-WALHeaderSize)%(FrameHeaderSize+int64(pageSize)) == 0
}

func IsFrameDataOffset(pageSize uint32, offset int64) bool {
	return offset >= WALHeaderSize+FrameHeaderSize && (offset-WALHeaderSize-FrameHeaderSize)%(FrameHeaderSize+int64(pageSize)) == 0
}

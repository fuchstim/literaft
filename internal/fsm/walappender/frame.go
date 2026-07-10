package walappender

import "encoding/binary"

type Frame struct {
	pgNo, nTruncate uint32
	page            []byte
}

func NewFrame(pgNo, nTruncate uint32, page []byte) *Frame {
	return &Frame{
		pgNo:      pgNo,
		nTruncate: nTruncate,
		page:      page,
	}
}

// encodeFrame builds a 24-byte WAL frame header and returns the running
// checksum after it, chaining from seed.
func (f *Frame) encodeHeader(salt [saltSize]byte, seed [2]uint32) ([frameHeaderSize]byte, [2]uint32) {
	var fh [frameHeaderSize]byte
	binary.BigEndian.PutUint32(fh[0:4], f.pgNo)
	binary.BigEndian.PutUint32(fh[4:8], f.nTruncate)
	copy(fh[8:16], salt[:])

	s0, s1 := checksum(seed[0], seed[1], fh[:8])
	s0, s1 = checksum(s0, s1, f.page)
	binary.BigEndian.PutUint32(fh[16:20], s0)
	binary.BigEndian.PutUint32(fh[20:24], s1)
	return fh, [2]uint32{s0, s1}
}

package wal

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

const (
	WALHeaderSize    = 32
	WALHeaderMagicLE = uint32(0x377f0682)
	WALHeaderMagicBE = uint32(0x377f0683)
	WALHeaderVersion = uint32(3007000)
)

type WALHeader [WALHeaderSize]byte

func InitHeader(pageSize uint32) (WALHeader, error) {
	var saltBytes [8]byte
	if _, err := rand.Read(saltBytes[:]); err != nil {
		return WALHeader{}, fmt.Errorf("failed to generate random salt for WAL header: %w", err)
	}

	var h WALHeader
	h.SetMagic(WALHeaderMagicLE)
	h.SetVersion(WALHeaderVersion)
	h.SetPageSize(pageSize)
	h.SetCheckpointSeq(0)
	h.SetSalt1(binary.BigEndian.Uint32(saltBytes[0:4]))
	h.SetSalt2(binary.BigEndian.Uint32(saltBytes[4:8]))

	h.UpdateChecksums()

	return h, nil
}

func (h WALHeader) Magic() uint32      { return binary.BigEndian.Uint32(h[0:4]) }
func (h *WALHeader) SetMagic(m uint32) { binary.BigEndian.PutUint32(h[0:4], m) }

func (h WALHeader) Version() uint32      { return binary.BigEndian.Uint32(h[4:8]) }
func (h *WALHeader) SetVersion(v uint32) { binary.BigEndian.PutUint32(h[4:8], v) }

func (h WALHeader) PageSize() uint32      { return binary.BigEndian.Uint32(h[8:12]) }
func (h *WALHeader) SetPageSize(p uint32) { binary.BigEndian.PutUint32(h[8:12], p) }

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

func (h *WALHeader) UpdateChecksums() {
	checksum1, checksum2 := ComputeChecksums(binary.BigEndian, h[:24], 0, 0)

	h.SetChecksum1(checksum1)
	h.SetChecksum2(checksum2)
}

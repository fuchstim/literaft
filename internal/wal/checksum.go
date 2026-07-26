package wal

import (
	"encoding/binary"
)

func ComputeChecksums(enc binary.ByteOrder, data []byte, prevChecksum1, prevChecksum2 uint32) (uint32, uint32) {
	checksum1, checksum2 := prevChecksum1, prevChecksum2
	for i := 0; i < len(data); i += 8 {
		checksum1 += enc.Uint32(data[i:i+4]) + checksum2
		checksum2 += enc.Uint32(data[i+4:i+8]) + checksum1
	}

	return checksum1, checksum2
}

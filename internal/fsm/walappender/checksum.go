package walappender

import "encoding/binary"

// checksum is SQLite's WAL checksum: two Fibonacci-weighted running sums
// over the input, taken 8 bytes (two u32 words) at a time. See
// https://sqlite.org/fileformat2.html §"Checksum Algorithm" and
// wal.c:walChecksumBytes.
//
// This build never needs the byte-swapped variant: SQLite here always runs
// compiled to wasm (github.com/ncruces/go-sqlite3), and wasm's linear
// memory is little-endian by spec regardless of host CPU, so this engine's
// "native" checksum order is always little-endian and its WAL magic low
// bit (bigEndCksum) is always 0. len(b)
// must be a positive multiple of 8.
func checksum(s0, s1 uint32, b []byte) (uint32, uint32) {
	for len(b) >= 8 {
		s0 += binary.LittleEndian.Uint32(b[0:4]) + s1
		s1 += binary.LittleEndian.Uint32(b[4:8]) + s0
		b = b[8:]
	}
	return s0, s1
}

package walappender

import (
	"encoding/binary"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// le encodes each uint32 in words as 8 little-endian bytes per word pair,
// matching checksum's own two-u32-words-at-a-time reading.
func le(words ...uint32) []byte {
	b := make([]byte, 4*len(words))
	for i, w := range words {
		binary.LittleEndian.PutUint32(b[4*i:], w)
	}
	return b
}

var _ = Describe("checksum", func() {
	It("returns the seed unchanged for empty input", func() {
		s0, s1 := checksum(7, 9, nil)
		Expect(s0).To(Equal(uint32(7)))
		Expect(s1).To(Equal(uint32(9)))
	})

	It("computes SQLite's Fibonacci-weighted running sum for one 8-byte word pair", func() {
		// s0 = seed0 + word0 + seed1; s1 = seed1 + word1 + s0 (one loop
		// iteration by hand).
		s0, s1 := checksum(0, 0, le(1, 2))
		Expect(s0).To(Equal(uint32(1)))
		Expect(s1).To(Equal(uint32(3)))
	})

	It("chains across multiple word pairs the same as one call over the concatenated bytes", func() {
		b := le(1, 2, 3, 4, 5, 6)
		whole0, whole1 := checksum(0, 0, b)

		mid0, mid1 := checksum(0, 0, b[:8])
		chained0, chained1 := checksum(mid0, mid1, b[8:])

		Expect(chained0).To(Equal(whole0))
		Expect(chained1).To(Equal(whole1))
	})

	It("produces different output for different seeds over the same bytes", func() {
		a0, a1 := checksum(0, 0, le(1, 2))
		b0, b1 := checksum(1, 1, le(1, 2))
		Expect([2]uint32{a0, a1}).NotTo(Equal([2]uint32{b0, b1}))
	})
})

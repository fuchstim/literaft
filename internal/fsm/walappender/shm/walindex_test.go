package shm_test

import (
	"encoding/binary"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("wal-index header encode/decode", func() {
	sample := func() walIndexHeader {
		return walIndexHeader{
			version:     walMaxVersion,
			change:      3,
			init:        true,
			bigEndCksum: false,
			pageSize:    4096,
			maxFrame:    12,
			nPage:       12,
			frameCksum:  [2]uint32{111, 222},
			salt:        [saltSize]byte{1, 2, 3, 4, 5, 6, 7, 8},
		}
	}

	It("round-trips every field through encode/decode", func() {
		h := sample()
		encoded := h.encode()
		got := decodeWALIndexHeader(encoded[:])
		Expect(got).To(Equal(h))
	})

	It("self-checksums so a bit flip in the payload is detectable", func() {
		b := sample().encode()
		corrupt := b
		corrupt[0] ^= 0xFF

		s0, s1 := checksum(0, 0, corrupt[:40])
		Expect(s0 == binary.LittleEndian.Uint32(corrupt[40:44]) &&
			s1 == binary.LittleEndian.Uint32(corrupt[44:48])).To(BeFalse(),
			"a corrupted copy must fail its own self-checksum")
	})
})

var _ = Describe("readWALIndexHeader/writeWALIndexHeader", func() {
	It("returns false, not a zero-value header mistaken for real, on a freshly zeroed region", func() {
		region0 := make([]byte, indexHdrSize)
		_, ok := readWALIndexHeader(region0)
		Expect(ok).To(BeFalse())
	})

	It("writes both copies so a plain read of copy 0 sees what was written", func() {
		region0 := make([]byte, indexHdrSize)
		h := walIndexHeader{version: walMaxVersion, init: true, pageSize: 4096, maxFrame: 5, nPage: 5}
		writeWALIndexHeader(region0, h)

		got, ok := readWALIndexHeader(region0)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(h))
	})

	It("falls back to copy 1 when copy 0's self-checksum doesn't verify", func() {
		region0 := make([]byte, indexHdrSize)
		h := walIndexHeader{version: walMaxVersion, init: true, pageSize: 4096, maxFrame: 7, nPage: 7}
		writeWALIndexHeader(region0, h)

		// Corrupt copy 0 only; copy 1 (written first by writeWALIndexHeader,
		// per SQLite's own tear-safe ordering) must still be intact.
		region0[0] ^= 0xFF

		got, ok := readWALIndexHeader(region0)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal(h))
	})

	It("returns false when neither copy verifies", func() {
		region0 := make([]byte, indexHdrSize)
		h := walIndexHeader{version: walMaxVersion, init: true, pageSize: 4096, maxFrame: 1, nPage: 1}
		writeWALIndexHeader(region0, h)
		region0[0] ^= 0xFF
		region0[hdrCopySize] ^= 0xFF

		_, ok := readWALIndexHeader(region0)
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("framePage/frameZero at the page 0 -> page 1 boundary", func() {
	It("keeps the last frame page 0 can hold on page 0", func() {
		Expect(framePage(hashtableNPageOne)).To(Equal(0))
	})

	It("puts the first frame past page 0's capacity on page 1", func() {
		Expect(framePage(hashtableNPageOne + 1)).To(Equal(1))
	})

	It("puts the last frame page 1 can hold on page 1", func() {
		Expect(framePage(hashtableNPageOne + hashtableNPage)).To(Equal(1))
	})

	It("puts the first frame past page 1's capacity on page 2", func() {
		Expect(framePage(hashtableNPageOne + hashtableNPage + 1)).To(Equal(2))
	})

	It("frameZero(0) is 0 and frameZero(N) is the last frame index the previous page held", func() {
		Expect(frameZero(0)).To(Equal(uint32(0)))
		Expect(frameZero(1)).To(Equal(uint32(hashtableNPageOne)))
		Expect(frameZero(2)).To(Equal(uint32(hashtableNPageOne + hashtableNPage)))
	})
})

var _ = Describe("hashTableOffsets", func() {
	It("shifts page 0's aPgno array past the wal-index header", func() {
		pgnoOff, hashOff := hashTableOffsets(0)
		Expect(pgnoOff).To(Equal(indexHdrSize))
		Expect(hashOff).To(Equal(hashtableNPage * 4))
	})

	It("gives every later page the full region for aPgno", func() {
		pgnoOff, hashOff := hashTableOffsets(1)
		Expect(pgnoOff).To(Equal(0))
		Expect(hashOff).To(Equal(hashtableNPage * 4))
	})
})

var _ = Describe("addFrameToWALIndex", func() {
	It("records pgno at the frame's slot and makes it findable via the hash chain", func() {
		region := make([]byte, hashtableNPage*4+hashtableNSlot*2)
		const frame, pgno = 3, uint32(42)
		addFrameToWALIndex(region, frame, pgno)

		pgnoOff, hashOff := hashTableOffsets(framePage(frame))
		idx := int(frame - frameZero(framePage(frame)))
		Expect(binary.LittleEndian.Uint32(region[pgnoOff+(idx-1)*4:])).To(Equal(pgno))

		found := false
		for k := walHash(pgno); ; k = walNextHash(k) {
			slot := binary.LittleEndian.Uint16(region[hashOff+k*2:])
			if slot == 0 {
				break
			}
			if int(slot) == idx {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue(), "pgno's slot must be reachable by walking its hash chain")
	})

	It("wipes stale data from a reused segment on that segment's first entry", func() {
		region := make([]byte, hashtableNPage*4+hashtableNSlot*2)
		pgnoOff, _ := hashTableOffsets(0)
		// Simulate leftover bytes from a prior WAL epoch that reused this
		// mapped region.
		for i := range region {
			region[i] = 0xAA
		}

		addFrameToWALIndex(region, 1, 99)

		// Every other pgno slot in the segment must have been cleared, not
		// left as stale 0xAA bytes.
		Expect(binary.LittleEndian.Uint32(region[pgnoOff+4:])).To(Equal(uint32(0)))
	})
})

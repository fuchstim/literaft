package shm

import (
	"encoding/binary"

	"github.com/fuchstim/literaft/internal/wal"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Header", func() {
	sample := func() *Header {
		h := InitHeader(4096, 111, 222, 1, 2)
		h.SetChangeCounter(3)
		h.SetMaxFrame(12)
		h.SetPageCount(12)
		return h
	}

	It("round-trips every field through its accessors", func() {
		h := sample()
		Expect(h.Version()).To(Equal(wal.WALHeaderVersion))
		Expect(h.ChangeCounter()).To(Equal(uint32(3)))
		Expect(h.IsInit()).To(BeTrue())
		Expect(h.BigEndianChecksum()).To(BeFalse())
		Expect(h.PageSize()).To(Equal(uint32(4096)))
		Expect(h.MaxFrame()).To(Equal(uint32(12)))
		Expect(h.PageCount()).To(Equal(uint32(12)))
		Expect(h.LastFrameChecksum1()).To(Equal(uint32(111)))
		Expect(h.LastFrameChecksum2()).To(Equal(uint32(222)))
		Expect(h.Salt1()).To(Equal(uint32(1)))
		Expect(h.Salt2()).To(Equal(uint32(2)))
	})

	It("round-trips the 65536 page size sentinel (encoded on disk as 1)", func() {
		var h Header
		h.SetPageSize(65536)
		Expect(h.PageSize()).To(Equal(uint32(65536)))
	})

	It("self-checksums so a bit flip in the payload is detectable", func() {
		h := sample()
		h.UpdateChecksums()

		corrupt := *h
		corrupt[0] ^= 0xFF

		checksum1, checksum2 := wal.ComputeChecksums(binary.LittleEndian, corrupt[:40], 0, 0)
		Expect(checksum1 == corrupt.Checksum1() && checksum2 == corrupt.Checksum2()).To(BeFalse(),
			"a corrupted copy must fail its own self-checksum")
	})
})

var _ = Describe("CheckpointInfo", func() {
	It("round-trips NBackfill, per-reader read marks, and NBackfillAttempted", func() {
		var c CheckpointInfo
		c.SetNBackfill(7)
		c.SetReadMark(0, 0)
		c.SetReadMark(1, 42)
		c.SetNBackfillAttempted(9)

		Expect(c.NBackfill()).To(Equal(uint32(7)))
		Expect(c.ReadMark(0)).To(Equal(uint32(0)))
		Expect(c.ReadMark(1)).To(Equal(uint32(42)))
		Expect(c.NBackfillAttempted()).To(Equal(uint32(9)))
	})

	It("panics on an out-of-range reader index", func() {
		var c CheckpointInfo
		Expect(func() { c.ReadMark(checkpointInfoNReaders) }).To(Panic())
		Expect(func() { c.SetReadMark(checkpointInfoNReaders, 0) }).To(Panic())
	})

	It("resets to the post-rewind state: nBackfill 0, reader 1 at mark 0, every other reader unused", func() {
		var c CheckpointInfo
		for i := range c {
			c[i] = 0xAA
		}

		c.ResetForRewind()

		Expect(c.NBackfill()).To(Equal(uint32(0)))
		Expect(c.ReadMark(1)).To(Equal(uint32(0)))
		for i := 2; i < checkpointInfoNReaders; i++ {
			Expect(c.ReadMark(uint8(i))).To(Equal(uint32(readMarkNotUsed)))
		}
		Expect(c.NBackfillAttempted()).To(Equal(uint32(0)))
	})
})

var _ = Describe("regionForFrame/frameZeroForRegion at the region 0 -> region 1 boundary", func() {
	It("keeps the last frame region 0 can hold in region 0", func() {
		Expect(regionForFrame(hashtableRegion0PageCount)).To(Equal(0))
	})

	It("puts the first frame past region 0's capacity in region 1", func() {
		Expect(regionForFrame(hashtableRegion0PageCount + 1)).To(Equal(1))
	})

	It("puts the last frame region 1 can hold in region 1", func() {
		Expect(regionForFrame(hashtableRegion0PageCount + hashtablePageCount)).To(Equal(1))
	})

	It("puts the first frame past region 1's capacity in region 2", func() {
		Expect(regionForFrame(hashtableRegion0PageCount + hashtablePageCount + 1)).To(Equal(2))
	})

	It("frameZeroForRegion(0) is 0, and frameZeroForRegion(N) is the last frame index the previous region held", func() {
		Expect(frameZeroForRegion(0)).To(Equal(uint32(0)))
		Expect(frameZeroForRegion(1)).To(Equal(uint32(hashtableRegion0PageCount)))
		Expect(frameZeroForRegion(2)).To(Equal(uint32(hashtableRegion0PageCount + hashtablePageCount)))
	})
})

var _ = Describe("hashTableOffsets", func() {
	It("shifts region 0's aPgno array past the wal-index header", func() {
		pgNoOff, hashOff := hashTableOffsets(0)
		Expect(pgNoOff).To(Equal(headerSize))
		Expect(hashOff).To(Equal(hashtablePageCount * 4))
	})

	It("gives every later region the full region for aPgno", func() {
		pgNoOff, hashOff := hashTableOffsets(1)
		Expect(pgNoOff).To(Equal(0))
		Expect(hashOff).To(Equal(hashtablePageCount * 4))
	})
})

var _ = Describe("hashSlotForPage/nextHashSlot", func() {
	It("wraps back to slot 0 past the last slot", func() {
		Expect(nextHashSlot(hashtableSlotCount - 1)).To(Equal(0))
	})

	It("stays within the hash table bounds for a range of page numbers", func() {
		for pgno := uint32(1); pgno < 5000; pgno++ {
			slot := hashSlotForPage(pgno)
			Expect(slot).To(BeNumerically(">=", 0))
			Expect(slot).To(BeNumerically("<", hashtableSlotCount))
		}
	})
})

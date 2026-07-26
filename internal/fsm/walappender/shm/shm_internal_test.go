package shm

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/fuchstim/literaft/internal/lock"
	"github.com/hashicorp/go-hclog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Open dead man's switch", func() {
	It("refuses to open while another opener holds it exclusively", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")

		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()
		Expect(lock.WriteLock(f, dmsOffset, 1, true)).To(Succeed())

		_, err = Open(path, hclog.NewNullLogger())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dead man's switch held exclusively"))
	})
})

var _ = Describe("Open first-opener/joiner semantics", func() {
	It("truncates stale content for the first opener", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")
		stale := make([]byte, regionSize)
		for i := range stale {
			stale[i] = 0xAA
		}
		Expect(os.WriteFile(path, stale, 0666)).To(Succeed())

		s, err := Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s.Close()

		fi, err := s.file.Stat()
		Expect(err).NotTo(HaveOccurred())
		Expect(fi.Size()).To(Equal(int64(0)), "first opener must truncate stale content")
	})

	It("joins without truncating while another opener is still attached", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")

		s1, err := Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s1.Close()

		h := InitHeader(4096, 1, 2, 3, 4)
		h.SetMaxFrame(5)
		Expect(s1.WriteHeader(h)).To(Succeed())

		s2, err := Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s2.Close()

		got, err := s2.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(h), "a joiner must see the first opener's already-written header, not a truncated file")
	})
})

var _ = Describe("SharedMemory.AddFrame", func() {
	It("records pgno at the frame's slot and makes it findable via the hash chain", func() {
		dir := GinkgoT().TempDir()
		s, err := Open(filepath.Join(dir, "test-shm"), hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s.Close()

		const frame, pgno = 3, uint32(42)
		Expect(s.AddFrame(frame, pgno)).To(Succeed())

		regionID := regionForFrame(frame)
		region, err := s.getRegion(regionID)
		Expect(err).NotTo(HaveOccurred())

		pgNoOff, hashOff := hashTableOffsets(regionID)
		idx := int(frame - frameZeroForRegion(regionID))
		Expect(binary.LittleEndian.Uint32(region[pgNoOff+(idx-1)*4:])).To(Equal(pgno))

		found := false
		for slot := hashSlotForPage(pgno); ; slot = nextHashSlot(slot) {
			got := binary.LittleEndian.Uint16(region[hashOff+slot*2:])
			if got == 0 {
				break
			}
			if int(got) == idx {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue(), "pgno's slot must be reachable by walking its hash chain")
	})

	It("wipes stale data from a reused segment on that segment's first entry", func() {
		dir := GinkgoT().TempDir()
		s, err := Open(filepath.Join(dir, "test-shm"), hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s.Close()

		region, err := s.getRegion(0)
		Expect(err).NotTo(HaveOccurred())
		for i := range region {
			region[i] = 0xAA
		}

		Expect(s.AddFrame(1, 99)).To(Succeed())

		pgNoOff, _ := hashTableOffsets(0)
		Expect(binary.LittleEndian.Uint32(region[pgNoOff+4:])).To(Equal(uint32(0)),
			"every other pgno slot in the segment must have been cleared, not left as stale 0xAA bytes")
	})

	It("grows the -shm file into a new region once frames cross the region-0 boundary", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")
		s, err := Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s.Close()

		Expect(s.AddFrame(1, 1)).To(Succeed())
		fi, err := s.file.Stat()
		Expect(err).NotTo(HaveOccurred())
		Expect(fi.Size()).To(Equal(int64(regionSize)))

		Expect(s.AddFrame(hashtableRegion0PageCount+1, 2)).To(Succeed())
		fi, err = s.file.Stat()
		Expect(err).NotTo(HaveOccurred())
		Expect(fi.Size()).To(Equal(int64(2 * regionSize)))
	})
})

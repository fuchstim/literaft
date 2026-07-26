package shm_test

import (
	"os"
	"path/filepath"

	"github.com/fuchstim/literaft/internal/fsm/walappender/shm"
	"github.com/hashicorp/go-hclog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SharedMemory header IO", func() {
	It("reports uninitialized on a freshly opened wal-index", func() {
		dir := GinkgoT().TempDir()
		s := openSHM(dir)
		defer s.Close()

		h, err := s.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(h).NotTo(BeNil())
		Expect(h.IsInit()).To(BeFalse())
	})

	It("round-trips a written header", func() {
		dir := GinkgoT().TempDir()
		s := openSHM(dir)
		defer s.Close()

		h := shm.InitHeader(4096, 111, 222, 1, 2)
		h.SetMaxFrame(7)
		h.SetPageCount(7)
		Expect(s.WriteHeader(h)).To(Succeed())

		got, err := s.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(h))
	})

	// SQLite's wal-index header lives as two 48-byte copies at the very
	// start of region 0 (bytes 0-47 and 48-95); a reader must fall back to
	// whichever copy still verifies.
	It("falls back to the second header copy when the first is corrupted", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")
		s, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s.Close()

		h := shm.InitHeader(4096, 1, 2, 3, 4)
		h.SetMaxFrame(5)
		h.SetPageCount(5)
		Expect(s.WriteHeader(h)).To(Succeed())

		raw, err := os.OpenFile(path, os.O_RDWR, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer raw.Close()
		_, err = raw.WriteAt([]byte{0xFF}, 0)
		Expect(err).NotTo(HaveOccurred())

		got, err := s.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(h), "must fall back to the intact second copy")
	})

	It("reports uninitialized when neither header copy verifies", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")
		s, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s.Close()

		h := shm.InitHeader(4096, 1, 2, 3, 4)
		Expect(s.WriteHeader(h)).To(Succeed())

		raw, err := os.OpenFile(path, os.O_RDWR, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer raw.Close()
		_, err = raw.WriteAt([]byte{0xFF}, 0)
		Expect(err).NotTo(HaveOccurred())
		_, err = raw.WriteAt([]byte{0xFF}, 48)
		Expect(err).NotTo(HaveOccurred())

		got, err := s.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil(), "neither copy verifying must report as no header at all, not a zero-value one")
	})
})

var _ = Describe("SharedMemory checkpoint-info IO", func() {
	It("round-trips a written CheckpointInfo", func() {
		dir := GinkgoT().TempDir()
		s := openSHM(dir)
		defer s.Close()

		var info shm.CheckpointInfo
		info.SetNBackfill(3)
		info.SetReadMark(0, 9)
		Expect(s.WriteCheckpointInfo(&info)).To(Succeed())

		got, err := s.ReadCheckpointInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(&info))
	})
})

var _ = Describe("SharedMemory across independent opens (same file, distinct handles)", func() {
	It("shares written state between a first opener and a later joiner", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")

		s1, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s1.Close()

		h := shm.InitHeader(4096, 1, 2, 3, 4)
		Expect(s1.WriteHeader(h)).To(Succeed())

		s2, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s2.Close()

		got, err := s2.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(h))
	})

	It("truncates stale content when reopened after every prior opener has closed", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")

		s1, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		h := shm.InitHeader(4096, 1, 2, 3, 4)
		Expect(s1.WriteHeader(h)).To(Succeed())
		Expect(s1.Close()).To(Succeed())

		s2, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s2.Close()

		got, err := s2.ReadHeader()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got.IsInit()).To(BeFalse(), "a fresh first opener must not see the previous opener's header")
	})
})

var _ = Describe("SharedMemory locks", func() {
	It("excludes a second handle's write lock while the first holds it", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")

		s1, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s1.Close()
		s2, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s2.Close()

		Expect(s1.TryLock(shm.WriteLock)).To(Succeed())
		Expect(s2.TryLock(shm.WriteLock)).To(HaveOccurred())

		Expect(s1.Unlock(shm.WriteLock)).To(Succeed())
		Expect(s2.TryLock(shm.WriteLock)).To(Succeed())
	})

	It("lets two handles hold the same read-mark lock concurrently, but excludes a writer", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test-shm")

		s1, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s1.Close()
		s2, err := shm.Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer s2.Close()

		idx := shm.ReadLock(0)
		Expect(s1.RLock(idx)).To(Succeed())
		Expect(s2.RLock(idx)).To(Succeed(), "two readers must be able to share the same read-mark lock")

		Expect(s2.TryLockRange(idx, 1)).To(HaveOccurred(),
			"an exclusive claim must be excluded while another handle holds the lock")

		Expect(s1.Unlock(idx)).To(Succeed())
		Expect(s2.Unlock(idx)).To(Succeed())
		Expect(s2.TryLockRange(idx, 1)).To(Succeed())
	})
})

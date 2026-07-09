package shm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSHM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "shm Suite")
}

var _ = Describe("Open", func() {
	It("lets a second opener join the mapping the first one initialized", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "test.db-shm")

		first, err := Open(path)
		Expect(err).NotTo(HaveOccurred())
		defer first.Close()

		region, err := first.Region(0)
		Expect(err).NotTo(HaveOccurred())
		region[0] = 0x42

		second, err := Open(path)
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		joined, err := second.Region(0)
		Expect(err).NotTo(HaveOccurred())
		Expect(joined[0]).To(Equal(byte(0x42)), "both openers must share the same mapping")
	})
})

// Regression tests for the dead-man's-switch handshake fix: Open used to
// take a fully blocking (F_OFD_SETLKW, no timeout) lock on the DMS byte
// range when claiming it as first opener, diverging from upstream's
// deliberately non-blocking attempt (shm/upstream/shm_ofd.go.upstream's own
// comment: "no point in blocking here"). A peer that's slow to downgrade its
// claim -- or stalls entirely -- could leave that blocking call waiting
// forever. retryNonBlocking replaces it with a short, bounded retry instead.
var _ = Describe("retryNonBlocking", func() {
	// heldLock opens path a second time and takes a blocking write lock on
	// the DMS byte range, simulating another opener mid-handshake (the
	// window Open's first-opener branch claims and then downgrades).
	heldLock := func(path string) *os.File {
		GinkgoHelper()
		holder, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		Expect(err).NotTo(HaveOccurred())
		Expect(writeLock(holder, dmsOffset, 1, true)).To(Succeed())
		return holder
	}

	It("succeeds once a contended lock is released, without blocking past that", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "contended.db-shm")

		holder := heldLock(path)
		defer holder.Close()

		// released happens-before this test's defers run: without an
		// explicit Go-level edge between the unlock below and holder.Close()
		// above, the race detector can't see that the kernel already
		// serialized them and flags a false positive on holder's fd.
		released := make(chan struct{})
		go func() {
			defer close(released)
			time.Sleep(100 * time.Millisecond)
			if err := unlock(holder, dmsOffset, 1); err != nil {
				panic(err)
			}
		}()
		defer func() { <-released }()

		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()

		done := make(chan error, 1)
		go func() { done <- retryNonBlocking(func() error { return writeLock(f, dmsOffset, 1, false) }) }()

		// Bounded: a blocking F_OFD_SETLKW here (the pre-fix behavior) would
		// also eventually succeed once released, so what this actually
		// proves is timing -- it must resolve close to the 100ms release,
		// not require the full retry budget.
		Eventually(done, 1*time.Second, 5*time.Millisecond).Should(Receive(Succeed()))
	})

	It("gives up after its retry budget instead of blocking indefinitely on a lock that's never released", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "stuck.db-shm")

		holder := heldLock(path)
		defer holder.Close()
		// Deliberately never released within this test.

		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()

		done := make(chan error, 1)
		go func() { done <- retryNonBlocking(func() error { return writeLock(f, dmsOffset, 1, false) }) }()

		Eventually(done, dmsLockRetryTimeout+2*time.Second, 10*time.Millisecond).Should(Receive(HaveOccurred()))
	})
})

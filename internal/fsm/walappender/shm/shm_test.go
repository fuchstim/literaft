package shm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fuchstim/literaft/internal/lock"
	"github.com/hashicorp/go-hclog"

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

		first, err := Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer first.Close()

		region, err := first.GetRegion(0)
		Expect(err).NotTo(HaveOccurred())
		region[0] = 0x42

		second, err := Open(path, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		joined, err := second.GetRegion(0)
		Expect(err).NotTo(HaveOccurred())
		Expect(joined[0]).To(Equal(byte(0x42)), "both openers must share the same mapping")
	})
})

var _ = Describe("non-blocking DMS", func() {
	acquireLock := func(path string) *os.File {
		GinkgoHelper()
		holder, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock.WriteLock(holder, dmsOffset, 1, true)).To(Succeed())
		return holder
	}

	It("succeeds once a contended lock is released, without blocking past that", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "contended.db-shm")

		holder := acquireLock(path)
		defer holder.Close()

		released := make(chan struct{})
		t := time.AfterFunc(100*time.Millisecond, func() {
			defer GinkgoRecover()
			defer close(released)
			Expect(lock.Unlock(holder, dmsOffset, 1)).To(Succeed())
		})
		DeferCleanup(func() { t.Stop(); <-released })

		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()

		done := make(chan error, 1)
		go func() { done <- acquireDMSWithRetries(f, lock.WriteLock) }()

		Eventually(done, 1*time.Second, 5*time.Millisecond).Should(Receive(BeNil()))
	})

	It("gives up after its retry budget instead of blocking indefinitely on a lock that's never released", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "stuck.db-shm")

		holder := acquireLock(path)
		defer holder.Close() // Only release after the test is done

		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()

		done := make(chan error, 1)
		go func() { done <- acquireDMSWithRetries(f, lock.WriteLock) }()

		Eventually(done, dmsLockRetryTimeout+2*time.Second).Should(Receive(Not(BeNil())))
	})
})

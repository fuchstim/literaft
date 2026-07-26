package shm_test

import (
	"path/filepath"
	"testing"

	"github.com/fuchstim/literaft/internal/fsm/walappender/shm"
	"github.com/hashicorp/go-hclog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSHM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "shm Suite")
}

// openSHM opens a fresh wal-index file under dir, failing the test on error.
func openSHM(dir string) *shm.SharedMemory {
	GinkgoHelper()
	s, err := shm.Open(filepath.Join(dir, "test-shm"), hclog.NewNullLogger())
	Expect(err).NotTo(HaveOccurred())
	return s
}

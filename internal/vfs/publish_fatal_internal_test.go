package vfs

import (
	"errors"

	"github.com/fuchstim/literaft/internal/wal"
	"github.com/hashicorp/go-hclog"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// faultyFile is a minimal sqlite3vfs.File whose WriteAt fails once the
// configured number of successful writes has elapsed.
type faultyFile struct {
	sqlite3vfs.File
	pageSize        uint32
	writesUntilFail int
	writes          int
}

var errInjectedWrite = errors.New("injected write failure")

func (f *faultyFile) ReadAt(p []byte, off int64) (int, error) {
	if off != 0 {
		return 0, errors.New("faultyFile only supports ReadAt(0)")
	}

	header := &wal.WALHeader{PageSize: f.pageSize}
	copy(p, header.Bytes())
	return len(p), nil
}

func (f *faultyFile) WriteAt(p []byte, off int64) (int, error) {
	f.writes++
	if f.writes > f.writesUntilFail {
		return 0, errInjectedWrite
	}
	return len(p), nil
}

type alwaysCommitGate struct{}

var _ Gate = alwaysCommitGate{}

func (alwaysCommitGate) ProposeTransaction(frames []*wal.Frame, nTruncate uint32) error { return nil }

// driveToCommitFlush feeds a single-frame commit transaction through a WAL
// File wrapping base, stopping just before the post-gate flush: it writes
// the commit-frame header (held back, never flushed by writeFrameHeader),
// then returns the closure that writes the paired page image -- the call
// that runs the fatal flush branch.
func driveToCommitFlush(base sqlite3vfs.File, pageSize uint32) func() {
	f, err := newGatedWALFile(base, alwaysCommitGate{}, hclog.NewNullLogger())
	Expect(err).NotTo(HaveOccurred())

	hdr := &wal.FrameHeader{PgNo: 1, NTruncate: 1}
	n, err := f.WriteAt(hdr.Bytes(), wal.WALHeaderSize)
	Expect(err).NotTo(HaveOccurred())
	Expect(n).To(Equal(wal.FrameHeaderSize))
	Expect(f.pendingFrameHeader).NotTo(BeNil(), "commit-frame header must be held pending")

	return func() { f.WriteAt(make([]byte, pageSize), wal.WALHeaderSize+wal.FrameHeaderSize) }
}

var _ = Describe("fatal publish-after-commit", func() {
	const pageSize = 512

	It("panics when the committed commit-frame header fails to flush", func() {
		// writesUntilFail 0: the very first real disk write (the withheld
		// header being flushed after gate success) fails.
		flush := driveToCommitFlush(&faultyFile{writesUntilFail: 0, pageSize: pageSize}, pageSize)
		Expect(flush).To(PanicWith(ContainSubstring("commit-frame header")))
	})

	It("panics when the committed commit-frame page image fails to flush", func() {
		// writesUntilFail 1: the header flush succeeds, the paired page-image
		// flush is the one that fails.
		flush := driveToCommitFlush(&faultyFile{writesUntilFail: 1, pageSize: pageSize}, pageSize)
		Expect(flush).To(PanicWith(ContainSubstring("commit-frame data")))
	})

	It("does not panic when both flushes succeed", func() {
		flush := driveToCommitFlush(&faultyFile{writesUntilFail: 2, pageSize: pageSize}, pageSize)
		Expect(flush).NotTo(Panic())
	})
})

package vfs

import (
	"encoding/binary"
	"errors"

	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// White-box tests for the fatal publish-after-commit contract in
// writeFrameData. Once the gate approves a commit frame the transaction is
// committed cluster-wide; any failure to then flush the withheld frame to
// the local -wal must panic rather than return an I/O error, because the
// entry will never be redelivered to this node and a silent local rollback
// would permanently diverge it. Driving this needs a base File whose WriteAt
// fails on demand, which the black-box vfs_test package can't inject.

// nilCommitGate is a Gate that always approves, so the commit frame reaches
// the post-gate flush branch under test.
type nilCommitGate struct{}

func (nilCommitGate) ProposeTransaction(frames []*Frame, nTruncate uint32) error { return nil }

// faultyFile is a minimal sqlite3vfs.File whose WriteAt fails once the
// configured number of successful writes has elapsed. Every other method is
// unused on the path under test; leaving the embedded interface nil makes an
// unexpected call panic loudly rather than silently misbehave.
type faultyFile struct {
	sqlite3vfs.File
	writesUntilFail int
	writes          int
}

var errInjectedWrite = errors.New("injected write failure")

func (f *faultyFile) WriteAt(p []byte, off int64) (int, error) {
	f.writes++
	if f.writes > f.writesUntilFail {
		return 0, errInjectedWrite
	}
	return len(p), nil
}

// commitFrameHeader builds a 24-byte WAL frame header for a commit frame:
// pgno in bytes 0-3, a non-zero post-commit db size in bytes 4-7 (which is
// what marks it a commit frame). Salt/checksum bytes are irrelevant to the
// gate, which only ever replays the exact bytes SQLite wrote.
func commitFrameHeader(pgno, dbSize uint32) []byte {
	b := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], pgno)
	binary.BigEndian.PutUint32(b[4:8], dbSize)
	return b
}

// driveToCommitFlush feeds a single-frame commit transaction through a WAL
// File wrapping base, stopping just before the post-gate flush: it writes
// the commit-frame header (held back, never flushed by writeFrameHeader),
// then returns the closure that writes the paired page image -- the call
// that runs the fatal flush branch.
func driveToCommitFlush(base sqlite3vfs.File, pageSize uint32) func() {
	f := wrapFile(base, FileTypeWAL, nilCommitGate{}, pageSize)

	hdr := commitFrameHeader(1, 1)
	headerOff := int64(walHeaderSize)
	n, err := f.WriteAt(hdr, headerOff)
	Expect(err).NotTo(HaveOccurred())
	Expect(n).To(Equal(frameHeaderSize))
	Expect(f.pending).NotTo(BeNil(), "commit-frame header must be held pending")

	page := make([]byte, pageSize)
	dataOff := headerOff + frameHeaderSize
	return func() { f.WriteAt(page, dataOff) }
}

var _ = Describe("fatal publish-after-commit", func() {
	const pageSize = 512

	It("panics when the committed commit-frame header fails to flush", func() {
		// writesUntilFail 0: the very first real disk write (the withheld
		// header being flushed after gate success) fails.
		flush := driveToCommitFlush(&faultyFile{writesUntilFail: 0}, pageSize)
		Expect(flush).To(PanicWith(ContainSubstring("commit-frame header")))
	})

	It("panics when the committed commit-frame page image fails to flush", func() {
		// writesUntilFail 1: the header flush succeeds, the paired page-image
		// flush is the one that fails.
		flush := driveToCommitFlush(&faultyFile{writesUntilFail: 1}, pageSize)
		Expect(flush).To(PanicWith(ContainSubstring("commit-frame page image")))
	})

	It("does not panic when both flushes succeed", func() {
		flush := driveToCommitFlush(&faultyFile{writesUntilFail: 2}, pageSize)
		Expect(flush).NotTo(Panic())
	})
})

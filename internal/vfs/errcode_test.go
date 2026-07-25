package vfs_test

import (
	"errors"
	"path/filepath"

	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/wal"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type codeGate struct{ err error }

func (g codeGate) ProposeTransaction(frames []*wal.Frame, nTruncate uint32) error { return g.err }

type codedErr struct {
	error
	code sqlite3.ExtendedErrorCode
}

func (e codedErr) ResultCode() sqlite3.ExtendedErrorCode { return e.code }

var _ = Describe("rejected-write error code mapping", func() {
	It("surfaces a CodedError's carried code instead of the IOERR_WRITE default", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "busy.db")

		gate := codeGate{err: codedErr{errors.New("catching up"), sqlite3.ExtendedErrorCode(sqlite3.BUSY)}}
		vfsName := registerVFSWithGate(gate)

		c := openDB(path, vfsName)

		writeErr := c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(writeErr).To(HaveOccurred())
		Expect(errors.Is(writeErr, sqlite3.BUSY)).To(BeTrue(),
			"a CodedError(..., BUSY) rejection must surface as sqlite3.BUSY, got: %v", writeErr)
	})

	It("defaults to IOERR_WRITE for a plain, uncoded rejection error", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "plain-reject.db")

		gate := codeGate{err: errors.New("rejected for test")}
		vfsName := registerVFSWithGate(gate)

		c := openDB(path, vfsName)

		writeErr := c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(writeErr).To(HaveOccurred())
		Expect(errors.Is(writeErr, sqlite3.IOERR_WRITE)).To(BeTrue(),
			"a plain rejection error must default to IOERR_WRITE, got: %v", writeErr)
	})
})

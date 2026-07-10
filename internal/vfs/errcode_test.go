package vfs_test

import (
	"errors"
	"path/filepath"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// codeGate is a stub vfs.Gate that always rejects a proposal with err,
// letting these tests control exactly what internal/vfs.File's abort path
// sees.
type codeGate struct{ err error }

func (g codeGate) ProposeTransaction(frames []*vfs.Frame, nTruncate uint32) error { return g.err }

var _ = Describe("rejected-write error code mapping", func() {
	It("surfaces a GateError's carried code instead of the IOERR_WRITE default", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "busy.db")

		gate := codeGate{err: vfs.GateError(errors.New("catching up"), sqlite3.ExtendedErrorCode(sqlite3.BUSY))}
		name := "literaft-errcode-test-busy"
		vfs.Register(name, sqlite3vfs.Find(""), gate, probePageSize())

		c, err := sqlite3.Open("file:" + path + "?vfs=" + name)
		Expect(err).NotTo(HaveOccurred())
		defer c.Close()
		Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
		Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())

		writeErr := c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(writeErr).To(HaveOccurred())
		Expect(errors.Is(writeErr, sqlite3.BUSY)).To(BeTrue(),
			"a GateError(..., BUSY) rejection must surface as sqlite3.BUSY, got: %v", writeErr)
	})

	It("defaults to IOERR_WRITE for a plain, uncoded rejection error", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "plain-reject.db")

		gate := codeGate{err: errors.New("rejected for test")}
		name := "literaft-errcode-test-plain"
		vfs.Register(name, sqlite3vfs.Find(""), gate, probePageSize())

		c, err := sqlite3.Open("file:" + path + "?vfs=" + name)
		Expect(err).NotTo(HaveOccurred())
		defer c.Close()
		Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
		Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())

		writeErr := c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(writeErr).To(HaveOccurred())
		Expect(errors.Is(writeErr, sqlite3.IOERR_WRITE)).To(BeTrue(),
			"a plain rejection error must default to IOERR_WRITE, got: %v", writeErr)
	})
})

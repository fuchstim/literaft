package vfs_test

import (
	"testing"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestVFS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "vfs Suite")
}

// alwaysCommitGate is a stub vfs.Gate that never rejects a proposal --
// single-node-equivalent replication, standing in for a real Gate in every
// test here that isn't specifically exercising the abort branch.
type alwaysCommitGate struct{}

func (alwaysCommitGate) ProposeTransaction(frames []*vfs.Frame, nTruncate uint32) error { return nil }

// probePageSize returns SQLite's actual default page size by asking a
// throwaway in-memory connection, rather than assuming a value. The
// registered VFS uses this value directly to compute frame-header offsets,
// so every registration in this package's tests needs the real value,
// never 0.
func probePageSize() uint32 {
	c, err := sqlite3.Open(":memory:")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	stmt, _, err := c.Prepare("PRAGMA page_size")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	if !stmt.Step() {
		panic("PRAGMA page_size returned no rows")
	}
	return uint32(stmt.ColumnInt64(0))
}

// vfsName is registered once, backed by alwaysCommitGate, for every test in
// this package that doesn't need to observe or reject a proposal itself.
const vfsName = "literaft-vfs-test"

func init() {
	vfs.Register(vfsName, sqlite3vfs.Find(""), alwaysCommitGate{}, probePageSize())
}

func queryInt(c *sqlite3.Conn, sql string) int64 {
	GinkgoHelper()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnInt64(0)
}

func queryText(c *sqlite3.Conn, sql string) string {
	GinkgoHelper()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnText(0)
}

// open opens path through the shared always-commit VFS registration, with
// the pragmas a real driver connection always sets.
func open(path string) *sqlite3.Conn {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + path + "?vfs=" + vfsName)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { c.Close() })
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
	Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())
	return c
}

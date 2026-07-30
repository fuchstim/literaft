package vfs_test

import (
	"os/exec"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var vfsName, sqlite3Path string

func TestVFS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "vfs Suite")
}

type alwaysCommitGate struct{}

var _ vfs.Gate = alwaysCommitGate{}

func (alwaysCommitGate) ProposeTransaction(frames []*wal.Frame) error { return nil }

var _ = BeforeSuite(func() {
	vfsName = uuid.NewString()
	vfs.Register(vfsName, sqlite3vfs.Find(""), alwaysCommitGate{}, hclog.NewNullLogger())

	var err error
	sqlite3Path, err = exec.LookPath("sqlite3")
	Expect(err).NotTo(HaveOccurred())

	if !sqlite3vfs.SupportsFileLocking || !sqlite3vfs.SupportsSharedMemory {
		Fail("platform lacks the file locking or shared memory support requirement #3 depends on")
	}
})

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

func openDB(path, vfsName string) *sqlite3.Conn {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + path + "?vfs=" + vfsName)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(c.Close)

	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
	Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())

	return c
}

func registerVFSWithGate(gate vfs.Gate) string {
	GinkgoHelper()
	name := uuid.NewString()
	vfs.Register(name, sqlite3vfs.Find(""), gate, hclog.NewNullLogger())

	return name
}

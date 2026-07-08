package node

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This file is package node, not node_test, specifically so it can reach
// n.backend directly -- an FSM-internal hook (raftadapter.Snapshotter), not
// part of Node's public API -- to unit-test the Snapshot/Restore round trip
// without a live multi-node raft cluster driving InstallSnapshot end to end
// (that's cluster_test.go's job).

// freeAddr grabs an OS-assigned loopback port. Mirrors cluster_test.go's
// freeTCPAddr, duplicated here because that one lives in package node_test.
func freeAddr() string {
	GinkgoHelper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	addr := l.Addr().String()
	Expect(l.Close()).To(Succeed())
	return addr
}

// startSingleNode brings up a one-node, self-bootstrapped cluster under
// dir/id, real enough (same node.Start production path) to have a fully
// wired dbBackend to exercise directly.
func startSingleNode(dir, id string, pageSize uint32) *Node {
	GinkgoHelper()
	addr := freeAddr()
	cfg := Config{
		ID:       id,
		BindAddr: addr,
		DataDir:  filepath.Join(dir, id),
		DBPath:   filepath.Join(dir, id+".db"),
		PageSize: pageSize,
		Bootstrap: []hraft.Server{
			{Suffrage: hraft.Voter, ID: hraft.ServerID(id), Address: hraft.ServerAddress(addr)},
		},
		ApplyTimeout: 2 * time.Second,
	}
	n, err := Start(cfg)
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() bool { return n.Ready() }, 5*time.Second, 10*time.Millisecond).Should(BeTrue())
	return n
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

// nodeExec/nodeQueryText/nodeQueryInt run against n's kept-alive connection
// via WithDB, rather than holding a *sqlite3.Conn from a since-removed DB()
// accessor across the call -- exactly the pattern a concurrent Restore
// (this file's own subject) could race. Duplicated from cluster_test.go's
// identical helpers since that file is package node_test, not node.
func nodeExec(n *Node, sql string) error {
	GinkgoHelper()
	return n.WithDB(func(c *sqlite3.Conn) error { return c.Exec(sql) })
}

func nodeQueryText(n *Node, sql string) string {
	GinkgoHelper()
	var result string
	Expect(n.WithDB(func(c *sqlite3.Conn) error {
		result = queryText(c, sql)
		return nil
	})).To(Succeed())
	return result
}

func nodeQueryInt(n *Node, sql string) int64 {
	GinkgoHelper()
	var result int64
	Expect(n.WithDB(func(c *sqlite3.Conn) error {
		result = queryInt(c, sql)
		return nil
	})).To(Succeed())
	return result
}

// pageSizeProbe returns SQLite's actual default page size, the same caution
// cluster_test.go and apply_test.go take (CLAUDE.md: verify, don't assume).
func pageSizeProbe() uint32 {
	GinkgoHelper()
	c, err := sqlite3.Open(":memory:")
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return uint32(queryInt(c, "PRAGMA page_size"))
}

// docs/ROADMAP.md M6 "done when": a Snapshot/Restore round trip must leave
// the destination logically identical to the source, including to an
// external, unmodified-VFS reader -- the same bar M1/M3 hold themselves to.
var _ = Describe("dbBackend snapshot/restore (M6)", func() {
	It("round-trips a database's full state via Snapshot and Restore", func() {
		dir := GinkgoT().TempDir()
		pageSize := pageSizeProbe()

		src := startSingleNode(dir, "src", pageSize)
		defer src.Shutdown()

		Expect(nodeExec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 50; i++ {
			Expect(nodeExec(src, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		rc, err := src.backend.Snapshot()
		Expect(err).NotTo(HaveOccurred())

		dst := startSingleNode(dir, "dst", pageSize)
		defer dst.Shutdown()

		Expect(dst.backend.Restore(rc)).To(Succeed())
		Expect(rc.Close()).To(Succeed())

		Expect(nodeQueryText(dst, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(nodeQueryInt(dst, "SELECT count(*) FROM t")).To(Equal(int64(50)))
		Expect(nodeQueryText(dst, "SELECT v FROM t WHERE id = 1")).To(Equal("row0"))
		Expect(nodeQueryText(dst, "SELECT v FROM t WHERE id = 50")).To(Equal("row49"))

		// External-reader compatibility (docs/ROADMAP.md M1/M3's bar): open
		// the restored file with a completely plain, unmodified-VFS
		// connection -- no "?vfs=" at all.
		external, err := sqlite3.Open(dst.cfg.DBPath)
		Expect(err).NotTo(HaveOccurred())
		defer external.Close()
		Expect(queryText(external, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(external, "SELECT count(*) FROM t")).To(Equal(int64(50)))
	})
})

package snapshotter_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/fsm/snapshotter"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSnapshotter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "snapshotter Suite")
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

// pageSizeProbe returns SQLite's actual default page size by asking a
// throwaway in-memory connection, rather than assuming a value (CLAUDE.md:
// verify, don't assume).
func pageSizeProbe() uint32 {
	GinkgoHelper()
	c, err := sqlite3.Open(":memory:")
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return uint32(queryInt(c, "PRAGMA page_size"))
}

// primeWALMode establishes path's WAL-mode identity via a plain connection,
// mirroring what fsm.New always does before FSM.Restore could plausibly be
// invoked against a real node's db path (see internal/fsm/walappender's
// identical requirement -- Restore drives the same walappender.Open).
func primeWALMode(path string) {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + path)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
}

var _ = Describe("Snapshotter.Snapshot / Restore", func() {
	It("round-trips a database's full state, readable by an external plain-VFS connection", func() {
		dir := GinkgoT().TempDir()
		pageSize := pageSizeProbe()

		srcPath := filepath.Join(dir, "src.db")
		primeWALMode(srcPath)
		src, err := sqlite3.Open("file:" + srcPath)
		Expect(err).NotTo(HaveOccurred())
		defer src.Close()
		Expect(src.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 50; i++ {
			Expect(src.Exec(fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		rc, err := snapshotter.New(srcPath, pageSize).Snapshot()
		Expect(err).NotTo(HaveOccurred())

		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)

		Expect(snapshotter.New(dstPath, pageSize).Restore(rc)).To(Succeed())
		Expect(rc.Close()).To(Succeed())

		dst, err := sqlite3.Open("file:" + dstPath)
		Expect(err).NotTo(HaveOccurred())
		defer dst.Close()
		Expect(queryText(dst, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(dst, "SELECT count(*) FROM t")).To(Equal(int64(50)))
		Expect(queryText(dst, "SELECT v FROM t WHERE id = 1")).To(Equal("row0"))
		Expect(queryText(dst, "SELECT v FROM t WHERE id = 50")).To(Equal("row49"))

		// External-reader compatibility: a completely plain,
		// unmodified-VFS connection (no "?vfs=" at all).
		external, err := sqlite3.Open(dstPath)
		Expect(err).NotTo(HaveOccurred())
		defer external.Close()
		Expect(queryText(external, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(external, "SELECT count(*) FROM t")).To(Equal(int64(50)))
	})

	// The following three cases exercise Restore's page-parsing validation
	// branches, all new relative to the old whole-file-swap Restore this
	// replaced (which had nothing to parse, and so nothing to validate).

	It("rejects an empty snapshot", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)

		err := snapshotter.New(dstPath, pageSizeProbe()).Restore(bytes.NewReader(nil))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a snapshot whose size isn't a whole multiple of the page size", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)
		pageSize := pageSizeProbe()

		err := snapshotter.New(dstPath, pageSize).Restore(bytes.NewReader(make([]byte, int(pageSize)+1)))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a snapshot whose own page size doesn't match the configured cluster page size", func() {
		dir := GinkgoT().TempDir()
		pageSize := pageSizeProbe()

		srcPath := filepath.Join(dir, "src.db")
		primeWALMode(srcPath)
		src, err := sqlite3.Open("file:" + srcPath)
		Expect(err).NotTo(HaveOccurred())
		defer src.Close()
		Expect(src.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")).To(Succeed())

		rc, err := snapshotter.New(srcPath, pageSize).Snapshot()
		Expect(err).NotTo(HaveOccurred())
		defer rc.Close()

		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)

		// The snapshot's own page-1 bytes say pageSize; claiming a
		// different cluster page size here must be rejected rather than
		// silently misinterpreting the frame layout.
		err = snapshotter.New(dstPath, pageSize+1).Restore(rc)
		Expect(err).To(HaveOccurred())
	})
})

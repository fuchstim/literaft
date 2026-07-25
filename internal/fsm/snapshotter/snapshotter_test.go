package snapshotter_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
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

func openDB(path string) *sqlite3.Conn {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + path)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(c.Close)

	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())

	return c
}

var _ = Describe("Snapshotter.Snapshot / Restore", func() {
	It("round-trips a database's full state, readable by an external plain-VFS connection", func() {
		dir := GinkgoT().TempDir()

		srcPath := filepath.Join(dir, "src.db")
		src := openDB(srcPath)
		pageSize := queryInt(src, "PRAGMA page_size")
		Expect(src.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 50; i++ {
			Expect(src.Exec(fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		rc, err := snapshotter.New(srcPath, uint32(pageSize), hclog.NewNullLogger()).Snapshot(42)
		Expect(err).NotTo(HaveOccurred())

		dstPath := filepath.Join(dir, "dst.db")
		dst := openDB(dstPath)

		header, err := snapshotter.New(dstPath, uint32(pageSize), hclog.NewNullLogger()).Restore(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(header.LastAppliedIndex).To(Equal(uint64(42)), "Restore must recover the snapshot's raft index from the stream header")
		Expect(rc.Close()).To(Succeed())

		Expect(queryText(dst, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(dst, "SELECT count(*) FROM t")).To(Equal(int64(50)))
		Expect(queryText(dst, "SELECT v FROM t WHERE id = 1")).To(Equal("row0"))
		Expect(queryText(dst, "SELECT v FROM t WHERE id = 50")).To(Equal("row49"))
	})

	It("rejects a snapshot missing the literaft header", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		dst := openDB(dstPath)
		pageSize := queryInt(dst, "PRAGMA page_size")

		page1 := make([]byte, pageSize)
		copy(page1, "SQLite format 3\x00")
		_, err := snapshotter.New(dstPath, uint32(pageSize), hclog.NewNullLogger()).Restore(bytes.NewReader(page1))
		Expect(err).To(HaveOccurred())
	})

	It("rejects an empty snapshot (header present, no pages)", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		dst := openDB(dstPath)
		pageSize := queryInt(dst, "PRAGMA page_size")

		_, err := snapshotter.New(dstPath, uint32(pageSize), hclog.NewNullLogger()).Restore(bytes.NewReader(snapshotter.NewSnapshotHeader(0).Bytes()))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a snapshot whose size isn't a whole multiple of the page size", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		dst := openDB(dstPath)
		pageSize := queryInt(dst, "PRAGMA page_size")

		stream := append(snapshotter.NewSnapshotHeader(0).Bytes(), make([]byte, int(pageSize)+1)...)
		_, err := snapshotter.New(dstPath, uint32(pageSize), hclog.NewNullLogger()).Restore(bytes.NewReader(stream))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a snapshot whose own page size doesn't match the configured cluster page size", func() {
		dir := GinkgoT().TempDir()

		srcPath := filepath.Join(dir, "src.db")
		src := openDB(srcPath)
		pageSize := queryInt(src, "PRAGMA page_size")
		Expect(src.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")).To(Succeed())

		rc, err := snapshotter.New(srcPath, uint32(pageSize), hclog.NewNullLogger()).Snapshot(0)
		Expect(err).NotTo(HaveOccurred())
		defer rc.Close()

		dstPath := filepath.Join(dir, "dst.db")

		_, err = snapshotter.New(dstPath, uint32(pageSize/2), hclog.NewNullLogger()).Restore(rc)
		Expect(err).To(HaveOccurred())
	})
})

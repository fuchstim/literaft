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
// throwaway in-memory connection, rather than assuming a value.
func pageSizeProbe() uint32 {
	GinkgoHelper()
	c, err := sqlite3.Open(":memory:")
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return uint32(queryInt(c, "PRAGMA page_size"))
}

// primeWALMode establishes path's WAL-mode identity via a plain connection,
// mirroring what a real node always does before Restore could plausibly be
// invoked against its db path.
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

		rc, err := snapshotter.New(srcPath, pageSize).Snapshot(42)
		Expect(err).NotTo(HaveOccurred())

		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)

		index, err := snapshotter.New(dstPath, pageSize).Restore(rc)
		Expect(err).NotTo(HaveOccurred())
		Expect(index).To(Equal(uint64(42)), "Restore must recover the snapshot's raft index from the stream header")
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

	It("rejects a snapshot missing the literaft header (an old, headerless snapshot)", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)
		pageSize := pageSizeProbe()

		// A headerless stream begins with page 1's "SQLite format 3\0",
		// which must not be mistaken for the header magic.
		page1 := make([]byte, pageSize)
		copy(page1, "SQLite format 3\x00")
		_, err := snapshotter.New(dstPath, pageSize).Restore(bytes.NewReader(page1))
		Expect(err).To(HaveOccurred())
	})

	It("rejects an empty snapshot (header present, no pages)", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)

		_, err := snapshotter.New(dstPath, pageSizeProbe()).Restore(bytes.NewReader(snapshotter.EncodeHeaderForTest(0)))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a snapshot whose size isn't a whole multiple of the page size", func() {
		dir := GinkgoT().TempDir()
		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)
		pageSize := pageSizeProbe()

		stream := append(snapshotter.EncodeHeaderForTest(0), make([]byte, int(pageSize)+1)...)
		_, err := snapshotter.New(dstPath, pageSize).Restore(bytes.NewReader(stream))
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

		rc, err := snapshotter.New(srcPath, pageSize).Snapshot(0)
		Expect(err).NotTo(HaveOccurred())
		defer rc.Close()

		dstPath := filepath.Join(dir, "dst.db")
		primeWALMode(dstPath)

		// The snapshot's own page-1 bytes say pageSize; claiming a
		// different cluster page size here must be rejected rather than
		// silently misinterpreting the frame layout.
		_, err = snapshotter.New(dstPath, pageSize+1).Restore(rc)
		Expect(err).To(HaveOccurred())
	})
})

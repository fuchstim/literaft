package vfs_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Once a write transaction has spilled its dirty pages to the WAL at least
// once, re-dirtying an already-spilled page overwrites that page's
// existing frame *in place*: a bare page-sized write with no preceding
// frame header, since the header (pgno, commit marker) is unchanged.
// Committing such a transaction can also rewrite every affected frame's
// header a second time -- including the just-written commit frame's own
// -- purely to fix the cumulative checksum chain: another bare write with
// no paired partner, this time header-shaped.
//
// A tiny cache_size forces spills every few dirty pages; running the same
// full-table UPDATE more than once inside one explicit transaction
// guarantees every page gets dirtied, spilled, and dirtied again well
// before commit.
var _ = Describe("commit-frame interception under page-cache spill", func() {
	It("captures a page's final content even when a spill re-dirties it mid-transaction", func() {
		dir := GinkgoT().TempDir()

		const rows = 2000
		var insertSQL strings.Builder
		insertSQL.WriteString("BEGIN;\nCREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);\n")
		for i := range rows {
			fmt.Fprintf(&insertSQL, "INSERT INTO t (v) VALUES ('%040d');\n", i)
		}
		insertSQL.WriteString("COMMIT;\n")

		const updateSQL = `
			BEGIN;
			UPDATE t SET v = v || 'x';
			UPDATE t SET v = v || 'y';
			UPDATE t SET v = v || 'z';
			COMMIT;
		`

		// Reference run: identical statements through the plain,
		// unintercepted default VFS, checkpointed so every committed page
		// lands in the .db file where it's easy to read back.
		plainPath := filepath.Join(dir, "plain.db")
		plain := openDB(plainPath, "")

		Expect(plain.Exec(insertSQL.String())).To(Succeed())
		Expect(plain.Exec("PRAGMA cache_size=3")).To(Succeed())
		Expect(plain.Exec(updateSQL)).To(Succeed())
		Expect(plain.Exec("PRAGMA wal_checkpoint(TRUNCATE)")).To(Succeed())

		pageSize := queryInt(plain, "PRAGMA page_size")
		pageCount := queryInt(plain, "PRAGMA page_count")
		Expect(queryText(plain, "SELECT v FROM t WHERE id = 1")).To(HaveSuffix("xyz"))
		referenceDB, err := os.ReadFile(plainPath)
		Expect(err).NotTo(HaveOccurred())

		// Intercepted run: same statements, small cache_size on the update
		// transaction, through a gate that records every proposal but
		// never rejects one.
		gate := &spyGate{}
		gatedVFSName := registerVFSWithGate(gate)
		gatedPath := filepath.Join(dir, "gated.db")
		gated := openDB(gatedPath, gatedVFSName)
		Expect(gated.Exec(insertSQL.String())).To(Succeed())
		Expect(gated.Exec("PRAGMA cache_size=3")).To(Succeed())
		Expect(gated.Exec(updateSQL)).To(Succeed())

		entries := gate.snapshot()
		Expect(len(entries)).To(BeNumerically(">=", 2),
			"expected at least one entry for the insert and one for the update")
		last := entries[len(entries)-1]
		Expect(int64(last.nTruncate)).To(Equal(pageCount),
			"the update proposal's nTruncate must be the post-commit database size")

		pages := map[uint32][]byte{}
		for _, p := range last.frames {
			pages[p.Header.PgNo] = p.Data
		}
		Expect(pages).NotTo(BeEmpty())
		for pgno, data := range pages {
			Expect(int64(len(data))).To(Equal(pageSize))
			offset := (int64(pgno) - 1) * pageSize
			Expect(data).To(Equal(referenceDB[offset:offset+pageSize]),
				"captured page %d must match its final on-disk content, not a stale pre-spill copy", pgno)
		}

		// The WAL itself must still be valid: reopening from scratch forces
		// recovery to actually validate the checksums that
		// walRewriteChecksums fixed up in place.
		Expect(gated.Close()).To(Succeed())
		reopened := openDB(gatedPath, gatedVFSName)
		Expect(queryText(reopened, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(reopened, "SELECT count(*) FROM t")).To(Equal(int64(rows)))
		Expect(queryText(reopened, "SELECT v FROM t WHERE id = 1")).To(HaveSuffix("xyz"))
	})
})

package vfs_test

import (
	"fmt"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// A committed transaction's frame headers can be rewritten in place to fix
// up checksums (same offset, same pgno, header only, never followed by a
// data write for that frame). A brand new transaction's first frame can
// coincidentally match on both offset and pgno too, though: a TRUNCATE
// checkpoint resets the WAL to empty, so the next transaction starts
// writing at the same low offsets as any transaction that began its own
// WAL epoch, and a transaction's first frame is often page 1 (the database
// header) or, as here, a table's freshly created root page.
//
// Offset+pgno-revisit tracking must tell that case apart from a genuine
// checksum-only rewrite of the same transaction's own frame, otherwise a
// brand new transaction's early frames get silently folded into the
// previous, already-completed transaction's stale bookkeeping: written to
// disk (so they're in the WAL and applied locally), but never captured or
// proposed to the gate, so those pages silently never replicate.
var _ = Describe("commit-frame interception across a checkpoint-truncated WAL", func() {
	It("still proposes every page of a transaction whose early frames reuse a just-committed transaction's offset and pgno", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "gated.db")

		gate := &spyGate{}
		gatedVFSName := registerVFSWithGate(gate)
		c := openDB(path, gatedVFSName)

		// First transaction of a fresh WAL epoch: its first two frames are
		// page 1 (the database header) and page 2 (the new table's root
		// page), at the WAL's two lowest frame offsets.
		Expect(c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		// Truncate the WAL back to empty. This is what a real checkpoint and
		// rewind does behind the wrapper's back in production, since a
		// separate component pokes the WAL file directly; TRUNCATE
		// reproduces the same effect through plain SQLite, with the
		// wrapper's in-memory tracking left unreset across it either way.
		Expect(c.Exec("PRAGMA wal_checkpoint(TRUNCATE)")).To(Succeed())

		before := len(gate.snapshot())

		// Second transaction of the new epoch: its first two frames are
		// also page 1 and page 2, at the exact same low offsets the CREATE
		// TABLE transaction used, colliding with the still-unreset, stale
		// tracking state left over from that transaction's commit.
		const rows = 2000
		var insertSQL strings.Builder
		insertSQL.WriteString("BEGIN;\n")
		for i := range rows {
			fmt.Fprintf(&insertSQL, "INSERT INTO t (v) VALUES ('%040d');\n", i)
		}
		insertSQL.WriteString("COMMIT;\n")
		Expect(c.Exec(insertSQL.String())).To(Succeed())

		entries := gate.snapshot()
		Expect(len(entries)).To(Equal(before+1),
			"the bulk insert's commit frame must reach the gate")

		pages := map[uint32]bool{}
		for _, frame := range entries[len(entries)-1] {
			pages[frame.Header.PgNo()] = true
		}
		Expect(pages[1]).To(BeTrue(),
			"page 1 must be part of the proposed transaction, not silently folded into the "+
				"previous (already-committed) transaction's stale checksum-rewrite tracking")
		Expect(pages[2]).To(BeTrue(),
			"page 2 must be part of the proposed transaction, not silently folded into the "+
				"previous (already-committed) transaction's stale checksum-rewrite tracking")

		Expect(queryInt(c, "SELECT count(*) FROM t")).To(Equal(int64(rows)))

		// The WAL and wal-index must also be self-consistent: reopening
		// from scratch forces recovery to actually validate what was
		// written, and a from-scratch integrity_check walks the on-disk
		// b-tree and freelist structure that page 1 anchors.
		Expect(c.Close()).To(Succeed())
		reopened := openDB(path, gatedVFSName)
		Expect(queryText(reopened, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(reopened, "SELECT count(*) FROM t")).To(Equal(int64(rows)))
	})
})

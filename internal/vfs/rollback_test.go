package vfs_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// SQLite's rollback only reverts mxFrame back to where the current write
// transaction began -- it does not touch the offset space beyond that. So
// a transaction that spills at least one frame and then rolls back (never
// reaching a commit frame) leaves the *next* transaction's first frame
// landing on the exact same WAL offset as the aborted transaction's first
// spilled frame, quite possibly for a different page.
//
// Offset-revisit tracking must tell that case apart from a genuine
// checksum-only rewrite of the *same* frame -- otherwise a brand new
// commit frame that reuses an old, rolled-back frame's offset gets
// misread as a stale checksum fixup: written to disk, but never captured
// or proposed to the gate, so the write commits locally while completely
// bypassing RAFT.
var _ = Describe("commit-frame interception across a rolled-back transaction", func() {
	It("still proposes a transaction whose first frame reuses a rolled-back transaction's offset", func() {
		dir := GinkgoT().TempDir()

		const rows = 500
		var setupSQL strings.Builder
		setupSQL.WriteString("BEGIN;\nCREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);\n")
		for i := range rows {
			fmt.Fprintf(&setupSQL, "INSERT INTO t (v) VALUES ('%040d');\n", i)
		}
		setupSQL.WriteString("COMMIT;\n")

		const spillAndRollbackSQL = `
			BEGIN;
			UPDATE t SET v = v || 'r';
			ROLLBACK;
		`

		const markerSQL = `
			BEGIN;
			UPDATE t SET v = 'MARKER' WHERE id = 1;
			COMMIT;
		`

		// Reference run: identical statements through the plain,
		// unintercepted default VFS.
		plainPath := filepath.Join(dir, "plain.db")
		plain := openDB(plainPath, "")
		Expect(plain.Exec(setupSQL.String())).To(Succeed())
		Expect(plain.Exec("PRAGMA cache_size=3")).To(Succeed())
		Expect(plain.Exec(spillAndRollbackSQL)).To(Succeed())
		Expect(plain.Exec(markerSQL)).To(Succeed())
		Expect(plain.Exec("PRAGMA wal_checkpoint(TRUNCATE)")).To(Succeed())

		pageSize := queryInt(plain, "PRAGMA page_size")
		Expect(queryText(plain, "SELECT v FROM t WHERE id = 1")).To(Equal("MARKER"))
		referenceDB, err := os.ReadFile(plainPath)
		Expect(err).NotTo(HaveOccurred())

		// Intercepted run: same statements through a gate that records
		// every proposal but never rejects one.
		gate := &spyGate{}
		gatedPath := filepath.Join(dir, "gated.db")
		gatedVFSName := registerVFSWithGate(gate)
		gated := openDB(gatedPath, gatedVFSName)
		Expect(gated.Exec(setupSQL.String())).To(Succeed())
		before := len(gate.snapshot())

		Expect(gated.Exec("PRAGMA cache_size=3")).To(Succeed())
		Expect(gated.Exec(spillAndRollbackSQL)).To(Succeed())
		Expect(len(gate.snapshot())).To(Equal(before),
			"a rolled-back transaction must never be proposed to the gate")

		Expect(gated.Exec(markerSQL)).To(Succeed())

		entries := gate.snapshot()
		Expect(len(entries)).To(Equal(before+1),
			"the marker transaction's commit frame must reach the gate, not be "+
				"mistaken for a checksum-only rewrite of the rolled-back transaction's frame")

		last := entries[len(entries)-1]
		Expect(last.frames).NotTo(BeEmpty())
		for _, frame := range last.frames {
			Expect(int64(len(frame.Data))).To(Equal(pageSize))
			offset := (int64(frame.Header.PgNo) - 1) * pageSize
			Expect(frame.Data).To(Equal(referenceDB[offset:offset+pageSize]),
				"captured page %d must match its final on-disk content", frame.Header.PgNo)
		}

		Expect(gated.Close()).To(Succeed())
		reopened := openDB(gatedPath, gatedVFSName)
		Expect(queryText(reopened, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(reopened, "SELECT count(*) FROM t")).To(Equal(int64(rows)))
		Expect(queryText(reopened, "SELECT v FROM t WHERE id = 1")).To(Equal("MARKER"))
	})
})

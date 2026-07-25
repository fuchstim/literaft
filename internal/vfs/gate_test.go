package vfs_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/fuchstim/literaft/internal/wal"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type capturedEntry struct {
	frames    []*wal.Frame
	nTruncate uint32
}

type spyGate struct {
	mu      sync.Mutex
	entries []capturedEntry
	reject  bool
}

func (g *spyGate) ProposeTransaction(frames []*wal.Frame, nTruncate uint32) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entries = append(g.entries, capturedEntry{frames, nTruncate})
	if g.reject {
		return errors.New("spyGate: rejected for test")
	}
	return nil
}

func (g *spyGate) setReject(v bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reject = v
}

func (g *spyGate) snapshot() []capturedEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.entries)
}

var _ = Describe("commit-frame interception", func() {
	It("captures exactly the frames SQLite writes for a committed transaction", func() {
		dir := GinkgoT().TempDir()
		const ddl = `
			CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
			INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');
		`

		// Reference run: identical statements through the plain,
		// unintercepted default VFS, checkpointed so every committed page
		// lands in the .db file where it's easy to read back.
		plainPath := filepath.Join(dir, "plain.db")
		plain := openDB(plainPath, "")
		Expect(plain.Exec(ddl)).To(Succeed())
		Expect(plain.Exec("PRAGMA wal_checkpoint(TRUNCATE)")).To(Succeed())

		pageSize := queryInt(plain, "PRAGMA page_size")
		pageCount := queryInt(plain, "PRAGMA page_count")
		referenceDB, err := os.ReadFile(plainPath)
		Expect(err).NotTo(HaveOccurred())

		// Intercepted run: same statements through a gate that records
		// every proposal but never rejects one.
		gate := &spyGate{}
		gatedVFSName := registerVFSWithGate(gate)
		gatedPath := filepath.Join(dir, "gated.db")
		gated := openDB(gatedPath, gatedVFSName)
		Expect(gated.Exec(ddl)).To(Succeed())

		entries := gate.snapshot()
		Expect(entries).NotTo(BeEmpty(), "gate must see at least one proposal for a committed write")

		last := entries[len(entries)-1]
		Expect(int64(last.nTruncate)).To(Equal(pageCount),
			"the final proposal's nTruncate must be the post-commit database size")

		pages := map[uint32][]byte{}
		for _, e := range entries {
			for _, f := range e.frames {
				pages[f.Header.PgNo] = f.Data
			}
		}
		Expect(pages).NotTo(BeEmpty())
		for pgno, data := range pages {
			Expect(int64(len(data))).To(Equal(pageSize))
			offset := (int64(pgno) - 1) * pageSize
			Expect(data).To(Equal(referenceDB[offset:offset+pageSize]),
				"captured page %d must match what SQLite actually persisted", pgno)
		}
	})

	It("leaves a clean, recoverable database when the gate rejects a commit frame", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "abort.db")

		gate := &spyGate{}
		gatedVFSName := registerVFSWithGate(gate)
		c := openDB(path, gatedVFSName)

		Expect(c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		Expect(c.Exec("INSERT INTO t (id, v) VALUES (1, 'first')")).To(Succeed())

		// Force a large, multi-page write so the rejected transaction spans
		// more than one WAL frame: earlier (non-commit) frames land on disk
		// immediately and must be left inert by the rejection, only the
		// final commit frame is withheld and discarded.
		gate.setReject(true)
		err := c.Exec("INSERT INTO t (id, v) VALUES (2, hex(randomblob(8000)))")
		Expect(err).To(HaveOccurred(), "a gate rejection must surface as a write/COMMIT failure")
		gate.setReject(false)

		snapshot := gate.snapshot()
		rejected := snapshot[len(snapshot)-1]
		Expect(len(rejected.frames)).To(BeNumerically(">", 1),
			"test setup should exercise a multi-frame transaction, not just the commit frame")

		// mxFrame never advanced: the rejected row must not be visible.
		Expect(queryInt(c, "SELECT count(*) FROM t")).To(Equal(int64(1)))

		// The db stays clean: the next transaction overwrites the inert
		// data frames cleanly rather than tripping over them.
		Expect(c.Exec("INSERT INTO t (id, v) VALUES (2, 'second')")).To(Succeed())
		Expect(queryInt(c, "SELECT count(*) FROM t")).To(Equal(int64(2)))
		Expect(queryText(c, "SELECT v FROM t WHERE id = 2")).To(Equal("second"))
		Expect(queryText(c, "PRAGMA integrity_check")).To(Equal("ok"))

		// Recoverable: closing and reopening from scratch sees the same,
		// consistent state -- the aborted transaction was never replayed.
		Expect(c.Close()).To(Succeed())

		reopened := openDB(path, gatedVFSName)
		Expect(queryInt(reopened, "SELECT count(*) FROM t")).To(Equal(int64(2)))
		Expect(queryText(reopened, "PRAGMA integrity_check")).To(Equal("ok"))
	})

	// A rejected commit leaves mxFrame unmoved, so a
	// same-shape retry lands on the exact same WAL offset(s) and pgno(s) as
	// the failed attempt. That retry must still be captured and proposed to
	// the gate, not mistaken for a checksum-only rewrite of an
	// already-approved transaction and written straight to disk.
	It("still proposes a retry that repeats the exact same statement after a rejection", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "retry.db")

		gate := &spyGate{}
		gatedVFSName := registerVFSWithGate(gate)
		c := openDB(path, gatedVFSName)
		Expect(c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		const stmt = "INSERT INTO t (id, v) VALUES (1, 'a')"

		gate.setReject(true)
		Expect(c.Exec(stmt)).To(HaveOccurred())
		gate.setReject(false)

		beforeRetry := len(gate.snapshot())

		// Same statement, same base state (the rejected txn never
		// committed): SQLite recomputes the identical page images at the
		// identical WAL offset, so a stale txnDone/headerPgno would wrongly
		// match this as a checksum rewrite of the rejected attempt.
		Expect(c.Exec(stmt)).To(Succeed())

		afterRetry := gate.snapshot()
		Expect(len(afterRetry)).To(Equal(beforeRetry+1),
			"the retry must reach gate.ProposeTransaction again, not bypass it as a stale checksum rewrite")
		Expect(queryInt(c, "SELECT count(*) FROM t")).To(Equal(int64(1)))
	})

	// The same bypass, but on what would be a follower: every rejection
	// reason (including a plain "not the leader") leaves txnDone/headerPgno
	// in the same stale state, so a second same-shape write attempt must
	// still reach the gate and be rejected again -- never silently accepted
	// with zero RAFT involvement.
	It("still rejects a same-shape retry after a not-leader-style rejection, never accepting it locally", func() {
		dir := GinkgoT().TempDir()
		path := filepath.Join(dir, "follower-retry.db")

		gate := &spyGate{}
		gatedVFSName := registerVFSWithGate(gate)
		c := openDB(path, gatedVFSName)
		Expect(c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		const stmt = "INSERT INTO t (id, v) VALUES (1, 'a')"

		gate.setReject(true)
		Expect(c.Exec(stmt)).To(HaveOccurred())

		beforeRetry := len(gate.snapshot())
		Expect(c.Exec(stmt)).To(HaveOccurred(),
			"a same-shape retry must still be proposed (and rejected) rather than bypassing the gate")
		Expect(len(gate.snapshot())).To(Equal(beforeRetry + 1))
		Expect(queryInt(c, "SELECT count(*) FROM t")).To(Equal(int64(0)))
	})
})

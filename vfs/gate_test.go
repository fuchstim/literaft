package vfs_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	raftvfs "github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ROADMAP.md M2: commit-frame interception with a stub gate. These tests
// register the wrapper VFS with a custom (non-default) Gate so they can
// observe exactly what the interception captured, and force the abort
// branch on demand -- neither is possible through the package's default
// AlwaysCommit registration used by the other test files.

// spyGate records every proposal it sees (whether or not it's told to
// reject it), so tests can inspect the captured frames independently of
// whatever data ended up on disk.
type spyGate struct {
	mu      sync.Mutex
	entries []raftvfs.Entry
	reject  bool
}

func (g *spyGate) Propose(e raftvfs.Entry) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entries = append(g.entries, e)
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

func (g *spyGate) snapshot() []raftvfs.Entry {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]raftvfs.Entry(nil), g.entries...)
}

// openGated registers a fresh literaft VFS instance backed by gate under a
// unique name and opens path through it. Each test needs its own
// registration since the gate is fixed at Wrap time.
func openGated(path string, gate raftvfs.Gate) *sqlite3.Conn {
	GinkgoHelper()
	name := "literaft-gate-test-" + filepath.Base(path)
	raftvfs.RegisterGate(name, sqlite3vfs.Find(""), gate)

	c, err := sqlite3.Open("file:" + path + "?vfs=" + name)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { c.Close() })
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
	Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())
	return c
}

var _ = Describe("commit-frame interception (M2)", func() {
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
		plain, err := sqlite3.Open("file:" + plainPath)
		Expect(err).NotTo(HaveOccurred())
		defer plain.Close()
		Expect(plain.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
		Expect(plain.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())
		Expect(plain.Exec(ddl)).To(Succeed())
		Expect(plain.Exec("PRAGMA wal_checkpoint(TRUNCATE)")).To(Succeed())

		pageSize := queryInt(plain, "PRAGMA page_size")
		pageCount := queryInt(plain, "PRAGMA page_count")
		referenceDB, err := os.ReadFile(plainPath)
		Expect(err).NotTo(HaveOccurred())

		// Intercepted run: same statements through a gate that records
		// every proposal but never rejects one.
		gate := &spyGate{}
		gated := openGated(filepath.Join(dir, "gated.db"), gate)
		Expect(gated.Exec(ddl)).To(Succeed())

		entries := gate.snapshot()
		Expect(entries).NotTo(BeEmpty(), "gate must see at least one proposal for a committed write")

		last := entries[len(entries)-1]
		Expect(int64(last.NTruncate)).To(Equal(pageCount),
			"the final proposal's nTruncate must be the post-commit database size")

		// Union all captured frames -- later frames for a page win, matching
		// "latest full image per page under total order" (docs/DECISIONS.md
		// ADR-003) -- and confirm every page named matches what the
		// reference run actually persisted at that page number.
		pages := map[uint32][]byte{}
		for _, e := range entries {
			for _, f := range e.Frames {
				pages[f.Pgno] = f.Page
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
		c := openGated(path, gate)
		Expect(c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		Expect(c.Exec("INSERT INTO t (id, v) VALUES (1, 'first')")).To(Succeed())

		// Force a large, multi-page write so the rejected transaction spans
		// more than one WAL frame: earlier (non-commit) frames land on disk
		// immediately and must be left inert by the rejection, only the
		// final commit frame is withheld and discarded
		// (docs/DESIGN.md §write path steps 2 and 5).
		gate.setReject(true)
		err := c.Exec("INSERT INTO t (id, v) VALUES (2, hex(randomblob(8000)))")
		Expect(err).To(HaveOccurred(), "a gate rejection must surface as a write/COMMIT failure")
		gate.setReject(false)

		snapshot := gate.snapshot()
		rejected := snapshot[len(snapshot)-1]
		Expect(len(rejected.Frames)).To(BeNumerically(">", 1),
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

		name := "literaft-gate-test-" + filepath.Base(path)
		reopened, err := sqlite3.Open("file:" + path + "?vfs=" + name)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { reopened.Close() })
		Expect(queryInt(reopened, "SELECT count(*) FROM t")).To(Equal(int64(2)))
		Expect(queryText(reopened, "PRAGMA integrity_check")).To(Equal("ok"))
	})
})

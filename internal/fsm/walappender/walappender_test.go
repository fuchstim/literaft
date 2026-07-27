package walappender_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/fsm/walappender"
	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWalappender(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "walappender Suite")
}

type recordingGate struct {
	entries [][]*wal.Frame
}

func (g *recordingGate) ProposeTransaction(frames []*wal.Frame) error {
	g.entries = append(g.entries, frames)
	return nil
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

// externalRead runs sql as a one-shot read-only query via the stock
// sqlite3 CLI -- an unmodified SQLite process outside this module
// entirely -- against a db this package built, not just opened.
func externalRead(path, sql string) (string, error) {
	sqlite3Path, err := exec.LookPath("sqlite3")
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, sqlite3Path, "-batch", "-readonly", "-noheader", "-list", path, sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	out := stdout.String()
	for len(out) > 0 && (out[len(out)-1] == '\n' || out[len(out)-1] == '\r') {
		out = out[:len(out)-1]
	}
	return out, nil
}

// leaderConn opens path through internal/vfs, registered with gate, and
// establishes WAL mode + synchronous=NORMAL, mirroring what a real driver
// connection does.
func leaderConn(path string, gate vfs.Gate) *sqlite3.Conn {
	GinkgoHelper()
	name := "literaft-walappender-test-" + filepath.Base(path)
	vfs.Register(name, sqlite3vfs.Find(""), gate, hclog.NewNullLogger())

	c, err := sqlite3.Open("file:" + path + "?vfs=" + name)
	Expect(err).NotTo(HaveOccurred())
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
	Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())
	return c
}

// primeFollowerWALMode establishes path's WAL-mode identity (page 1's WAL
// marker) via a plain connection before walappender.Open ever touches
// -wal/-shm: walappender materializes frames without a SQLite connection
// in the loop at all, and a fresh, schema-less main db file gives it
// nothing to be recognized as WAL-mode against.
func primeFollowerWALMode(path string) {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + path)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
}

// singleRowUpdates opens a leader connection, creates a one-row table, then
// issues n updates to that same row -- each producing its own small,
// autocommitted transaction -- returning the table-setup transactions and
// the update transactions separately, plus the page size.
func singleRowUpdates(dir string, n int) (setup, updates [][]*wal.Frame, pageSize uint32) {
	GinkgoHelper()

	gate := &recordingGate{}
	leaderPath := filepath.Join(dir, "leader.db")
	leader := leaderConn(leaderPath, gate)
	defer leader.Close()

	Expect(leader.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
	Expect(leader.Exec("INSERT INTO t (id, v) VALUES (1, 'seed')")).To(Succeed())
	setup = gate.entries
	gate.entries = nil

	for i := 0; i < n; i++ {
		Expect(leader.Exec(fmt.Sprintf("UPDATE t SET v = 'v%d' WHERE id = 1", i))).To(Succeed())
	}
	updates = gate.entries

	return setup, updates, uint32(queryInt(leader, "PRAGMA page_size"))
}

// walFileSize stats path's -wal file, failing the test if it can't.
func walFileSize(path string) int64 {
	GinkgoHelper()
	fi, err := os.Stat(path + "-wal")
	Expect(err).NotTo(HaveOccurred())
	return fi.Size()
}

// walFileSalt reads path's -wal file's own on-disk header salt (bytes
// 16:24). A rewind always picks a fresh random salt; a plain append leaves
// it untouched -- since a rewound epoch reuses the same file bytes an
// unrewound append might also still occupy, comparing file size alone
// can't reliably tell the two apart, but the salt always can.
func walFileSalt(path string) [8]byte {
	GinkgoHelper()
	f, err := os.Open(path + "-wal")
	Expect(err).NotTo(HaveOccurred())
	defer f.Close()
	var salt [8]byte
	_, err = f.ReadAt(salt[:], 16)
	Expect(err).NotTo(HaveOccurred())
	return salt
}

var _ = Describe("WALAppender.AppendTransaction", func() {
	It("replays a leader's captured entries into a fresh follower with identical reads", func() {
		dir := GinkgoT().TempDir()

		gate := &recordingGate{}
		leaderPath := filepath.Join(dir, "leader.db")
		leader := leaderConn(leaderPath, gate)
		defer leader.Close()

		Expect(leader.Exec(`
			CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
			INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');
		`)).To(Succeed())
		Expect(leader.Exec("UPDATE t SET v = 'z' WHERE id = 2")).To(Succeed())
		Expect(leader.Exec("DELETE FROM t WHERE id = 3")).To(Succeed())

		Expect(gate.entries).NotTo(BeEmpty())
		pageSize := queryInt(leader, "PRAGMA page_size")

		followerPath := filepath.Join(dir, "follower.db")
		primeFollowerWALMode(followerPath)

		appender, err := walappender.Open(followerPath, uint32(pageSize), -1, 0, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		for i, txn := range gate.entries {
			Expect(appender.AppendFrames(txn, nil)).To(Succeed(), "applying entry %d", i)
		}

		follower, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()

		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(follower, "SELECT count(*) FROM t")).To(Equal(queryInt(leader, "SELECT count(*) FROM t")))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal("a"))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 2")).To(Equal("z"))
		Expect(queryInt(follower, "SELECT count(*) FROM t WHERE id = 3")).To(Equal(int64(0)))

		// Fail loudly, not skip: a Skip here would let an environment
		// missing the sqlite3 CLI pass quietly instead of surfacing that
		// external-reader compatibility was never checked for a
		// walappender-built db specifically.
		if _, err := exec.LookPath("sqlite3"); err != nil {
			Fail("stock sqlite3 CLI not found in PATH; required to re-run the external-reader check against a walappender-built db")
		}

		check, err := externalRead(followerPath, "PRAGMA integrity_check")
		Expect(err).NotTo(HaveOccurred())
		Expect(check).To(Equal("ok"))

		rows, err := externalRead(followerPath, "SELECT id, v FROM t ORDER BY id")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal("1|a\n2|z"))
	})

	// Wal-index page 0 (frames 1-4062) is special-cased versus every later
	// page, but the round-trip test above only ever produces a handful of
	// frames -- never enough to exercise the page != 0 branches. A
	// regression here would silently corrupt any sufficiently large apply,
	// undetected.
	It("replays enough frames to overflow wal-index page 0 into page 1", func() {
		dir := GinkgoT().TempDir()

		gate := &recordingGate{}
		leaderPath := filepath.Join(dir, "leader-multipage.db")
		leader := leaderConn(leaderPath, gate)
		defer leader.Close()

		Expect(leader.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		const rows = 5000
		for i := 0; i < rows; i++ {
			Expect(leader.Exec(fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		var totalFrames int
		for _, txn := range gate.entries {
			totalFrames += len(txn)
		}
		Expect(totalFrames).To(BeNumerically(">", 4062),
			"test setup must produce enough frames to actually exercise wal-index page 1")

		pageSize := queryInt(leader, "PRAGMA page_size")

		followerPath := filepath.Join(dir, "follower-multipage.db")
		primeFollowerWALMode(followerPath)

		appender, err := walappender.Open(followerPath, uint32(pageSize), -1, 0, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		for i, txn := range gate.entries {
			Expect(appender.AppendFrames(txn, nil)).To(Succeed(), "applying entry %d", i)
		}

		follower, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()

		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(follower, "SELECT count(*) FROM t")).To(Equal(int64(rows)))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal("row0"))
		Expect(queryText(follower, fmt.Sprintf("SELECT v FROM t WHERE id = %d", rows))).To(Equal(fmt.Sprintf("row%d", rows-1)))
	})
})

// copyFile copies src to dst byte-for-byte, failing the test on any error.
func copyFile(src, dst string) {
	GinkgoHelper()
	b, err := os.ReadFile(src)
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(dst, b, 0o666)).To(Succeed())
}

// crashImage snapshots dbPath's .db + -wal while conn is still open --
// a byte-exact image of what a SIGKILL would leave behind, with committed
// frames still in an un-checkpointed -wal -- into a fresh path, and returns
// it. The -shm is intentionally not copied: a crash can leave it stale or
// gone, and SQLite rebuilds it from the -wal at open regardless.
func crashImage(dir, srcPath string) string {
	GinkgoHelper()
	dstPath := filepath.Join(dir, "crash.db")
	copyFile(srcPath, dstPath)
	copyFile(srcPath+"-wal", dstPath+"-wal")
	fi, err := os.Stat(dstPath + "-wal")
	Expect(err).NotTo(HaveOccurred())
	Expect(fi.Size()).To(BeNumerically(">", walHeaderSizeConst),
		"crash image must have a non-empty -wal, or it doesn't exercise recovery")
	return dstPath
}

// walHeaderSizeConst mirrors the unexported walHeaderSize rather than
// importing it.
const walHeaderSizeConst = 32

// WAL recovery at open. A publish-after-commit failure on the leader is
// fatal (a panic), so the process restarts on a crash image: committed
// frames sit in a non-empty -wal whose wal-index was never published.
// walappender.Open refuses such a WAL on its own ("recovery from an existing
// WAL isn't implemented yet"), and relies on a SQLite connection having
// recovered the wal-index first -- exactly what fsm.New's open ordering
// guarantees (open + WAL-mode PRAGMAs on its own connection, kept open,
// before opening the WALAppender). These tests pin that ordering.
var _ = Describe("WALAppender WAL recovery at open", func() {
	It("opens a crash-left non-empty -wal once a SQLite connection has recovered it first", func() {
		dir := GinkgoT().TempDir()

		gate := &recordingGate{}
		leaderPath := filepath.Join(dir, "leader-crash.db")
		leader := leaderConn(leaderPath, gate)
		Expect(leader.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		Expect(leader.Exec("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')")).To(Succeed())
		pageSize := uint32(queryInt(leader, "PRAGMA page_size"))

		// Snapshot the files while the connection is open, then close: the
		// image has committed frames in an un-checkpointed -wal, like a crash.
		crashPath := crashImage(dir, leaderPath)
		Expect(leader.Close()).To(Succeed())

		// Mirror fsm.New's open ordering: a plain SQLite connection opens and
		// enters WAL mode first (recovering the wal-index from the -wal) and
		// stays open, holding the shm alive, before the WALAppender opens.
		recoverConn, err := sqlite3.Open("file:" + crashPath)
		Expect(err).NotTo(HaveOccurred())
		defer recoverConn.Close()
		Expect(recoverConn.Exec("PRAGMA journal_mode=WAL")).To(Succeed())

		appender, err := walappender.Open(crashPath, pageSize, -1, 0, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred(),
			"Open must not reject the crash-left -wal once SQLite has recovered the wal-index")
		defer appender.Close()

		// The recovered state is intact and further apply still works.
		Expect(queryInt(recoverConn, "SELECT count(*) FROM t")).To(Equal(int64(2)))

		var followUp [][]*wal.Frame
		func() {
			g := &recordingGate{}
			p := filepath.Join(dir, "src.db")
			c := leaderConn(p, g)
			defer c.Close()
			Expect(c.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
			Expect(c.Exec("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')")).To(Succeed())
			g.entries = nil
			Expect(c.Exec("INSERT INTO t (id, v) VALUES (3, 'c')")).To(Succeed())
			followUp = g.entries
		}()
		for _, txn := range followUp {
			Expect(appender.AppendFrames(txn, nil)).To(Succeed())
		}
		Expect(queryInt(recoverConn, "SELECT count(*) FROM t")).To(Equal(int64(3)))
		Expect(queryText(recoverConn, "PRAGMA integrity_check")).To(Equal("ok"))
	})
})

var _ = Describe("WALAppender log rewind", func() {
	// Mirrors an internal, unexported constant rather than importing it.
	const frameHeaderSize = 24

	It("keeps the follower -wal bounded across many checkpoint/rewind cycles, instead of growing forever", func() {
		dir := GinkgoT().TempDir()

		const numUpdates = 30
		setup, updates, pageSize := singleRowUpdates(dir, numUpdates)

		followerPath := filepath.Join(dir, "follower-bounded.db")
		primeFollowerWALMode(followerPath)

		// Threshold of 1: every applied transaction is immediately
		// followed by a PASSIVE checkpoint attempt, giving the very next
		// apply the best possible chance to rewind.
		appender, err := walappender.Open(followerPath, pageSize, 1, 0, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		for i, txn := range setup {
			Expect(appender.AppendFrames(txn, nil)).To(Succeed(), "applying setup entry %d", i)
		}

		var peak int64
		for i, txn := range updates {
			Expect(appender.AppendFrames(txn, nil)).To(Succeed(), "applying update %d", i)
			if size := walFileSize(followerPath); size > peak {
				peak = size
			}
		}

		// Without a rewind, numUpdates single-row updates would leave the
		// -wal at roughly numUpdates*(frameHeaderSize+pageSize) bytes. With
		// it, no single cycle between successful rewinds should ever need
		// more than a couple of transactions' worth of frames, regardless
		// of how many updates ran in total.
		unboundedEstimate := int64(numUpdates) * int64(frameHeaderSize+pageSize)
		Expect(peak).To(BeNumerically("<", unboundedEstimate/4),
			"peak -wal size %d bytes looks like it grew proportionally to all %d updates instead of staying bounded",
			peak, numUpdates)

		follower, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()
		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal(fmt.Sprintf("v%d", numUpdates-1)))
	})

	It("keeps the follower -wal bounded for loaned (forwarded-write) appends via the release-path checkpoint, with no ticker", func() {
		dir := GinkgoT().TempDir()

		const numUpdates = 30
		setup, updates, pageSize := singleRowUpdates(dir, numUpdates)

		followerPath := filepath.Join(dir, "follower-loaned-bounded.db")
		primeFollowerWALMode(followerPath)

		// Ticker disabled (interval 0), so the only thing that can checkpoint a
		// loaned append is the threshold check that runs when its lock is
		// released. Threshold of 1 so every released loan attempts a checkpoint,
		// letting the next apply rewind -- exactly the self-locked behavior, but
		// reached through the forwarded-write path. Before the release-path
		// checkpoint, a loaned append skipped the threshold entirely, so with no
		// ticker nothing ever checkpointed and the -wal grew without bound.
		appender, err := walappender.Open(followerPath, pageSize, 1, 0, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		// applyLoaned materializes txn the way a forwarded write does: under a
		// lock acquired up front and released only after the append, so the
		// threshold checkpoint runs on release rather than under the (would-be
		// round-trip) hold.
		applyLoaned := func(txn []*wal.Frame) {
			GinkgoHelper()
			h, err := appender.AcquireWriteLock(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(appender.AppendFramesUnderLock(h, txn, nil)).To(Succeed())
			h.Release()
		}

		for _, txn := range setup {
			applyLoaned(txn)
		}

		var peak int64
		for _, txn := range updates {
			applyLoaned(txn)
			if size := walFileSize(followerPath); size > peak {
				peak = size
			}
		}

		unboundedEstimate := int64(numUpdates) * int64(frameHeaderSize+pageSize)
		Expect(peak).To(BeNumerically("<", unboundedEstimate/4),
			"peak -wal size %d bytes looks like it grew proportionally to all %d loaned updates instead of staying bounded by the release-path checkpoint",
			peak, numUpdates)

		follower, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()
		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal(fmt.Sprintf("v%d", numUpdates-1)))
	})

	It("does not rewind while a reader holds an older snapshot, and resumes once it's released", func() {
		dir := GinkgoT().TempDir()

		const numUpdates = 14
		setup, updates, pageSize := singleRowUpdates(dir, numUpdates)

		followerPath := filepath.Join(dir, "follower-contended.db")
		primeFollowerWALMode(followerPath)

		// Threshold of 3, not 1: large enough that a single update doesn't
		// immediately trigger its own checkpoint, leaving a window where
		// nBackfill lags maxFrame. A reader connecting in that window must
		// claim a real, non-zero read-mark rather than the mark-0
		// fallback, which only applies once nBackfill is fully caught up.
		appender, err := walappender.Open(followerPath, pageSize, 3, 0, hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		for i, txn := range setup {
			Expect(appender.AppendFrames(txn, nil)).To(Succeed(), "applying setup entry %d", i)
		}

		Expect(appender.AppendFrames(updates[0], nil)).To(Succeed())
		saltBeforeReader := walFileSalt(followerPath)

		// Attach an external reader and hold a read transaction open while
		// nBackfill is stale relative to the current maxFrame, forcing it
		// to claim a real snapshot mark rather than mark 0.
		reader, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()
		Expect(reader.Exec("BEGIN")).To(Succeed())
		Expect(queryInt(reader, "SELECT count(*) FROM t")).To(Equal(int64(1)))

		// Apply more updates while the reader is attached: the log must
		// never actually rewind -- the epoch's salt, which only a rewind
		// ever changes, must stay identical throughout.
		const attachedUpdates = 8
		for i := 1; i <= attachedUpdates; i++ {
			Expect(appender.AppendFrames(updates[i], nil)).To(Succeed(), "applying update %d with reader attached", i)
			Expect(walFileSalt(followerPath)).To(Equal(saltBeforeReader),
				"expected the WAL epoch to be unchanged while a reader holds an older snapshot (update %d)", i)
		}

		// Release the reader, then keep applying until a rewind is
		// observed -- exactly which call it lands on depends on how the
		// dirty-page count lines up with the threshold.
		Expect(reader.Exec("COMMIT")).To(Succeed())

		rewound := false
		for i := attachedUpdates + 1; i < numUpdates; i++ {
			Expect(appender.AppendFrames(updates[i], nil)).To(Succeed(), "applying update %d after reader release", i)
			if walFileSalt(followerPath) != saltBeforeReader {
				rewound = true
			}
		}
		Expect(rewound).To(BeTrue(), "expected the log to eventually rewind to a fresh epoch once nothing blocks it")

		follower, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()
		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal(fmt.Sprintf("v%d", numUpdates-1)))
	})
})

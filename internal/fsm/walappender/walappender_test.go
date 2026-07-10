package walappender_test

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/fsm/walappender"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWalappender(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "walappender Suite")
}

// recordingGate implements vfs.Gate, recording every proposal (as the
// raftproto.Entry wire shape a real Gate would produce) rather than ever
// rejecting one -- gate_test.go's "always commit but observe everything"
// stand-in, reused here to drive a follower's AppendEntry with exactly
// what a leader connection actually captured.
type recordingGate struct {
	entries []*raftproto.Entry
}

func (g *recordingGate) Propose(frames []*vfs.Frame, nTruncate uint32) error {
	g.entries = append(g.entries, &raftproto.Entry{NodeID: "leader", Frames: frames, NTruncate: nTruncate})
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

// pageSizeProbe returns SQLite's actual default page size by asking a
// throwaway in-memory connection, rather than assuming a value (CLAUDE.md:
// verify, don't assume). internal/vfs.File.isFrameHeaderOffset uses the
// pageSize passed to Register directly to compute frame-header offsets
// (walHeaderSize + n*(frameHeaderSize+pageSize)), not just to enforce a
// mismatch -- unlike the old package, passing 0 here doesn't merely
// disable enforcement, it breaks offset detection outright, so every test
// in this file must register with the real page size.
func pageSizeProbe() uint32 {
	GinkgoHelper()
	c, err := sqlite3.Open(":memory:")
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return uint32(queryInt(c, "PRAGMA page_size"))
}

// leaderConn opens path through internal/vfs, registered with gate, and
// establishes WAL mode + synchronous=NORMAL, mirroring what a real driver
// connection does.
func leaderConn(path string, gate vfs.Gate) *sqlite3.Conn {
	GinkgoHelper()
	name := "literaft-walappender-test-" + filepath.Base(path)
	vfs.Register(name, sqlite3vfs.Find(""), gate, pageSizeProbe())

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

var _ = Describe("WALAppender.AppendEntry", func() {
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

		appender, err := walappender.Open(followerPath, uint32(pageSize), -1, 0)
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		for i, e := range gate.entries {
			Expect(appender.AppendEntry(e)).To(Succeed(), "applying entry %d", i)
		}

		follower, err := sqlite3.Open("file:" + followerPath)
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()

		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(follower, "SELECT count(*) FROM t")).To(Equal(queryInt(leader, "SELECT count(*) FROM t")))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal("a"))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 2")).To(Equal("z"))
		Expect(queryInt(follower, "SELECT count(*) FROM t WHERE id = 3")).To(Equal(int64(0)))

		// Fail loudly, not skip: CLAUDE.md calls external-reader
		// compatibility the single most load-bearing verification in the
		// project. A Skip here would let an environment missing the
		// sqlite3 CLI pass quietly instead of surfacing that this claim
		// was never checked for a walappender-built db specifically.
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

	// walindex.go's frameZero and hashTableOffsets both special-case
	// wal-index page 0 (frames 1-4062) versus every later page, but the
	// round-trip test above only ever produces a handful of frames -- never
	// enough to exercise the page != 0 branches. A regression here (e.g. an
	// off-by-one in hashtableNPageOne+(page-1)*hashtableNPage) would
	// silently corrupt any sufficiently large apply, undetected.
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
		for _, e := range gate.entries {
			totalFrames += len(e.Frames)
		}
		Expect(totalFrames).To(BeNumerically(">", 4062),
			"test setup must produce enough frames to actually exercise wal-index page 1")

		pageSize := queryInt(leader, "PRAGMA page_size")

		followerPath := filepath.Join(dir, "follower-multipage.db")
		primeFollowerWALMode(followerPath)

		appender, err := walappender.Open(followerPath, uint32(pageSize), -1, 0)
		Expect(err).NotTo(HaveOccurred())
		defer appender.Close()

		for i, e := range gate.entries {
			Expect(appender.AppendEntry(e)).To(Succeed(), "applying entry %d", i)
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

package apply_test

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/apply"
	raftvfs "github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
// entirely, the same load-bearing check as vfs's M1 tests
// (docs/ROADMAP.md M1), re-run here against a db this package built.
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

// ROADMAP.md M3: an entry captured on one instance applies on another and
// both serve identical reads, including to an external reader (re-run M1
// against an apply-built db).
var _ = Describe("follower apply (M3)", func() {
	It("replays a leader's captured entries into a fresh follower with identical reads", func() {
		dir := GinkgoT().TempDir()

		// Leader: a normal literaft connection, but registered with a
		// recording gate so the test can get at exactly what M2 captured,
		// the same way gate_test.go does for M2.
		var entries []raftvfs.Entry
		gateName := "literaft-apply-test-leader"
		raftvfs.RegisterGate(gateName, sqlite3vfs.Find(""), raftvfs.GateFunc(func(e raftvfs.Entry) error {
			entries = append(entries, e)
			return nil
		}))

		leaderPath := filepath.Join(dir, "leader.db")
		leader, err := sqlite3.Open("file:" + leaderPath + "?vfs=" + gateName)
		Expect(err).NotTo(HaveOccurred())
		defer leader.Close()
		Expect(leader.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
		Expect(leader.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())
		Expect(leader.Exec(`
			CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
			INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');
		`)).To(Succeed())
		Expect(leader.Exec("UPDATE t SET v = 'z' WHERE id = 2")).To(Succeed())
		Expect(leader.Exec("DELETE FROM t WHERE id = 3")).To(Succeed())

		Expect(entries).NotTo(BeEmpty())
		pageSize := queryInt(leader, "PRAGMA page_size")

		// Follower: a completely fresh db file. Establishing WAL mode is
		// still SQLite's job, not apply/'s -- it's what the CLAUDE.md
		// invariant "keep >=1 RW connection open per node process" is
		// really buying beyond just keeping the shm live: without it, the
		// main .db file never gets the page-1 bytes marking it as WAL
		// mode, and a fresh connection has nothing to notice the -wal
		// file for at all. apply/ only ever touches -wal/-shm from here.
		followerPath := filepath.Join(dir, "follower.db")
		keeper, err := sqlite3.Open("file:" + followerPath + "?vfs=literaft")
		Expect(err).NotTo(HaveOccurred())
		defer keeper.Close()
		Expect(keeper.Exec("PRAGMA journal_mode=WAL")).To(Succeed())

		applier, err := apply.Open(followerPath, uint32(pageSize))
		Expect(err).NotTo(HaveOccurred())
		defer applier.Close()

		for i, e := range entries {
			Expect(applier.Apply(e)).To(Succeed(), "applying entry %d", i)
		}

		follower, err := sqlite3.Open("file:" + followerPath + "?vfs=literaft")
		Expect(err).NotTo(HaveOccurred())
		defer follower.Close()

		Expect(queryText(follower, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(follower, "SELECT count(*) FROM t")).To(Equal(queryInt(leader, "SELECT count(*) FROM t")))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal("a"))
		Expect(queryText(follower, "SELECT v FROM t WHERE id = 2")).To(Equal("z"))
		Expect(queryInt(follower, "SELECT count(*) FROM t WHERE id = 3")).To(Equal(int64(0)))

		// Fail loudly, not skip: this re-runs M1's load-bearing
		// external-reader check (CLAUDE.md/ROADMAP.md: "the whole premise
		// depends on it") against an apply-built db specifically. A Skip
		// here would let an environment missing the sqlite3 CLI pass
		// quietly instead of surfacing that this claim was never checked.
		if _, err := exec.LookPath("sqlite3"); err != nil {
			Fail("stock sqlite3 CLI not found in PATH; required to re-run M1 against an apply-built db")
		}

		check, err := externalRead(followerPath, "PRAGMA integrity_check")
		Expect(err).NotTo(HaveOccurred())
		Expect(check).To(Equal("ok"))

		rows, err := externalRead(followerPath, "SELECT id, v FROM t ORDER BY id")
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal("1|a\n2|z"))
	})
})

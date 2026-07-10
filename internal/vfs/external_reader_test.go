//go:build darwin || linux

package vfs_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// externalTimeout bounds every interaction with the external sqlite3 CLI.
// A hang here is itself a finding (the load-bearing lock-interop claim
// failed), so tests must fail with a clear diagnostic instead of wedging
// the whole suite.
const externalTimeout = 10 * time.Second

// Prove, before building anything else on top, that an *unmodified* stock
// SQLite process can open our wrapper-VFS-written database read-only and
// stay correct concurrently with local writes and checkpoints. This is
// requirement #3, and the load-bearing risk is lock interop: ncruces'
// default VFS uses OFD locks (F_OFD_SETLK), stock SQLite's unix VFS uses
// classic POSIX advisory locks (F_SETLK). These tests drive the real
// system `sqlite3` CLI as the external reader, not another ncruces
// connection.

var sqlite3Path string

func init() {
	sqlite3Path, _ = exec.LookPath("sqlite3")
}

// requireExternalSQLite fails loudly, rather than skipping, if this
// environment can't run the external-reader compatibility checks: CLAUDE.md
// calls this the single most load-bearing verification in the project. A
// Skip here would let a CI environment missing the sqlite3 CLI (or lacking
// the file-locking/shared-memory support requirement #3 depends on) pass
// quietly instead of surfacing that the one claim the whole architecture is
// staked on was never actually checked.
func requireExternalSQLite() {
	GinkgoHelper()
	if sqlite3Path == "" {
		Fail("stock sqlite3 CLI not found in PATH; required for external-reader compatibility tests")
	}
	if !sqlite3vfs.SupportsFileLocking || !sqlite3vfs.SupportsSharedMemory {
		Fail("platform lacks the file locking or shared memory support requirement #3 depends on")
	}
}

// externalRead runs sql as a one-shot read-only query via the stock sqlite3
// CLI and returns stdout with surrounding whitespace trimmed. Each call is
// its own process and its own SQLite connection: a fresh external reader.
func externalRead(path, sql string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), externalTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, sqlite3Path, "-batch", "-readonly", "-noheader", "-list", path, sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("external reader timed out after %s (lock interop hang?): %w", externalTimeout, ctx.Err())
		}
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	return trimTrailingNewline(stdout.String()), nil
}

func trimTrailingNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// externalSession is a long-lived external reader: a stock sqlite3 CLI
// process fed statements over stdin, one at a time, so it can hold an open
// transaction (and therefore a pinned WAL read-mark) across multiple
// queries -- the case that matters for checkpoint interop.
type externalSession struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	stderr *bytes.Buffer
}

func startExternalReader(path string) (*externalSession, error) {
	cmd := exec.Command(sqlite3Path, "-batch", "-readonly", "-noheader", "-list", path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &externalSession{cmd: cmd, stdin: stdin, stdout: bufio.NewScanner(stdoutPipe), stderr: &stderr}, nil
}

// exec sends sql to the CLI's stdin. The trailing ";" is required: the CLI
// reads stdin line by line and only recognizes a statement as complete once
// it sees a terminating semicolon, so a bare "BEGIN\n" just accumulates as
// a partial statement forever instead of executing.
func (s *externalSession) exec(sql string) error {
	_, err := io.WriteString(s.stdin, sql+";\n")
	return err
}

// queryLine sends sql, which must produce exactly one output line, and
// returns it. Bounded by externalTimeout: a hang here means the external
// reader never got to respond, which is itself the finding to report.
func (s *externalSession) queryLine(sql string) (string, error) {
	if err := s.exec(sql); err != nil {
		return "", err
	}

	scanned := make(chan bool, 1)
	go func() { scanned <- s.stdout.Scan() }()

	select {
	case ok := <-scanned:
		if !ok {
			if err := s.stdout.Err(); err != nil {
				return "", err
			}
			return "", fmt.Errorf("sqlite3 CLI exited early: %s", s.stderr.String())
		}
		return s.stdout.Text(), nil
	case <-time.After(externalTimeout):
		return "", fmt.Errorf("timed out after %s waiting for external reader response to %q (lock interop hang?); stderr so far: %s",
			externalTimeout, sql, s.stderr.String())
	}
}

// close closes stdin (EOF) and waits for the CLI to exit. If it doesn't
// exit within externalTimeout -- e.g. because an earlier query genuinely
// deadlocked rather than merely erroring -- the process is killed so a
// stuck external reader fails the test instead of hanging the suite.
func (s *externalSession) close() error {
	s.stdin.Close()

	waited := make(chan error, 1)
	go func() { waited <- s.cmd.Wait() }()

	select {
	case err := <-waited:
		if err != nil {
			return fmt.Errorf("%w: %s", err, s.stderr.String())
		}
		return nil
	case <-time.After(externalTimeout):
		s.cmd.Process.Kill()
		return fmt.Errorf("external reader did not exit within %s after close (lock interop hang?); stderr so far: %s",
			externalTimeout, s.stderr.String())
	}
}

var _ = Describe("external reader compatibility", func() {
	BeforeEach(requireExternalSQLite)

	It("never sees an in-flight, uncommitted local write", func() {
		path := filepath.Join(GinkgoT().TempDir(), "m1_uncommitted.db")
		writer := open(path)
		Expect(writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")).To(Succeed())
		Expect(writer.Exec("INSERT INTO t (id, v) VALUES (1, 1)")).To(Succeed())

		Expect(writer.Exec("BEGIN IMMEDIATE")).To(Succeed())
		Expect(writer.Exec("INSERT INTO t (id, v) VALUES (2, 2)")).To(Succeed())

		out, err := externalRead(path, "SELECT count(*) FROM t")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("1"), "external reader must not see the writer's uncommitted insert")

		Expect(writer.Exec("COMMIT")).To(Succeed())

		out, err = externalRead(path, "SELECT count(*) FROM t")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("2"))
	})

	It("stays correct under a concurrent write storm: no torn reads, no lock-interop errors", func() {
		path := filepath.Join(GinkgoT().TempDir(), "m1_storm.db")
		writer := open(path)
		Expect(writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")).To(Succeed())
		Expect(writer.Exec("INSERT INTO t (id, v) VALUES (1, 0)")).To(Succeed())

		const writes = 300
		done := make(chan error, 1)
		go func() {
			for i := 1; i <= writes; i++ {
				if err := writer.Exec(fmt.Sprintf("UPDATE t SET v = %d WHERE id = 1", i)); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()

		// Poll a fixed number of times regardless of how fast the writer
		// finishes, so the read count doesn't depend on winning a race
		// against in-process WASM execution (which can complete all 300
		// writes before a single subprocess spawn returns).
		const reads = 40
		var lastSeen int64 = -1
		for range reads {
			check, rerr := externalRead(path, "PRAGMA integrity_check")
			Expect(rerr).NotTo(HaveOccurred(), "external reader errored (lock interop broken?)")
			Expect(check).To(Equal("ok"), "external reader observed a corrupt/torn database mid-write")

			val, rerr := externalRead(path, "SELECT v FROM t WHERE id = 1")
			Expect(rerr).NotTo(HaveOccurred())
			n, perr := strconv.ParseInt(val, 10, 64)
			Expect(perr).NotTo(HaveOccurred())
			Expect(n).To(BeNumerically(">=", lastSeen), "external reader saw v go backwards: visibility/locking is broken")
			lastSeen = n
		}

		Expect(<-done).NotTo(HaveOccurred())

		out, err := externalRead(path, "SELECT v FROM t WHERE id = 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal(strconv.Itoa(writes)))
	})

	It("lets an external reader keep its snapshot across a local checkpoint", func() {
		path := filepath.Join(GinkgoT().TempDir(), "m1_checkpoint.db")
		writer := open(path)
		Expect(writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")).To(Succeed())
		Expect(writer.Exec("INSERT INTO t (id, v) VALUES (1, 1), (2, 2), (3, 3)")).To(Succeed())

		reader, err := startExternalReader(path)
		Expect(err).NotTo(HaveOccurred())
		defer reader.close()

		Expect(reader.exec("BEGIN")).To(Succeed())
		baseline, err := reader.queryLine("SELECT count(*) FROM t")
		Expect(err).NotTo(HaveOccurred())
		Expect(baseline).To(Equal("3"))

		// Grow and checkpoint the WAL while the external reader's snapshot
		// (and the read-mark backing it) is still pinned to the pre-growth
		// state.
		for i := range 2000 {
			Expect(writer.Exec(fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, %d)", 100+i, i))).To(Succeed())
		}
		Expect(writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)")).To(Succeed())

		stillBaseline, err := reader.queryLine("SELECT count(*) FROM t")
		Expect(err).NotTo(HaveOccurred())
		Expect(stillBaseline).To(Equal("3"),
			"checkpoint disturbed a snapshot the external reader's read-mark should have pinned")

		Expect(reader.exec("COMMIT")).To(Succeed())
		Expect(reader.close()).To(Succeed())

		out, err := externalRead(path, "SELECT count(*) FROM t")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("2003"))
	})
})

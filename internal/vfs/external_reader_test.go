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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const externalTimeout = 10 * time.Second

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
	return strings.TrimSpace(stdout.String()), nil
}

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

func (s *externalSession) exec(sql string) error {
	_, err := io.WriteString(s.stdin, sql+";\n")
	return err
}

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
	It("never sees an in-flight, uncommitted local write", func() {
		path := filepath.Join(GinkgoT().TempDir(), "m1_uncommitted.db")
		writer := openDB(path, vfsName)
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
		writer := openDB(path, vfsName)
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
		writer := openDB(path, vfsName)
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

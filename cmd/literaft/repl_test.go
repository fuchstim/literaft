package main

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/node"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// freeAddr grabs an OS-assigned loopback port, mirroring the identical
// helper in internal/node's own test files (unexported there, so it can't
// be reused from this package).
func freeAddr() string {
	GinkgoHelper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	addr := l.Addr().String()
	Expect(l.Close()).To(Succeed())
	return addr
}

// startSingleNode brings up a one-node, self-bootstrapped cluster -- real
// enough (same node.Start production path) to run the REPL against a live
// commit-frame gate, without a multi-node cluster's complexity.
func startSingleNode(dir, id string) *node.Node {
	GinkgoHelper()
	addr := freeAddr()
	cfg := node.Config{
		ID:       id,
		BindAddr: addr,
		DataDir:  filepath.Join(dir, id),
		DBPath:   filepath.Join(dir, id+".db"),
		PageSize: 4096,
		Bootstrap: []hraft.Server{
			{Suffrage: hraft.Voter, ID: hraft.ServerID(id), Address: hraft.ServerAddress(addr)},
		},
		ApplyTimeout: 2 * time.Second,
	}
	n, err := node.Start(cfg)
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() bool { return n.Ready() }, 5*time.Second, 10*time.Millisecond).Should(BeTrue())
	return n
}

// startJoiningNode brings up a node not yet part of any cluster (Bootstrap
// left nil), for exercising ".addvoter" against a real second raft member
// rather than just asserting on hraft.Raft.AddVoter's return value alone.
func startJoiningNode(dir, id string) (*node.Node, string) {
	GinkgoHelper()
	addr := freeAddr()
	cfg := node.Config{
		ID:           id,
		BindAddr:     addr,
		DataDir:      filepath.Join(dir, id),
		DBPath:       filepath.Join(dir, id+".db"),
		PageSize:     4096,
		ApplyTimeout: 2 * time.Second,
	}
	n, err := node.Start(cfg)
	Expect(err).NotTo(HaveOccurred())
	return n, addr
}

func rowCount(n *node.Node) (int64, error) {
	var count int64
	err := n.WithDB(func(c *sqlite3.Conn) error {
		stmt, _, err := c.Prepare("SELECT count(*) FROM t")
		if err != nil {
			return err
		}
		defer stmt.Close()
		if !stmt.Step() {
			return fmt.Errorf("no rows")
		}
		count = stmt.ColumnInt64(0)
		return nil
	})
	return count, err
}

var _ = Describe("runREPL", func() {
	var n *node.Node

	BeforeEach(func() {
		n = startSingleNode(GinkgoT().TempDir(), "solo")
		DeferCleanup(func() { Expect(n.Shutdown()).To(Succeed()) })
	})

	It("creates a table, inserts rows, and queries them back", func() {
		var out strings.Builder
		runREPL(n, strings.NewReader(
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);\n"+
				"INSERT INTO t (v) VALUES ('a');\n"+
				"SELECT id, v FROM t;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("OK"))
		Expect(out.String()).To(ContainSubstring("id\tv"))
		Expect(out.String()).To(ContainSubstring("1\ta"))
	})

	It("assembles a statement split across multiple lines", func() {
		var out strings.Builder
		runREPL(n, strings.NewReader(
			"CREATE TABLE t (\n  id INTEGER PRIMARY KEY\n);\n"+
				"SELECT count(*) FROM t;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("count(*)"))
		Expect(out.String()).To(ContainSubstring("0"))
	})

	It("reports NULL and blob columns without mangling them", func() {
		var out strings.Builder
		runREPL(n, strings.NewReader(
			"SELECT NULL, x'ff00';\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("NULL"))
		Expect(out.String()).To(ContainSubstring("<blob 2 bytes>"))
	})

	It("reports an error for invalid SQL and keeps accepting input", func() {
		var out strings.Builder
		runREPL(n, strings.NewReader(
			"NOT VALID SQL;\n"+
				"SELECT 1;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("error:"))
		Expect(out.String()).To(ContainSubstring("1"))
	})

	It("stops on .exit without requiring EOF, reporting an explicit exit", func() {
		var out strings.Builder
		var explicitExit bool
		r, w := io.Pipe()

		done := make(chan struct{})
		go func() {
			defer close(done)
			explicitExit = runREPL(n, r, &out)
		}()

		_, err := w.Write([]byte(".exit\n"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(done, 2*time.Second).Should(BeClosed())
		Expect(w.Close()).To(Succeed())
		Expect(explicitExit).To(BeTrue())
	})

	It("reports an incomplete statement left over at EOF, and does not report an explicit exit", func() {
		var out strings.Builder
		explicitExit := runREPL(n, strings.NewReader("SELECT 1"), &out)

		Expect(out.String()).To(ContainSubstring("unexpected EOF"))
		Expect(explicitExit).To(BeFalse())
	})

	It("does not report an explicit exit for a plain EOF, e.g. stdin from /dev/null under a headless launch", func() {
		explicitExit := runREPL(n, strings.NewReader(""), io.Discard)

		Expect(explicitExit).To(BeFalse())
	})

	It("reports usage for a malformed .addvoter command", func() {
		var out strings.Builder
		runREPL(n, strings.NewReader(".addvoter onlyid\n"), &out)

		Expect(out.String()).To(ContainSubstring("usage: .addvoter"))
	})

	It("adds a new voter via .addvoter, which then replicates writes", func() {
		joiner, addr := startJoiningNode(GinkgoT().TempDir(), "joiner")
		DeferCleanup(func() { Expect(joiner.Shutdown()).To(Succeed()) })

		var out strings.Builder
		runREPL(n, strings.NewReader(
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);\n"+
				".addvoter joiner "+addr+"\n"+
				"INSERT INTO t (v) VALUES ('a');\n",
		), &out)

		Expect(out.String()).NotTo(ContainSubstring("error:"))
		Expect(out.String()).To(ContainSubstring("OK"))

		Eventually(func() (int64, error) { return rowCount(joiner) }, 5*time.Second, 20*time.Millisecond).
			Should(Equal(int64(1)))
	})
})

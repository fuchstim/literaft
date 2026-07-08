package main

import (
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"

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

	It("stops on .exit without requiring EOF", func() {
		var out strings.Builder
		r, w := io.Pipe()

		done := make(chan struct{})
		go func() {
			defer close(done)
			runREPL(n, r, &out)
		}()

		_, err := w.Write([]byte(".exit\n"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(done, 2*time.Second).Should(BeClosed())
		Expect(w.Close()).To(Succeed())
	})

	It("reports an incomplete statement left over at EOF", func() {
		var out strings.Builder
		runREPL(n, strings.NewReader("SELECT 1"), &out)

		Expect(out.String()).To(ContainSubstring("unexpected EOF"))
	})
})

package main

import (
	"io"
	"strings"
	"time"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// startSoloNode brings up a one-node, self-bootstrapped cluster -- real
// enough (the same production wiring testutils.NewTCPCluster mirrors) to
// run the REPL against a live commit-frame gate, without a multi-node
// cluster's complexity.
func startSoloNode() (*testutils.TCPCluster, *testutils.Node) {
	GinkgoHelper()
	c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 1)
	n := c.ReadyLeader()
	return c, n
}

func rowCount(n *testutils.Node) (int64, error) {
	var count int64
	err := n.DB.QueryRow("SELECT count(*) FROM t").Scan(&count)
	return count, err
}

var _ = Describe("runREPL", func() {
	var c *testutils.TCPCluster
	var n *testutils.Node

	BeforeEach(func() {
		c, n = startSoloNode()
		DeferCleanup(func() { c.Shutdown() })
	})

	It("creates a table, inserts rows, and queries them back", func() {
		var out strings.Builder
		runREPL(n.Raft, n.DB, strings.NewReader(
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
		runREPL(n.Raft, n.DB, strings.NewReader(
			"CREATE TABLE t (\n  id INTEGER PRIMARY KEY\n);\n"+
				"SELECT count(*) FROM t;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("count(*)"))
		Expect(out.String()).To(ContainSubstring("0"))
	})

	It("reports NULL and blob columns without mangling them", func() {
		var out strings.Builder
		runREPL(n.Raft, n.DB, strings.NewReader(
			"SELECT NULL, x'ff00';\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("NULL"))
		Expect(out.String()).To(ContainSubstring("<blob 2 bytes>"))
	})

	It("reports an error for invalid SQL and keeps accepting input", func() {
		var out strings.Builder
		runREPL(n.Raft, n.DB, strings.NewReader(
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
			explicitExit = runREPL(n.Raft, n.DB, r, &out)
		}()

		_, err := w.Write([]byte(".exit\n"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(done, 2*time.Second).Should(BeClosed())
		Expect(w.Close()).To(Succeed())
		Expect(explicitExit).To(BeTrue())
	})

	It("reports an incomplete statement left over at EOF, and does not report an explicit exit", func() {
		var out strings.Builder
		explicitExit := runREPL(n.Raft, n.DB, strings.NewReader("SELECT 1"), &out)

		Expect(out.String()).To(ContainSubstring("unexpected EOF"))
		Expect(explicitExit).To(BeFalse())
	})

	It("does not report an explicit exit for a plain EOF, e.g. stdin from /dev/null under a headless launch", func() {
		explicitExit := runREPL(n.Raft, n.DB, strings.NewReader(""), io.Discard)

		Expect(explicitExit).To(BeFalse())
	})

	It("reports usage for a malformed .addvoter command", func() {
		var out strings.Builder
		runREPL(n.Raft, n.DB, strings.NewReader(".addvoter onlyid\n"), &out)

		Expect(out.String()).To(ContainSubstring("usage: .addvoter"))
	})

	It("adds a new voter via .addvoter, which then replicates writes", func() {
		joiner := c.Join(GinkgoT(), "joiner")

		var out strings.Builder
		runREPL(n.Raft, n.DB, strings.NewReader(
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);\n"+
				".addvoter joiner "+string(joiner.Addr)+"\n"+
				"INSERT INTO t (v) VALUES ('a');\n",
		), &out)

		Expect(out.String()).NotTo(ContainSubstring("error:"))
		Expect(out.String()).To(ContainSubstring("OK"))

		Eventually(func() (int64, error) { return rowCount(joiner) }, 5*time.Second, 20*time.Millisecond).
			Should(Equal(int64(1)))
	})
})

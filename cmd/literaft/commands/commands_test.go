package commands

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCommands(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cmd/literaft/commands Suite")
}

func startSoloNode() (*testutils.TCPCluster, *testutils.Node) {
	GinkgoHelper()
	c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 1)
	n := c.ReadyLeader()
	return c, n
}

func rowCount(n *testutils.Node, table string) (int64, error) {
	var count int64
	err := n.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count)
	return count, err
}

var _ = Describe("CommandHandler.RunREPL", func() {
	var c *testutils.TCPCluster
	var n *testutils.Node
	var h *CommandHandler

	BeforeEach(func() {
		c, n = startSoloNode()
		h = NewCommandHandler(n.Raft, n.DB)
		DeferCleanup(func() { c.Shutdown() })
	})

	It("creates a table, inserts rows, and queries them back", func() {
		var out strings.Builder
		h.RunREPL(strings.NewReader(
			"CREATE TABLE widgets (id INTEGER PRIMARY KEY, v TEXT);\n"+
				"INSERT INTO widgets (v) VALUES ('hello');\n"+
				"SELECT id, v FROM widgets;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("OK"))
		Expect(out.String()).To(ContainSubstring("id"))
		Expect(out.String()).To(ContainSubstring("hello"))
	})

	It("assembles a statement split across multiple lines", func() {
		var out strings.Builder
		h.RunREPL(strings.NewReader(
			"CREATE TABLE widgets (\n  id INTEGER PRIMARY KEY\n);\n"+
				"SELECT count(*) AS n FROM widgets;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("n"))
		Expect(out.String()).To(ContainSubstring("0"))
	})

	It("reports NULL and blob columns without mangling them", func() {
		var out strings.Builder
		h.RunREPL(strings.NewReader("SELECT NULL, x'ff00';\n"), &out)

		Expect(out.String()).To(ContainSubstring("NULL"))
		Expect(out.String()).To(ContainSubstring("<blob 2 bytes>"))
	})

	It("reports an error for invalid SQL and keeps accepting input", func() {
		var out strings.Builder
		h.RunREPL(strings.NewReader(
			"NOT VALID SQL;\n"+
				"SELECT 42 AS answer;\n",
		), &out)

		Expect(out.String()).To(ContainSubstring("Error:"))
		Expect(out.String()).To(ContainSubstring("42"))
	})

	It("stops on .exit without requiring EOF, reporting an explicit exit", func() {
		var out strings.Builder
		var explicitExit bool
		r, w := io.Pipe()

		done := make(chan struct{})
		go func() {
			defer close(done)
			explicitExit = h.RunREPL(r, &out)
		}()

		_, err := w.Write([]byte(".exit\n"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(done, 2*time.Second).Should(BeClosed())
		Expect(w.Close()).To(Succeed())
		Expect(explicitExit).To(BeTrue())
		Expect(out.String()).To(ContainSubstring("Goodbye"))
	})

	It("reports an incomplete statement left over at EOF, and does not report an explicit exit", func() {
		var out strings.Builder
		explicitExit := h.RunREPL(strings.NewReader("SELECT 1"), &out)

		Expect(out.String()).To(ContainSubstring("unexpected EOF"))
		Expect(explicitExit).To(BeFalse())
	})

	It("does not report an explicit exit for a plain EOF, e.g. stdin from /dev/null under a headless launch", func() {
		explicitExit := h.RunREPL(strings.NewReader(""), io.Discard)

		Expect(explicitExit).To(BeFalse())
	})
})

var _ = Describe("CommandHandler.Handle", func() {
	var c *testutils.TCPCluster
	var n *testutils.Node
	var h *CommandHandler

	BeforeEach(func() {
		c, n = startSoloNode()
		h = NewCommandHandler(n.Raft, n.DB)
		DeferCleanup(func() { c.Shutdown() })
	})

	It("runs a bare SQL statement", func() {
		var out strings.Builder
		Expect(h.Handle("SELECT 7 AS lucky;", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("7"))
	})

	It("returns true and says goodbye on .exit and .quit", func() {
		var out strings.Builder
		Expect(h.Handle(".exit", &out)).To(BeTrue())
		Expect(out.String()).To(ContainSubstring("Goodbye"))

		out.Reset()
		Expect(h.Handle(".quit", &out)).To(BeTrue())
		Expect(out.String()).To(ContainSubstring("Goodbye"))
	})

	It("lists the registered commands on .help", func() {
		var out strings.Builder
		Expect(h.Handle(".help", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("Available commands:"))
		Expect(out.String()).To(ContainSubstring(".tables"))
		Expect(out.String()).To(ContainSubstring(".addvoter"))
		Expect(out.String()).To(ContainSubstring(".help"))
		Expect(out.String()).To(ContainSubstring(".exit"))
	})

	It("reports an unknown command and shows help", func() {
		var out strings.Builder
		Expect(h.Handle(".bogus", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("unknown command: .bogus"))
		Expect(out.String()).To(ContainSubstring("Available commands:"))
	})

	It("lists tables with .tables", func() {
		var out strings.Builder
		h.Handle("CREATE TABLE widgets (id INTEGER PRIMARY KEY);", &out)
		out.Reset()

		Expect(h.Handle(".tables", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("widgets"))
	})

	It("prints the schema with .schema", func() {
		var out strings.Builder
		h.Handle("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);", &out)
		out.Reset()

		Expect(h.Handle(".schema", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("CREATE TABLE widgets"))
	})

	It("lists cluster servers with .servers", func() {
		var out strings.Builder
		Expect(h.Handle(".servers", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("Address"))
		Expect(out.String()).To(ContainSubstring("n0"))
		Expect(out.String()).To(ContainSubstring("Voter"))
	})

	It("takes a snapshot with .snapshot", func() {
		var out strings.Builder
		// Commit an entry first so there is something new to snapshot.
		h.Handle("CREATE TABLE widgets (id INTEGER PRIMARY KEY);", &out)
		out.Reset()

		Expect(h.Handle(".snapshot", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("OK"))
	})

	It("executes SQL from a file with .load", func() {
		var out strings.Builder
		h.Handle("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);", &out)
		out.Reset()

		path := filepath.Join(GinkgoT().TempDir(), "seed.sql")
		Expect(os.WriteFile(path, []byte("INSERT INTO widgets (name) VALUES ('loaded');\n"), 0o600)).To(Succeed())

		Expect(h.Handle(".load "+path, &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("OK"))
		Expect(rowCount(n, "widgets")).To(Equal(int64(1)))
	})

	It("reports the error from .load for a missing file", func() {
		var out strings.Builder
		Expect(h.Handle(".load /no/such/file.sql", &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("Error:"))
		Expect(out.String()).To(ContainSubstring("failed to read file"))
	})

	DescribeTable("reports usage for a malformed command",
		func(line, wantUsage string) {
			var out strings.Builder
			Expect(h.Handle(line, &out)).To(BeFalse())
			Expect(out.String()).To(ContainSubstring(wantUsage))
		},
		Entry(".addvoter with one argument", ".addvoter onlyid", ".addvoter <id> <address>"),
		Entry(".removevoter with no arguments", ".removevoter", ".removevoter <id>"),
		Entry(".tables with a stray argument", ".tables extra", ".tables"),
		Entry(".schema with a stray argument", ".schema extra", ".schema"),
		Entry(".servers with a stray argument", ".servers extra", ".servers"),
		Entry(".snapshot with a stray argument", ".snapshot extra", ".snapshot"),
		Entry(".load with no arguments", ".load", ".load <filename>"),
	)
})

var _ = Describe("cluster membership commands", func() {
	It("adds a new voter via .addvoter, which then replicates writes", func() {
		c, n := startSoloNode()
		DeferCleanup(func() { c.Shutdown() })
		h := NewCommandHandler(n.Raft, n.DB)

		joiner := c.Join(GinkgoT(), "joiner")

		var out strings.Builder
		h.RunREPL(strings.NewReader(
			"CREATE TABLE widgets (id INTEGER PRIMARY KEY, v TEXT);\n"+
				".addvoter joiner "+string(joiner.Addr)+"\n"+
				"INSERT INTO widgets (v) VALUES ('a');\n",
		), &out)

		Expect(out.String()).NotTo(ContainSubstring("Error:"))
		Expect(out.String()).To(ContainSubstring("OK"))

		Eventually(func() (int64, error) { return rowCount(joiner, "widgets") }, 5*time.Second, 20*time.Millisecond).
			Should(Equal(int64(1)))
	})

	It("removes a voter via .removevoter", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3)
		DeferCleanup(func() { c.Shutdown() })
		n := c.ReadyLeader()
		h := NewCommandHandler(n.Raft, n.DB)

		var target string
		for _, s := range n.Raft.GetConfiguration().Configuration().Servers {
			if string(s.ID) != n.ID {
				target = string(s.ID)
				break
			}
		}
		Expect(target).NotTo(BeEmpty())

		var out strings.Builder
		Expect(h.Handle(".removevoter "+target, &out)).To(BeFalse())
		Expect(out.String()).To(ContainSubstring("OK"))

		Eventually(func() []string {
			var ids []string
			for _, s := range n.Raft.GetConfiguration().Configuration().Servers {
				ids = append(ids, string(s.ID))
			}
			return ids
		}, 5*time.Second, 20*time.Millisecond).ShouldNot(ContainElement(target))
	})
})

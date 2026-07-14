package raftgate_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "raft/gate Suite")
}

// capturedTxn is one call to funcGate's ProposeTransaction, recorded
// verbatim.
type capturedTxn struct {
	frames    []*vfs.Frame
	nTruncate uint32
}

// funcGate adapts a function to vfs.Gate.
type funcGate func(frames []*vfs.Frame, nTruncate uint32) error

func (fn funcGate) ProposeTransaction(frames []*vfs.Frame, nTruncate uint32) error {
	return fn(frames, nTruncate)
}

// captureTransactions runs each of stmts as its own committed transaction
// against a fresh, local, single-node connection through internal/vfs --
// entirely independent of any Gate under test -- via a gate stub that only
// records what it captures. It returns realistic, valid page content in
// commit order, ready to feed straight into Gate.ProposeTransaction so
// these tests can exercise Gate's own entry-building and self-skip logic
// against real data instead of fabricated bytes.
func captureTransactions(pageSize uint32, stmts ...string) []capturedTxn {
	GinkgoHelper()
	var got []capturedTxn
	gate := funcGate(func(frames []*vfs.Frame, nTruncate uint32) error {
		got = append(got, capturedTxn{frames, nTruncate})
		return nil
	})

	name := "raftgate-test-capture-" + uuid.NewString()
	vfs.Register(name, sqlite3vfs.Find(""), gate, pageSize, hclog.NewNullLogger())

	path := filepath.Join(GinkgoT().TempDir(), "capture.db")
	c, err := sqlite3.Open("file:" + path + "?vfs=" + name)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
	Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())

	for _, s := range stmts {
		Expect(c.Exec(s)).To(Succeed())
	}
	return got
}

// fakeLog is a stub raftgate.LogAdapter that records every wire entry it
// receives and, if apply is set, defers each call's outcome to it -- letting
// a test observe or act on the FSM mid-call, e.g. to simulate hraft applying
// a self-authored entry before ProposeTransaction itself returns.
type fakeLog struct {
	mu      sync.Mutex
	entries [][]byte
	apply   func(entry []byte) error
}

func (l *fakeLog) Apply(entry []byte) error {
	l.mu.Lock()
	l.entries = append(l.entries, entry)
	hook := l.apply
	l.mu.Unlock()

	if hook != nil {
		return hook(entry)
	}
	return nil
}

func (l *fakeLog) snapshot() [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][]byte(nil), l.entries...)
}

// tableExists reports whether name exists in the SQLite database at dbPath,
// opened as a fresh, unwrapped connection independent of any FSM or Gate
// under test.
func tableExists(dbPath, name string) bool {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + dbPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	stmt, _, err := c.Prepare("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?")
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.BindText(1, name)).To(Succeed())
	Expect(stmt.Step()).To(BeTrue())
	return stmt.ColumnInt64(0) > 0
}

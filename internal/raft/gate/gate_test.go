package raftgate_test

import (
	"errors"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	"google.golang.org/protobuf/proto"

	"github.com/fuchstim/literaft/fsm"
	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// newTestFSM returns a real fsm.FSM over a fresh temp-dir SQLite file -- no
// raft involved, since Gate's own logic (entry encoding, self-skip
// bracketing, LastRejection) doesn't need a cluster to exercise. Cluster-
// level concerns (leadership, Ready/drain, Figure-8) live in the log
// package's tests instead, since that's where they're now implemented.
func newTestFSM() *fsm.FSM {
	GinkgoHelper()
	dir := GinkgoT().TempDir()
	f, err := fsm.New(filepath.Join(dir, "node.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { f.Close() })
	return f
}

var _ = Describe("Gate", func() {
	It("builds a wire entry from the captured frames, with a fresh header id per proposal", func() {
		f := newTestFSM()
		l := &fakeLog{}
		gate := raftgate.New(f, l)

		txns := captureTransactions(f.PageSize(), "CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(txns).To(HaveLen(1))
		txn := txns[0]

		Expect(gate.ProposeTransaction(txn.frames, txn.nTruncate)).To(Succeed())
		Expect(gate.ProposeTransaction(txn.frames, txn.nTruncate)).To(Succeed())

		entries := l.snapshot()
		Expect(entries).To(HaveLen(2))

		var first, second raftproto.Entry
		Expect(proto.Unmarshal(entries[0], &first)).To(Succeed())
		Expect(proto.Unmarshal(entries[1], &second)).To(Succeed())

		for _, e := range []*raftproto.Entry{&first, &second} {
			got := e.GetTransaction()
			Expect(got).NotTo(BeNil())
			Expect(got.NTruncate).To(Equal(txn.nTruncate))
			Expect(got.Pages).To(HaveLen(len(txn.frames)))
			for i, p := range got.Pages {
				Expect(p.Pgno).To(Equal(txn.frames[i].Pgno))
				Expect(p.Data).To(Equal(txn.frames[i].Page))
			}
		}

		Expect(first.GetHeader().GetId()).NotTo(BeEmpty())
		Expect(second.GetHeader().GetId()).NotTo(BeEmpty())
		Expect(first.GetHeader().GetId()).NotTo(Equal(second.GetHeader().GetId()),
			"each proposal must get its own self-apply skip marker")
	})

	// The self-apply skip marker (fsm.FSM.SkipEntry/UnskipEntry) must stay
	// transient, scoped to exactly the one ProposeTransaction call that set
	// it -- see CLAUDE.md's "self-apply skip must stay transient" gotcha.
	It("skips materializing its own entry on the FSM only while ProposeTransaction is in flight", func() {
		f := newTestFSM()
		txn := captureTransactions(f.PageSize(), "CREATE TABLE t (id INTEGER PRIMARY KEY)")[0]

		var duringApply bool
		l := &fakeLog{}
		l.apply = func(entry []byte) error {
			// Simulate hraft applying this exact entry synchronously, as it
			// would for a single-node cluster's own proposer before
			// ProposeTransaction itself returns.
			f.Apply(&raft.Log{Data: entry})
			duringApply = tableExists(f.DBPath(), "t")
			return nil
		}
		gate := raftgate.New(f, l)

		Expect(gate.ProposeTransaction(txn.frames, txn.nTruncate)).To(Succeed())
		Expect(duringApply).To(BeFalse(), "the proposer's own entry must not materialize while still in flight")

		// The marker is scoped to that one call: once ProposeTransaction has
		// returned, replaying the exact same entry (as a follower or a
		// later resend would) must materialize normally.
		entries := l.snapshot()
		Expect(entries).To(HaveLen(1))
		f.Apply(&raft.Log{Data: entries[0]})
		Expect(tableExists(f.DBPath(), "t")).To(BeTrue())
	})

	// ProposeTransaction's concrete error doesn't reliably survive the round
	// trip back through *sqlite3.Conn.Exec/Stmt.Step, so LastRejection is
	// the mechanism a caller holding the Gate directly should actually use.
	It("exposes the most recent rejection via LastRejection, clearing it on the next success", func() {
		f := newTestFSM()
		txn := captureTransactions(f.PageSize(), "CREATE TABLE t (id INTEGER PRIMARY KEY)")[0]

		l := &fakeLog{apply: func(entry []byte) error { return errors.New("fakeLog: rejected") }}
		gate := raftgate.New(f, l)

		Expect(gate.LastRejection()).To(BeNil(), "no proposal attempted yet")

		err := gate.ProposeTransaction(txn.frames, txn.nTruncate)
		Expect(err).To(HaveOccurred())
		Expect(gate.LastRejection()).To(Equal(err))

		l.apply = nil
		Expect(gate.ProposeTransaction(txn.frames, txn.nTruncate)).To(Succeed())
		Expect(gate.LastRejection()).To(BeNil(), "a successful proposal must clear the previous rejection")
	})

	// vfs.File relies on errors.As to recover a *gateError's carried sqlite
	// code (internal/vfs/file.go), so Gate's own error wrapping must not
	// break that chain.
	It("wraps a LogAdapter error without breaking errors.Is discovery of the concrete cause", func() {
		f := newTestFSM()
		txn := captureTransactions(f.PageSize(), "CREATE TABLE t (id INTEGER PRIMARY KEY)")[0]

		sentinel := vfs.GateError(errors.New("catching up"), sqlite3.ExtendedErrorCode(sqlite3.BUSY))
		l := &fakeLog{apply: func(entry []byte) error { return sentinel }}
		gate := raftgate.New(f, l)

		err := gate.ProposeTransaction(txn.frames, txn.nTruncate)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, sentinel)).To(BeTrue(),
			"a rejected LogAdapter error must stay discoverable through Gate's own wrap")
	})
})

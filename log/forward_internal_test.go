package log

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	"google.golang.org/protobuf/proto"

	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This file lives in package log (not log_test) so it can drive ForwardingLog
// against fakes. Its Describe blocks register into the same suite the
// external log_test package's RunSpecs (log_suite_test.go) runs.

// --- fakes -----------------------------------------------------------------

type fakeInner struct {
	isLeader   bool
	leaderAddr raft.ServerAddress
	apply      func(entry []byte) error
	applyCalls int
}

func (f *fakeInner) Apply(entry []byte) error {
	f.applyCalls++
	if f.apply != nil {
		return f.apply(entry)
	}
	return nil
}
func (f *fakeInner) IsLeader() bool                 { return f.isLeader }
func (f *fakeInner) LeaderAddr() raft.ServerAddress { return f.leaderAddr }

type fakeTarget struct {
	lastApplied uint64
	pageSize    uint32
	await       func(ctx context.Context, id string) error
	begin       func(ctx context.Context, id string) (func(), error)
	beginCalls  int
	releases    int
}

func (t *fakeTarget) LastApplied() uint64 { return t.lastApplied }
func (t *fakeTarget) PageSize() uint32    { return t.pageSize }
func (t *fakeTarget) AwaitEntryApplied(ctx context.Context, id string) error {
	if t.await != nil {
		return t.await(ctx, id)
	}
	return nil
}
func (t *fakeTarget) BeginHeldApply(ctx context.Context, id string) (func(), error) {
	t.beginCalls++
	if t.begin != nil {
		return t.begin(ctx, id)
	}
	return func() { t.releases++ }, nil
}

type fakeTransport struct {
	handler func(ctx context.Context, req []byte) ([]byte, error)
	propose func(ctx context.Context, leader raft.ServerAddress, req []byte) ([]byte, error)
	calls   int
}

func (t *fakeTransport) Handle(h func(ctx context.Context, req []byte) ([]byte, error)) {
	t.handler = h
}
func (t *fakeTransport) Propose(ctx context.Context, leader raft.ServerAddress, req []byte) ([]byte, error) {
	t.calls++
	return t.propose(ctx, leader, req)
}

// entryBytes marshals a one-page committed transaction entry with id.
func entryBytes(id string, pageSize uint32) []byte {
	GinkgoHelper()
	e := &raftproto.Entry{
		Header: &raftproto.Header{Id: id},
		Payload: &raftproto.Entry_Transaction{Transaction: &raftproto.Transaction{
			Pages:     []*raftproto.Page{{Pgno: 1, Data: make([]byte, pageSize)}},
			NTruncate: 1,
		}},
	}
	b, err := proto.Marshal(e)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func respBytes(r *raftproto.ForwardResponse) []byte {
	GinkgoHelper()
	b, err := proto.Marshal(r)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func newFL(inner *fakeInner, tr *fakeTransport, tg *fakeTarget) *ForwardingLog {
	o := defaultOptions()
	fl := &ForwardingLog{
		inner:              inner,
		transport:          tr,
		target:             tg,
		forwardTimeout:     o.forwardTimeout,
		handlerLockTimeout: o.handlerLockTimeout,
	}
	tr.Handle(fl.handle)
	return fl
}

// --- follower (Apply) path -------------------------------------------------

var _ = Describe("ForwardingLog follower Apply path", func() {
	const pageSize = 4096

	It("passes a leader's successful proposal straight through without forwarding", func() {
		inner := &fakeInner{apply: func([]byte) error { return nil }}
		tr := &fakeTransport{}
		fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})

		Expect(fl.Apply(entryBytes("x", pageSize))).To(Succeed())
		Expect(tr.calls).To(Equal(0), "a leader must not forward")
	})

	It("returns a non-NotLeader inner error as-is without forwarding", func() {
		boom := errors.New("boom")
		inner := &fakeInner{apply: func([]byte) error { return boom }}
		tr := &fakeTransport{}
		fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})

		Expect(fl.Apply(entryBytes("x", pageSize))).To(MatchError(boom))
		Expect(tr.calls).To(Equal(0))
	})

	It("does not forward a NotLeaderError with no leader hint", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{} }}
		tr := &fakeTransport{}
		fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})

		err := fl.Apply(entryBytes("x", pageSize))
		var nl *NotLeaderError
		Expect(errors.As(err, &nl)).To(BeTrue())
		Expect(tr.calls).To(Equal(0))
	})

	It("forwards on NotLeaderError, stamping the base index, and succeeds on OK once consumed", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "leader:1"} }}
		var gotBase uint64
		var gotLeader raft.ServerAddress
		tr := &fakeTransport{propose: func(_ context.Context, leader raft.ServerAddress, req []byte) ([]byte, error) {
			gotLeader = leader
			fr := &raftproto.ForwardRequest{}
			Expect(proto.Unmarshal(req, fr)).To(Succeed())
			gotBase = fr.GetBaseIndex()
			return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_OK}), nil
		}}
		consumed := false
		tg := &fakeTarget{pageSize: pageSize, lastApplied: 7, await: func(context.Context, string) error {
			consumed = true
			return nil
		}}
		fl := newFL(inner, tr, tg)

		Expect(fl.Apply(entryBytes("x", pageSize))).To(Succeed())
		Expect(gotLeader).To(Equal(raft.ServerAddress("leader:1")))
		Expect(gotBase).To(Equal(uint64(7)), "base index must be the follower's lastApplied")
		Expect(consumed).To(BeTrue(), "OK must dual-wait on local consumption")
	})

	It("surfaces STALE_BASE as a retryable (sqlite3.BUSY) StaleBaseError", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "leader:1"} }}
		tr := &fakeTransport{propose: func(context.Context, raft.ServerAddress, []byte) ([]byte, error) {
			return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STALE_BASE, LastApplied: 99}), nil
		}}
		fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})

		err := fl.Apply(entryBytes("x", pageSize))
		var stale *StaleBaseError
		Expect(errors.As(err, &stale)).To(BeTrue())
		Expect(stale.LeaderLastApplied).To(Equal(uint64(99)))
		assertRetryable(err)
	})

	It("surfaces CATCHING_UP and BUSY as retryable errors", func() {
		for _, tc := range []struct {
			status raftproto.ForwardResponse_Status
		}{{raftproto.ForwardResponse_CATCHING_UP}, {raftproto.ForwardResponse_BUSY}} {
			inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "leader:1"} }}
			status := tc.status
			tr := &fakeTransport{propose: func(context.Context, raft.ServerAddress, []byte) ([]byte, error) {
				return respBytes(&raftproto.ForwardResponse{Status: status}), nil
			}}
			fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})
			assertRetryable(fl.Apply(entryBytes("x", pageSize)))
		}
	})

	It("re-resolves and re-sends exactly once on NOT_LEADER with a fresh hint", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "stale:1"} }}
		var targets []raft.ServerAddress
		tr := &fakeTransport{propose: func(_ context.Context, leader raft.ServerAddress, _ []byte) ([]byte, error) {
			targets = append(targets, leader)
			if len(targets) == 1 {
				return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_NOT_LEADER, LeaderAddr: "real:2"}), nil
			}
			return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_OK}), nil
		}}
		fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})

		Expect(fl.Apply(entryBytes("x", pageSize))).To(Succeed())
		Expect(targets).To(Equal([]raft.ServerAddress{"stale:1", "real:2"}))
	})

	It("does not re-send more than once even if NOT_LEADER keeps coming back", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "a:1"} }}
		tr := &fakeTransport{propose: func(context.Context, raft.ServerAddress, []byte) ([]byte, error) {
			return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_NOT_LEADER, LeaderAddr: "b:2"}), nil
		}}
		fl := newFL(inner, tr, &fakeTarget{pageSize: pageSize})

		err := fl.Apply(entryBytes("x", pageSize))
		Expect(err).To(HaveOccurred())
		Expect(tr.calls).To(Equal(2), "at most one re-resolve")
	})

	It("resolves AMBIGUOUS via the marker CAS: consumed -> success", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "leader:1"} }}
		tr := &fakeTransport{propose: func(context.Context, raft.ServerAddress, []byte) ([]byte, error) {
			return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_AMBIGUOUS, Detail: "lost leadership"}), nil
		}}
		tg := &fakeTarget{pageSize: pageSize, await: func(context.Context, string) error { return nil }}
		fl := newFL(inner, tr, tg)

		Expect(fl.Apply(entryBytes("x", pageSize))).To(Succeed())
		Expect(tr.calls).To(Equal(1), "never re-propose after a possibly-proposed outcome")
	})

	It("resolves AMBIGUOUS via the marker CAS: abandoned -> non-retryable ambiguous error", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "leader:1"} }}
		tr := &fakeTransport{propose: func(context.Context, raft.ServerAddress, []byte) ([]byte, error) {
			return respBytes(&raftproto.ForwardResponse{Status: raftproto.ForwardResponse_AMBIGUOUS}), nil
		}}
		tg := &fakeTarget{pageSize: pageSize, await: func(context.Context, string) error { return context.DeadlineExceeded }}
		fl := newFL(inner, tr, tg)

		err := fl.Apply(entryBytes("x", pageSize))
		var amb *AmbiguousForwardError
		Expect(errors.As(err, &amb)).To(BeTrue())
		assertNotRetryable(err)
	})

	It("resolves a transport error via the marker CAS without re-proposing", func() {
		inner := &fakeInner{apply: func([]byte) error { return &NotLeaderError{Leader: "leader:1"} }}
		tr := &fakeTransport{propose: func(context.Context, raft.ServerAddress, []byte) ([]byte, error) {
			return nil, errors.New("connection reset")
		}}
		tg := &fakeTarget{pageSize: pageSize, await: func(context.Context, string) error { return context.DeadlineExceeded }}
		fl := newFL(inner, tr, tg)

		err := fl.Apply(entryBytes("x", pageSize))
		var amb *AmbiguousForwardError
		Expect(errors.As(err, &amb)).To(BeTrue())
		Expect(tr.calls).To(Equal(1))
	})
})

// --- leader (handler) path -------------------------------------------------

var _ = Describe("ForwardingLog leader handler path", func() {
	const pageSize = 4096

	call := func(fl *ForwardingLog, base uint64, id string) *raftproto.ForwardResponse {
		GinkgoHelper()
		req := &raftproto.ForwardRequest{Entry: entryBytes(id, pageSize), BaseIndex: base}
		reqBytes, err := proto.Marshal(req)
		Expect(err).NotTo(HaveOccurred())
		respRaw, err := fl.handle(context.Background(), reqBytes)
		Expect(err).NotTo(HaveOccurred())
		resp := &raftproto.ForwardResponse{}
		Expect(proto.Unmarshal(respRaw, resp)).To(Succeed())
		return resp
	}

	It("accepts a matching base: proposes, applies, responds OK, and releases the loan", func() {
		inner := &fakeInner{isLeader: true, apply: func([]byte) error { return nil }}
		tg := &fakeTarget{pageSize: pageSize, lastApplied: 5}
		fl := newFL(inner, &fakeTransport{}, tg)

		resp := call(fl, 5, "x")
		Expect(resp.GetStatus()).To(Equal(raftproto.ForwardResponse_OK))
		Expect(inner.applyCalls).To(Equal(1))
		Expect(tg.beginCalls).To(Equal(1))
		Expect(tg.releases).To(Equal(1), "the loan must be released on every path")
	})

	It("rejects a stale base with STALE_BASE and never proposes", func() {
		inner := &fakeInner{isLeader: true}
		tg := &fakeTarget{pageSize: pageSize, lastApplied: 9}
		fl := newFL(inner, &fakeTransport{}, tg)

		resp := call(fl, 5, "x")
		Expect(resp.GetStatus()).To(Equal(raftproto.ForwardResponse_STALE_BASE))
		Expect(resp.GetLastApplied()).To(Equal(uint64(9)))
		Expect(inner.applyCalls).To(Equal(0), "nothing may be proposed on a stale base")
		Expect(tg.releases).To(Equal(1))
	})

	It("answers a mis-routed request with NOT_LEADER before touching the write lock", func() {
		inner := &fakeInner{isLeader: false, leaderAddr: "real:1"}
		tg := &fakeTarget{pageSize: pageSize}
		fl := newFL(inner, &fakeTransport{}, tg)

		resp := call(fl, 0, "x")
		Expect(resp.GetStatus()).To(Equal(raftproto.ForwardResponse_NOT_LEADER))
		Expect(resp.GetLeaderAddr()).To(Equal("real:1"))
		Expect(tg.beginCalls).To(Equal(0), "must not acquire the lock on a mis-route")
	})

	It("answers BUSY when the write lock can't be acquired in time", func() {
		inner := &fakeInner{isLeader: true}
		tg := &fakeTarget{pageSize: pageSize, begin: func(context.Context, string) (func(), error) {
			return nil, context.DeadlineExceeded
		}}
		fl := newFL(inner, &fakeTransport{}, tg)

		resp := call(fl, 0, "x")
		Expect(resp.GetStatus()).To(Equal(raftproto.ForwardResponse_BUSY))
		Expect(inner.applyCalls).To(Equal(0))
	})

	It("rejects a malformed shape (nTruncate == 0) with BUSY before proposing", func() {
		inner := &fakeInner{isLeader: true}
		tg := &fakeTarget{pageSize: pageSize}
		fl := newFL(inner, &fakeTransport{}, tg)

		e := &raftproto.Entry{
			Header:  &raftproto.Header{Id: "x"},
			Payload: &raftproto.Entry_Transaction{Transaction: &raftproto.Transaction{Pages: []*raftproto.Page{{Pgno: 1, Data: make([]byte, pageSize)}}, NTruncate: 0}},
		}
		eb, err := proto.Marshal(e)
		Expect(err).NotTo(HaveOccurred())
		reqBytes, err := proto.Marshal(&raftproto.ForwardRequest{Entry: eb})
		Expect(err).NotTo(HaveOccurred())
		respRaw, err := fl.handle(context.Background(), reqBytes)
		Expect(err).NotTo(HaveOccurred())
		resp := &raftproto.ForwardResponse{}
		Expect(proto.Unmarshal(respRaw, resp)).To(Succeed())

		Expect(resp.GetStatus()).To(Equal(raftproto.ForwardResponse_BUSY))
		Expect(inner.applyCalls).To(Equal(0))
		Expect(tg.beginCalls).To(Equal(0))
	})

	It("rejects a wrong-page-size shape with BUSY", func() {
		inner := &fakeInner{isLeader: true}
		tg := &fakeTarget{pageSize: pageSize}
		fl := newFL(inner, &fakeTransport{}, tg)
		// entryBytes builds a page at pageSize; claim a different cluster size.
		tg.pageSize = pageSize + 1

		resp := call(fl, 0, "x")
		Expect(resp.GetStatus()).To(Equal(raftproto.ForwardResponse_BUSY))
		Expect(inner.applyCalls).To(Equal(0))
	})

	DescribeTable("maps an inner Apply error to the right status",
		func(applyErr error, want raftproto.ForwardResponse_Status) {
			inner := &fakeInner{isLeader: true, apply: func([]byte) error { return applyErr }}
			tg := &fakeTarget{pageSize: pageSize, lastApplied: 3}
			fl := newFL(inner, &fakeTransport{}, tg)

			resp := call(fl, 3, "x")
			Expect(resp.GetStatus()).To(Equal(want))
			Expect(tg.releases).To(Equal(1), "the loan must be released on every error path")
		},
		Entry("NotLeader -> NOT_LEADER", &NotLeaderError{Leader: "l:1"}, raftproto.ForwardResponse_NOT_LEADER),
		Entry("CatchingUp -> CATCHING_UP", vfs.GateError(CatchingUpError{}, sqlite3.ExtendedErrorCode(sqlite3.BUSY)), raftproto.ForwardResponse_CATCHING_UP),
		Entry("EnqueueTimeout -> BUSY", &EnqueueTimeoutError{Err: errors.New("enqueue")}, raftproto.ForwardResponse_BUSY),
		Entry("ambiguous -> AMBIGUOUS", &AmbiguousError{Err: errors.New("lost")}, raftproto.ForwardResponse_AMBIGUOUS),
	)
})

// assertRetryable checks err carries a sqlite3.BUSY-tagged gate error so
// internal/vfs surfaces it as retryable.
func assertRetryable(err error) {
	GinkgoHelper()
	Expect(err).To(HaveOccurred())
	code, ok := vfs.ErrCode(err)
	Expect(ok).To(BeTrue(), "retryable rejection must be a tagged gate error")
	Expect(code).To(Equal(busyCode))
}

func assertNotRetryable(err error) {
	GinkgoHelper()
	Expect(err).To(HaveOccurred())
	code, ok := vfs.ErrCode(err)
	Expect(ok && code == busyCode).To(BeFalse(), "must not be BUSY-tagged")
}

var _ = Describe("classification", func() {
	It("uses a small enough forward timeout default to bound the write-lock stall", func() {
		Expect(defaultOptions().forwardTimeout).To(BeNumerically("<=", 5*time.Second))
	})
})

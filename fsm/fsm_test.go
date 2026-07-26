package fsm

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	raftproto "github.com/fuchstim/literaft/internal/raft/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFSM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "fsm Suite")
}

func newFSM() *FSM {
	GinkgoHelper()
	f, err := New(filepath.Join(GinkgoT().TempDir(), "db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(f.Close)
	return f
}

func createLog(id string, index uint64) *raft.Log {
	GinkgoHelper()
	b, err := proto.Marshal(&raftproto.Entry{Header: &raftproto.Header{Id: id}})
	Expect(err).NotTo(HaveOccurred())
	return &raft.Log{Type: raft.LogCommand, Index: index, Data: b}
}

var _ = Describe("fsm skip-marker CAS", func() {
	It("consumes a pending marker on Apply and advances lastApplied", func() {
		f := newFSM()
		f.CreateSkipMarker("a")
		Expect(f.lastApplied.Load()).To(Equal(uint64(0)))

		f.Apply(createLog("a", 12))

		Expect(f.lastApplied.Load()).To(Equal(uint64(12)), "consumption must advance lastApplied to the entry index")
		Expect(f.AwaitSkipMarkerConsumed(context.Background(), "a")).To(Succeed(), "a consumed marker resolves immediately")
	})

	It("abandons a pending marker when the wait times out, and Apply then does not consume it", func() {
		f := newFSM()
		f.CreateSkipMarker("a")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		Expect(f.AwaitSkipMarkerConsumed(ctx, "a")).To(MatchError(context.DeadlineExceeded))

		// The marker is terminal (abandoned): Apply must not consume it, so
		// lastApplied stays put (the no-txn entry has nothing to materialize).
		f.Apply(createLog("a", 5))
		Expect(f.lastApplied.Load()).To(Equal(uint64(0)))
	})

	It("resolves a lost race as consumed: a marker consumed before the wait's ctx is observed still returns nil", func() {
		f := newFSM()
		f.CreateSkipMarker("a")

		// Consume first, then wait with an already-expired context: consumed
		// wins the CAS even though the deadline has passed.
		f.Apply(createLog("a", 3))
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		cancel()
		Expect(f.AwaitSkipMarkerConsumed(ctx, "a")).To(Succeed())
	})

	It("lands in exactly one terminal state under a concurrent consume vs abandon", func() {
		for i := 0; i < 100; i++ {
			// Built directly (not newFSM) so each iteration owns its Close;
			// newFSM's DeferCleanup would double-close it.
			f, err := New(filepath.Join(GinkgoT().TempDir(), "db"))
			Expect(err).NotTo(HaveOccurred())
			f.CreateSkipMarker("a")

			var wg sync.WaitGroup

			var awaitErr error
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			wg.Go(func() { awaitErr = f.AwaitSkipMarkerConsumed(ctx, "a") })
			wg.Go(func() { f.Apply(createLog("a", 9)) })
			wg.Wait()
			cancel()

			// Consistency: consumed (await nil) implies lastApplied advanced;
			// abandoned (await err) implies Apply did not consume it.
			if awaitErr == nil {
				Expect(f.lastApplied.Load()).To(Equal(uint64(9)), "iteration %d: consumed must have advanced lastApplied", i)
			} else {
				Expect(f.lastApplied.Load()).To(Equal(uint64(0)), "iteration %d: abandoned must not have advanced lastApplied", i)
			}
			f.Close()
		}
	})

	It("returns an error awaiting an unknown skip marker", func() {
		f := newFSM()
		Expect(f.AwaitSkipMarkerConsumed(context.Background(), "nope")).To(HaveOccurred())
	})

	It("ignores non-command log entries", func() {
		f := newFSM()
		f.Apply(&raft.Log{Type: raft.LogNoop, Index: 7})
		Expect(f.lastApplied.Load()).To(Equal(uint64(0)))
	})
})

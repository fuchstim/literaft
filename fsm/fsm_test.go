package fsm_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	"github.com/fuchstim/literaft/fsm"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFSM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "fsm Suite")
}

func newFSM() *fsm.FSM {
	GinkgoHelper()
	f, err := fsm.New(filepath.Join(GinkgoT().TempDir(), "fsm.db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(f.Close)
	return f
}

// markerLog builds a committed LogCommand carrying id with no transaction
// payload. Applying it exercises the skip-marker path without needing valid
// WAL frames: a pending marker is consumed (skipped), and a missing/abandoned
// marker falls through the txn==nil early return without materializing.
func markerLog(id string, index uint64) *raft.Log {
	GinkgoHelper()
	b, err := proto.Marshal(&raftproto.Entry{Header: &raftproto.Header{Id: id}})
	Expect(err).NotTo(HaveOccurred())
	return &raft.Log{Type: raft.LogCommand, Index: index, Data: b}
}

var _ = Describe("fsm skip-marker CAS", func() {
	It("consumes a pending marker on Apply and advances lastApplied", func() {
		f := newFSM()
		f.SkipEntry("a")
		Expect(f.LastApplied()).To(Equal(uint64(0)))

		f.Apply(markerLog("a", 12))

		Expect(f.LastApplied()).To(Equal(uint64(12)), "consumption must advance lastApplied to the entry index")
		Expect(f.AwaitEntryApplied(context.Background(), "a")).To(Succeed(), "a consumed marker resolves immediately")
	})

	It("abandons a pending marker when the wait times out, and Apply then does not consume it", func() {
		f := newFSM()
		f.SkipEntry("a")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		Expect(f.AwaitEntryApplied(ctx, "a")).To(MatchError(context.DeadlineExceeded))

		// The marker is terminal (abandoned): Apply must not consume it, so
		// lastApplied stays put (the no-txn entry has nothing to materialize).
		f.Apply(markerLog("a", 5))
		Expect(f.LastApplied()).To(Equal(uint64(0)))
	})

	It("resolves a lost race as consumed: a marker consumed before the wait's ctx is observed still returns nil", func() {
		f := newFSM()
		f.SkipEntry("a")

		// Consume first, then wait with an already-expired context: consumed
		// wins the CAS even though the deadline has passed.
		f.Apply(markerLog("a", 3))
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		time.Sleep(time.Millisecond)
		Expect(f.AwaitEntryApplied(ctx, "a")).To(Succeed())
	})

	It("lands in exactly one terminal state under a concurrent consume vs abandon", func() {
		for i := 0; i < 100; i++ {
			// Built directly (not newFSM) so each iteration owns its Close;
			// newFSM's DeferCleanup would double-close it.
			f, err := fsm.New(filepath.Join(GinkgoT().TempDir(), "fsm.db"))
			Expect(err).NotTo(HaveOccurred())
			f.SkipEntry("a")

			var wg sync.WaitGroup
			wg.Add(2)

			var awaitErr error
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			go func() { defer wg.Done(); awaitErr = f.AwaitEntryApplied(ctx, "a") }()
			go func() { defer wg.Done(); f.Apply(markerLog("a", 9)) }()
			wg.Wait()
			cancel()

			// Consistency: consumed (await nil) implies lastApplied advanced;
			// abandoned (await err) implies Apply did not consume it.
			if awaitErr == nil {
				Expect(f.LastApplied()).To(Equal(uint64(9)), "iteration %d: consumed must have advanced lastApplied", i)
			} else {
				Expect(f.LastApplied()).To(Equal(uint64(0)), "iteration %d: abandoned must not have advanced lastApplied", i)
			}
			f.Close()
		}
	})

	It("returns an error awaiting an unknown proposal id", func() {
		f := newFSM()
		Expect(f.AwaitEntryApplied(context.Background(), "nope")).To(HaveOccurred())
	})

	It("ignores non-command log entries", func() {
		f := newFSM()
		f.Apply(&raft.Log{Type: raft.LogNoop, Index: 7})
		Expect(f.LastApplied()).To(Equal(uint64(0)))
	})
})

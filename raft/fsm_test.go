package raft_test

import (
	"errors"

	hraft "github.com/hashicorp/raft"

	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// FSM.Apply used to return a decode/materialization failure as hraft's
// generic response value -- a value that, for every follower-received entry
// (and any retroactively-committed Figure-8 entry), hraft's runFSM dispatches
// with no local future to receive it, so the error was silently discarded
// forever while lastApplied had already moved past the failed entry. These
// tests exercise FSM.Apply directly (not through a live hraft cluster) so the
// panic can be safely recovered by the test itself, rather than propagating
// through hraft's own unrecovered runFSM goroutine.
var _ = Describe("FSM.Apply fatal-error behavior", func() {
	It("panics if the underlying materializer fails to apply a committed entry", func() {
		spy := &spyMaterializer{failWith: errors.New("boom")}
		fsm := raftadapter.NewFSM(spy)

		entry := vfs.Entry{Frames: []vfs.Frame{{Pgno: 1, Page: []byte("x")}}, NTruncate: 1}
		log := &hraft.Log{Type: hraft.LogCommand, Index: 7, Data: raftadapter.EncodeEntry(entry)}

		Expect(func() { fsm.Apply(log) }).To(PanicWith(ContainSubstring("index 7")))
	})

	It("panics on a committed entry that fails to decode", func() {
		fsm := raftadapter.NewFSM(&spyMaterializer{})

		log := &hraft.Log{Type: hraft.LogCommand, Index: 3, Data: []byte{0x00}}

		Expect(func() { fsm.Apply(log) }).To(PanicWith(ContainSubstring("index 3")))
	})
})

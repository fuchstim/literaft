package raftsqlite_test

import (
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/raftsqlite"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Store", func() {
	var store *raftsqlite.Store

	BeforeEach(func() {
		var err error
		store, err = raftsqlite.New(":memory:")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
	})

	Describe("log store", func() {
		It("reports zero indexes on an empty store", func() {
			first, err := store.FirstIndex()
			Expect(err).NotTo(HaveOccurred())
			Expect(first).To(BeZero())

			last, err := store.LastIndex()
			Expect(err).NotTo(HaveOccurred())
			Expect(last).To(BeZero())
		})

		It("round-trips a single log entry via StoreLog/GetLog", func() {
			want := &raft.Log{
				Index:      5,
				Term:       2,
				Type:       raft.LogCommand,
				Data:       []byte("hello"),
				Extensions: []byte("ext"),
				AppendedAt: time.Now().UTC(),
			}
			Expect(store.StoreLog(want)).To(Succeed())

			var got raft.Log
			Expect(store.GetLog(5, &got)).To(Succeed())
			Expect(got.Index).To(Equal(want.Index))
			Expect(got.Term).To(Equal(want.Term))
			Expect(got.Type).To(Equal(want.Type))
			Expect(got.Data).To(Equal(want.Data))
			Expect(got.Extensions).To(Equal(want.Extensions))
			Expect(got.AppendedAt).To(BeTemporally("==", want.AppendedAt))
		})

		It("returns raft.ErrLogNotFound for a missing index", func() {
			var got raft.Log
			Expect(store.GetLog(42, &got)).To(MatchError(raft.ErrLogNotFound))
		})

		It("stores a batch and reports First/LastIndex across it", func() {
			logs := []*raft.Log{
				{Index: 3, Term: 1, Type: raft.LogCommand, Data: []byte("a")},
				{Index: 4, Term: 1, Type: raft.LogCommand, Data: []byte("b")},
				{Index: 7, Term: 2, Type: raft.LogCommand, Data: []byte("c")},
			}
			Expect(store.StoreLogs(logs)).To(Succeed())

			first, err := store.FirstIndex()
			Expect(err).NotTo(HaveOccurred())
			Expect(first).To(Equal(uint64(3)))

			last, err := store.LastIndex()
			Expect(err).NotTo(HaveOccurred())
			Expect(last).To(Equal(uint64(7)))

			var got raft.Log
			Expect(store.GetLog(4, &got)).To(Succeed())
			Expect(got.Data).To(Equal([]byte("b")))
		})

		It("overwrites an existing index when stored again", func() {
			Expect(store.StoreLog(&raft.Log{Index: 1, Term: 1, Data: []byte("first")})).To(Succeed())
			Expect(store.StoreLog(&raft.Log{Index: 1, Term: 2, Data: []byte("second")})).To(Succeed())

			var got raft.Log
			Expect(store.GetLog(1, &got)).To(Succeed())
			Expect(got.Term).To(Equal(uint64(2)))
			Expect(got.Data).To(Equal([]byte("second")))
		})

		It("deletes an inclusive range and leaves the rest intact", func() {
			for i := uint64(1); i <= 5; i++ {
				Expect(store.StoreLog(&raft.Log{Index: i, Term: 1, Data: []byte{byte(i)}})).To(Succeed())
			}

			Expect(store.DeleteRange(2, 4)).To(Succeed())

			var got raft.Log
			Expect(store.GetLog(1, &got)).To(Succeed())
			Expect(store.GetLog(5, &got)).To(Succeed())
			for _, idx := range []uint64{2, 3, 4} {
				Expect(store.GetLog(idx, &got)).To(MatchError(raft.ErrLogNotFound))
			}

			first, err := store.FirstIndex()
			Expect(err).NotTo(HaveOccurred())
			Expect(first).To(Equal(uint64(1)))
			last, err := store.LastIndex()
			Expect(err).NotTo(HaveOccurred())
			Expect(last).To(Equal(uint64(5)))
		})
	})

	Describe("stable store", func() {
		// hraft treats a stable-store miss as "no error" by checking this
		// exact error message, not a sentinel value (see
		// vendor/github.com/hashicorp/raft/raft.go's stable.Get callers).
		It("returns an error whose message is exactly \"not found\" for a missing key", func() {
			_, err := store.Get([]byte("missing"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("not found"))
		})

		It("round-trips Set/Get", func() {
			Expect(store.Set([]byte("k"), []byte("v"))).To(Succeed())
			got, err := store.Get([]byte("k"))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("v")))
		})

		It("overwrites an existing key", func() {
			Expect(store.Set([]byte("k"), []byte("v1"))).To(Succeed())
			Expect(store.Set([]byte("k"), []byte("v2"))).To(Succeed())
			got, err := store.Get([]byte("k"))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal([]byte("v2")))
		})

		It("round-trips SetUint64/GetUint64", func() {
			Expect(store.SetUint64([]byte("term"), 42)).To(Succeed())
			got, err := store.GetUint64([]byte("term"))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(uint64(42)))
		})

		It("returns a \"not found\" error for a missing uint64 key", func() {
			_, err := store.GetUint64([]byte("missing"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("not found"))
		})
	})
})

var _ = Describe("New", func() {
	It("isolates separate :memory: stores from each other", func() {
		a, err := raftsqlite.New(":memory:")
		Expect(err).NotTo(HaveOccurred())
		defer a.Close()

		b, err := raftsqlite.New(":memory:")
		Expect(err).NotTo(HaveOccurred())
		defer b.Close()

		Expect(a.Set([]byte("k"), []byte("a-only"))).To(Succeed())
		_, err = b.Get([]byte("k"))
		Expect(err).To(MatchError(ContainSubstring("not found")))
	})

	It("persists an on-disk store across separate Store instances against the same path", func() {
		path := filepath.Join(GinkgoT().TempDir(), "raft.db")

		first, err := raftsqlite.New(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(first.SetUint64([]byte("term"), 7)).To(Succeed())
		Expect(first.StoreLog(&raft.Log{Index: 1, Term: 7, Data: []byte("row")})).To(Succeed())
		Expect(first.Close()).To(Succeed())

		second, err := raftsqlite.New(path)
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		term, err := second.GetUint64([]byte("term"))
		Expect(err).NotTo(HaveOccurred())
		Expect(term).To(Equal(uint64(7)))

		var got raft.Log
		Expect(second.GetLog(1, &got)).To(Succeed())
		Expect(got.Data).To(Equal([]byte("row")))
	})

	It("rejects an empty path", func() {
		_, err := raftsqlite.New("")
		Expect(err).To(HaveOccurred())
	})
})

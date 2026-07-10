package raftproto_test

import (
	"encoding/binary"
	"math"
	"testing"

	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEntry(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "raft/proto Suite")
}

var _ = Describe("Entry encoding", func() {
	It("round-trips a multi-frame entry", func() {
		e := &raftproto.Entry{
			NodeID: "leader-1",
			Frames: []*vfs.Frame{
				{Pgno: 1, Page: []byte("first page..............")},
				{Pgno: 7, Page: []byte("second page.............")},
			},
			NTruncate: 7,
		}

		decoded, err := raftproto.DecodeEntry(e.Encode())
		Expect(err).NotTo(HaveOccurred())
		Expect(decoded).To(Equal(e))
	})

	It("round-trips a single-frame entry", func() {
		e := &raftproto.Entry{
			NodeID:    "n0",
			Frames:    []*vfs.Frame{{Pgno: 1, Page: []byte("only page")}},
			NTruncate: 1,
		}

		decoded, err := raftproto.DecodeEntry(e.Encode())
		Expect(err).NotTo(HaveOccurred())
		Expect(decoded).To(Equal(e))
	})

	It("round-trips an entry with an empty NodeID", func() {
		e := &raftproto.Entry{
			NodeID:    "",
			Frames:    []*vfs.Frame{{Pgno: 1, Page: []byte("page")}},
			NTruncate: 1,
		}

		decoded, err := raftproto.DecodeEntry(e.Encode())
		Expect(err).NotTo(HaveOccurred())
		Expect(decoded).To(Equal(e))
	})

	It("rejects truncated input", func() {
		e := &raftproto.Entry{
			NodeID:    "leader-1",
			Frames:    []*vfs.Frame{{Pgno: 1, Page: []byte("some page data")}},
			NTruncate: 1,
		}
		full := e.Encode()

		for cut := 0; cut < len(full); cut++ {
			_, err := raftproto.DecodeEntry(full[:cut])
			Expect(err).To(HaveOccurred(), "truncating to %d bytes should fail to decode", cut)
		}
	})

	It("rejects a corrupted frame count without allocating based on it", func() {
		// A count claiming far more frames than the remaining bytes could
		// possibly encode must be rejected before it's ever used as a slice
		// capacity -- otherwise a corrupted or malformed entry can force a
		// multi-gigabyte allocation attempt instead of a clean decode error.
		b := make([]byte, 8)
		binary.BigEndian.PutUint32(b[0:4], 0)              // NodeID length 0
		binary.BigEndian.PutUint32(b[4:8], math.MaxUint32) // frame count

		_, err := raftproto.DecodeEntry(b)
		Expect(err).To(HaveOccurred())
	})

	It("rejects trailing garbage", func() {
		e := &raftproto.Entry{
			NodeID:    "leader-1",
			Frames:    []*vfs.Frame{{Pgno: 1, Page: []byte("some page data")}},
			NTruncate: 1,
		}
		full := append(e.Encode(), 0xFF)

		_, err := raftproto.DecodeEntry(full)
		Expect(err).To(HaveOccurred())
	})
})

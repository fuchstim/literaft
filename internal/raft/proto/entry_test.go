package raftproto_test

import (
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

	It("rejects malformed input", func() {
		// A lone 0xFF is an incomplete varint tag (continuation bit set,
		// no following byte) -- not valid protobuf wire format, unlike a
		// merely-empty or field-truncated (but still well-formed) message.
		_, err := raftproto.DecodeEntry([]byte{0xFF})
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

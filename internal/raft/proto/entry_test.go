package raftproto_test

import (
	"testing"

	raftproto "github.com/fuchstim/literaft/internal/raft/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"
)

func TestEntry(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "raft/proto Suite")
}

var _ = Describe("Entry encoding", func() {
	// proto.Equal, not gomega's (reflect.DeepEqual-based) Equal, is required
	// to compare generated messages: they carry unexported caching/internal
	// state that reflect.DeepEqual sees but that isn't part of the message's
	// semantic content.
	It("round-trips a multi-page transaction entry", func() {
		e := &raftproto.Entry{
			Header: &raftproto.Header{Id: "leader-1"},
			Payload: &raftproto.Entry_Transaction{
				Transaction: &raftproto.Transaction{
					Pages: []*raftproto.Page{
						{Pgno: 1, Data: []byte("first page..............")},
						{Pgno: 7, Data: []byte("second page.............")},
					},
					NTruncate: 7,
				},
			},
		}

		b, err := proto.Marshal(e)
		Expect(err).NotTo(HaveOccurred())

		decoded := &raftproto.Entry{}
		Expect(proto.Unmarshal(b, decoded)).To(Succeed())
		Expect(proto.Equal(decoded, e)).To(BeTrue())
	})

	It("round-trips a single-page transaction entry", func() {
		e := &raftproto.Entry{
			Header: &raftproto.Header{Id: "n0"},
			Payload: &raftproto.Entry_Transaction{
				Transaction: &raftproto.Transaction{
					Pages:     []*raftproto.Page{{Pgno: 1, Data: []byte("only page")}},
					NTruncate: 1,
				},
			},
		}

		b, err := proto.Marshal(e)
		Expect(err).NotTo(HaveOccurred())

		decoded := &raftproto.Entry{}
		Expect(proto.Unmarshal(b, decoded)).To(Succeed())
		Expect(proto.Equal(decoded, e)).To(BeTrue())
	})

	It("round-trips an entry with an empty header id", func() {
		e := &raftproto.Entry{
			Header: &raftproto.Header{Id: ""},
			Payload: &raftproto.Entry_Transaction{
				Transaction: &raftproto.Transaction{
					Pages:     []*raftproto.Page{{Pgno: 1, Data: []byte("page")}},
					NTruncate: 1,
				},
			},
		}

		b, err := proto.Marshal(e)
		Expect(err).NotTo(HaveOccurred())

		decoded := &raftproto.Entry{}
		Expect(proto.Unmarshal(b, decoded)).To(Succeed())
		Expect(proto.Equal(decoded, e)).To(BeTrue())
	})

	It("leaves the payload oneof unset, and GetTransaction nil, for an entry with no payload", func() {
		// FSM.Apply relies on GetTransaction returning nil for entries whose
		// payload isn't a Transaction (future non-transaction entry kinds),
		// rather than panicking or returning a zero-valued Transaction.
		e := &raftproto.Entry{Header: &raftproto.Header{Id: "leader-1"}}

		b, err := proto.Marshal(e)
		Expect(err).NotTo(HaveOccurred())

		decoded := &raftproto.Entry{}
		Expect(proto.Unmarshal(b, decoded)).To(Succeed())
		Expect(decoded.GetTransaction()).To(BeNil())
	})

	It("rejects malformed input", func() {
		// A lone 0xFF is an incomplete varint tag (continuation bit set,
		// no following byte) -- not valid protobuf wire format, unlike a
		// merely-empty or field-truncated (but still well-formed) message.
		err := proto.Unmarshal([]byte{0xFF}, &raftproto.Entry{})
		Expect(err).To(HaveOccurred())
	})

	It("rejects trailing garbage after a valid encoding", func() {
		e := &raftproto.Entry{
			Header: &raftproto.Header{Id: "leader-1"},
			Payload: &raftproto.Entry_Transaction{
				Transaction: &raftproto.Transaction{
					Pages:     []*raftproto.Page{{Pgno: 1, Data: []byte("some page data")}},
					NTruncate: 1,
				},
			},
		}
		b, err := proto.Marshal(e)
		Expect(err).NotTo(HaveOccurred())
		full := append(b, 0xFF)

		err = proto.Unmarshal(full, &raftproto.Entry{})
		Expect(err).To(HaveOccurred())
	})
})

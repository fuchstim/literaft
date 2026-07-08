package raft_test

import (
	"encoding/binary"
	"math"

	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("entry encoding", func() {
	It("round-trips a multi-frame entry", func() {
		e := vfs.Entry{
			Frames: []vfs.Frame{
				{Pgno: 1, Page: []byte("first page..............")},
				{Pgno: 7, Page: []byte("second page.............")},
			},
			NTruncate: 7,
		}

		decoded, err := raftadapter.DecodeEntry(raftadapter.EncodeEntry(e))
		Expect(err).NotTo(HaveOccurred())
		Expect(decoded).To(Equal(e))
	})

	It("round-trips a single-frame entry", func() {
		e := vfs.Entry{
			Frames:    []vfs.Frame{{Pgno: 1, Page: []byte("only page")}},
			NTruncate: 1,
		}

		decoded, err := raftadapter.DecodeEntry(raftadapter.EncodeEntry(e))
		Expect(err).NotTo(HaveOccurred())
		Expect(decoded).To(Equal(e))
	})

	It("rejects truncated input", func() {
		e := vfs.Entry{
			Frames:    []vfs.Frame{{Pgno: 1, Page: []byte("some page data")}},
			NTruncate: 1,
		}
		full := raftadapter.EncodeEntry(e)

		for cut := 0; cut < len(full); cut++ {
			_, err := raftadapter.DecodeEntry(full[:cut])
			Expect(err).To(HaveOccurred(), "truncating to %d bytes should fail to decode", cut)
		}
	})

	It("rejects a corrupted frame count without allocating based on it", func() {
		// A count claiming far more frames than the remaining bytes could
		// possibly encode must be rejected before it's ever used as a slice
		// capacity -- otherwise a corrupted or malformed entry can force a
		// multi-gigabyte allocation attempt instead of a clean decode error.
		b := make([]byte, 8)
		binary.BigEndian.PutUint32(b[0:4], math.MaxUint32)
		binary.BigEndian.PutUint32(b[4:8], 0)

		_, err := raftadapter.DecodeEntry(b)
		Expect(err).To(HaveOccurred())
	})

	It("rejects trailing garbage", func() {
		e := vfs.Entry{
			Frames:    []vfs.Frame{{Pgno: 1, Page: []byte("some page data")}},
			NTruncate: 1,
		}
		full := append(raftadapter.EncodeEntry(e), 0xFF)

		_, err := raftadapter.DecodeEntry(full)
		Expect(err).To(HaveOccurred())
	})
})

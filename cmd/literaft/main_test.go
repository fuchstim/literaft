package main

import (
	"testing"

	"github.com/hashicorp/raft"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCmdLiteraft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cmd/literaft Suite")
}

var _ = Describe("parsePeers", func() {
	It("parses a comma-separated id=addr list", func() {
		servers, err := parsePeers("a=127.0.0.1:9001,b=127.0.0.1:9002")
		Expect(err).NotTo(HaveOccurred())
		Expect(servers).To(Equal([]raft.Server{
			{Suffrage: raft.Voter, ID: "a", Address: "127.0.0.1:9001"},
			{Suffrage: raft.Voter, ID: "b", Address: "127.0.0.1:9002"},
		}))
	})

	It("trims surrounding whitespace around each peer", func() {
		servers, err := parsePeers(" a=127.0.0.1:9001 , b=127.0.0.1:9002 ")
		Expect(err).NotTo(HaveOccurred())
		Expect(servers).To(Equal([]raft.Server{
			{Suffrage: raft.Voter, ID: "a", Address: "127.0.0.1:9001"},
			{Suffrage: raft.Voter, ID: "b", Address: "127.0.0.1:9002"},
		}))
	})

	It("rejects an empty peer list", func() {
		_, err := parsePeers("")
		Expect(err).To(HaveOccurred())
	})

	It("rejects a peer missing '='", func() {
		_, err := parsePeers("a127.0.0.1:9001")
		Expect(err).To(HaveOccurred())
	})

	It("rejects a peer with an empty id", func() {
		_, err := parsePeers("=127.0.0.1:9001")
		Expect(err).To(HaveOccurred())
	})

	It("rejects a peer with an empty address", func() {
		_, err := parsePeers("a=")
		Expect(err).To(HaveOccurred())
	})
})

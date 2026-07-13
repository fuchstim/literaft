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

var _ = Describe("reannounceTargets", func() {
	servers := []raft.Server{
		{Suffrage: raft.Voter, ID: "n1", Address: "10.0.0.1:9000"},
		{Suffrage: raft.Voter, ID: "n2", Address: "10.0.0.2:9000"},
		{Suffrage: raft.Voter, ID: "n3", Address: "10.0.0.3:9000"},
	}

	It("does nothing when our address is unchanged", func() {
		changed, targets := reannounceTargets(servers, "n2", "10.0.0.2:9000", "")
		Expect(changed).To(BeFalse())
		Expect(targets).To(BeNil())
	})

	It("does nothing when we aren't in the configuration", func() {
		changed, targets := reannounceTargets(servers, "n9", "10.0.0.9:9000", "")
		Expect(changed).To(BeFalse())
		Expect(targets).To(BeNil())
	})

	It("targets the other members when our address changed", func() {
		changed, targets := reannounceTargets(servers, "n2", "10.9.9.9:9000", "")
		Expect(changed).To(BeTrue())
		Expect(targets).To(Equal([]string{"10.0.0.1:9000", "10.0.0.3:9000"}))
	})

	It("puts the -join hint first when given", func() {
		changed, targets := reannounceTargets(servers, "n2", "10.9.9.9:9000", "seed:9000")
		Expect(changed).To(BeTrue())
		Expect(targets).To(Equal([]string{"seed:9000", "10.0.0.1:9000", "10.0.0.3:9000"}))
	})

	It("reports the change but has no targets for a lone moved node", func() {
		lone := []raft.Server{{Suffrage: raft.Voter, ID: "n1", Address: "10.0.0.1:9000"}}
		changed, targets := reannounceTargets(lone, "n1", "10.9.9.9:9000", "")
		Expect(changed).To(BeTrue())
		Expect(targets).To(BeEmpty())
	})
})

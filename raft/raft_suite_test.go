package raft_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRaft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "raft Suite")
}

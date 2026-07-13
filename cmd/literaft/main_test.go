package main

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCmdLiteraft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cmd/literaft Suite")
}

package main

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLiteraft(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cmd/literaft Suite")
}

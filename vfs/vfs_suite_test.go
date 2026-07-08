package vfs_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestVFS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "vfs Suite")
}

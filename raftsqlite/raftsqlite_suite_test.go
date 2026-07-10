package raftsqlite_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRaftSqlite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "raftsqlite Suite")
}

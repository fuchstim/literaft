// Package integration_test holds whole-system tests that are too slow to
// live alongside the fast unit suites they exercise (internal/testutils,
// fsm, vfs, ...): a throughput benchmark and a replication-fidelity
// correctness test, both driven against a real testutils.TCPCluster.
package integration_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "integration Suite")
}

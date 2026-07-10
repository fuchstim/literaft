package driver_test

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ncruces/go-sqlite3"
	ncrdriver "github.com/ncruces/go-sqlite3/driver"

	"github.com/fuchstim/literaft/driver"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDriver(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "driver Suite")
}

var driverAliasSeq atomic.Uint64

// uniqueAlias returns a sql.Register alias unique within this test binary:
// sql.Register panics on a duplicate name, and every spec below registers
// its own Driver instance.
func uniqueAlias() string {
	return fmt.Sprintf("literaft-driver-test-%d", driverAliasSeq.Add(1))
}

// openDB registers drv under a fresh alias and opens a *sql.DB against it.
func openDB(drv *driver.Driver) *sql.DB {
	GinkgoHelper()
	alias := uniqueAlias()
	sql.Register(alias, drv)
	db, err := sql.Open(alias, "")
	Expect(err).NotTo(HaveOccurred())
	return db
}

func queryText(c *sqlite3.Conn, query string) string {
	GinkgoHelper()
	stmt, _, err := c.Prepare(query)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", query)
	return stmt.ColumnText(0)
}

func queryInt(c *sqlite3.Conn, query string) int64 {
	GinkgoHelper()
	stmt, _, err := c.Prepare(query)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", query)
	return stmt.ColumnInt64(0)
}

// pragmaSynchronous reads back PRAGMA synchronous on c's underlying
// *sqlite3.Conn via database/sql's Raw escape hatch.
func pragmaSynchronous(c *sql.Conn) int64 {
	GinkgoHelper()
	var value int64
	Expect(c.Raw(func(driverConn any) error {
		raw, ok := driverConn.(ncrdriver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver conn type %T", driverConn)
		}
		stmt, _, err := raw.Raw().Prepare("PRAGMA synchronous")
		if err != nil {
			return err
		}
		defer stmt.Close()
		if !stmt.Step() {
			return fmt.Errorf("no rows")
		}
		value = stmt.ColumnInt64(0)
		return nil
	})).To(Succeed())
	return value
}

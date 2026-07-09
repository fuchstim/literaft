package driver_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ncruces/go-sqlite3"
	ncrdriver "github.com/ncruces/go-sqlite3/driver"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/driver"
	raftadapter "github.com/fuchstim/literaft/raft"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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

var _ = Describe("Driver", func() {
	It("commits a write visible to a plain-VFS external reader", func() {
		dir := GinkgoT().TempDir()
		nodes := newRaftCluster(dir, 1, 4096)
		defer shutdownRaftCluster(nodes)
		n := nodes[0]

		drv, err := driver.New(n.raft, n.fsm, n.dbPath,
			driver.WithPageSize(4096), driver.WithCheckpointInterval(50*time.Millisecond))
		Expect(err).NotTo(HaveOccurred())
		defer drv.Close()

		db := openDB(drv)
		defer db.Close()

		Eventually(drv.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		ctx := context.Background()
		_, err = db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		Expect(err).NotTo(HaveOccurred())
		_, err = db.ExecContext(ctx, "INSERT INTO t (v) VALUES ('hello'), ('world')")
		Expect(err).NotTo(HaveOccurred())

		external, err := sqlite3.Open(n.dbPath)
		Expect(err).NotTo(HaveOccurred())
		defer external.Close()
		Expect(queryText(external, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(external, "SELECT count(*) FROM t")).To(Equal(int64(2)))
	})

	It("surfaces a follower rejection via LastRejection", func() {
		dir := GinkgoT().TempDir()
		nodes := newRaftCluster(dir, 3, 4096)
		defer shutdownRaftCluster(nodes)

		drivers := make(map[*raftNode]*driver.Driver, len(nodes))
		for _, n := range nodes {
			d, err := driver.New(n.raft, n.fsm, n.dbPath,
				driver.WithPageSize(4096), driver.WithCheckpointInterval(50*time.Millisecond))
			Expect(err).NotTo(HaveOccurred())
			defer d.Close()
			drivers[n] = d
		}

		leader := waitForRaftLeader(nodes)
		follower := otherRaftNode(nodes, leader)
		followerDriver := drivers[follower]

		db := openDB(followerDriver)
		defer db.Close()

		_, err := db.ExecContext(context.Background(), "CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(err).To(HaveOccurred())

		var notLeader *raftadapter.NotLeaderError
		rejection := followerDriver.LastRejection()
		Expect(errors.As(rejection, &notLeader)).To(BeTrue(),
			"got %v (%T), not a NotLeaderError", rejection, rejection)
	})

	It("guarantees unique VFS names across Driver instances, even with the same WithName hint", func() {
		dir1 := GinkgoT().TempDir()
		dir2 := GinkgoT().TempDir()
		nodes1 := newRaftCluster(dir1, 1, 4096)
		defer shutdownRaftCluster(nodes1)
		nodes2 := newRaftCluster(dir2, 1, 4096)
		defer shutdownRaftCluster(nodes2)

		drv1, err := driver.New(nodes1[0].raft, nodes1[0].fsm, nodes1[0].dbPath,
			driver.WithPageSize(4096), driver.WithName("shared-hint"))
		Expect(err).NotTo(HaveOccurred())
		defer drv1.Close()
		drv2, err := driver.New(nodes2[0].raft, nodes2[0].fsm, nodes2[0].dbPath,
			driver.WithPageSize(4096), driver.WithName("shared-hint"))
		Expect(err).NotTo(HaveOccurred())
		defer drv2.Close()

		Expect(drv1.VFSName()).NotTo(Equal(drv2.VFSName()))

		db1 := openDB(drv1)
		defer db1.Close()
		db2 := openDB(drv2)
		defer db2.Close()

		Eventually(drv1.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())
		Eventually(drv2.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		ctx := context.Background()
		_, err = db1.ExecContext(ctx, "CREATE TABLE only1 (id INTEGER PRIMARY KEY)")
		Expect(err).NotTo(HaveOccurred())

		_, err = db2.ExecContext(ctx, "SELECT * FROM only1")
		Expect(err).To(HaveOccurred())
	})

	It("Close is idempotent, cleans up the VFS registration, and isn't triggered by sql.DB.Close", func() {
		dir := GinkgoT().TempDir()
		nodes := newRaftCluster(dir, 1, 4096)
		defer shutdownRaftCluster(nodes)
		n := nodes[0]

		drv, err := driver.New(n.raft, n.fsm, n.dbPath,
			driver.WithPageSize(4096), driver.WithCheckpointInterval(50*time.Millisecond))
		Expect(err).NotTo(HaveOccurred())

		db := openDB(drv)

		Eventually(drv.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		vfsName := drv.VFSName()
		Expect(sqlite3vfs.Find(vfsName)).NotTo(BeNil())

		// db.Close() alone must not tear down the Driver.
		Expect(db.Close()).To(Succeed())
		Expect(sqlite3vfs.Find(vfsName)).NotTo(BeNil())
		Expect(drv.Ready()).To(BeTrue())

		Expect(drv.Close()).To(Succeed())
		Expect(sqlite3vfs.Find(vfsName)).To(BeNil())

		// Idempotent.
		Expect(drv.Close()).To(Succeed())
	})

	It("applies PRAGMA synchronous=NORMAL to every pooled connection, not just the first", func() {
		dir := GinkgoT().TempDir()
		nodes := newRaftCluster(dir, 1, 4096)
		defer shutdownRaftCluster(nodes)
		n := nodes[0]

		drv, err := driver.New(n.raft, n.fsm, n.dbPath,
			driver.WithPageSize(4096), driver.WithCheckpointInterval(50*time.Millisecond))
		Expect(err).NotTo(HaveOccurred())
		defer drv.Close()

		db := openDB(drv)
		defer db.Close()
		db.SetMaxOpenConns(2)

		Eventually(drv.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		ctx := context.Background()
		c1, err := db.Conn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer c1.Close()
		c2, err := db.Conn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer c2.Close()

		Expect(pragmaSynchronous(c1)).To(Equal(int64(1)))
		Expect(pragmaSynchronous(c2)).To(Equal(int64(1)))
	})
})

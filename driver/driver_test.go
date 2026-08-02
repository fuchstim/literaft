package driver_test

import (
	"context"
	"errors"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/internal/testutils"
	rafterrors "github.com/fuchstim/literaft/proto/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Driver", func() {
	It("commits a write visible to a plain-VFS external reader", func() {
		c := testutils.NewInmemCluster(GinkgoT(), 1)
		defer c.Shutdown()
		n := c.Nodes()[0]

		drv := driver.New(n.Raft, n.FSM, driver.WithApplyTimeout(2*time.Second))
		defer drv.Close()

		db := openDB(drv)
		defer db.Close()

		Eventually(drv.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		ctx := context.Background()
		_, err := db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
		Expect(err).NotTo(HaveOccurred())
		_, err = db.ExecContext(ctx, "INSERT INTO t (v) VALUES ('hello'), ('world')")
		Expect(err).NotTo(HaveOccurred())

		external, err := sqlite3.Open(n.DBPath)
		Expect(err).NotTo(HaveOccurred())
		defer external.Close()
		Expect(queryText(external, "PRAGMA integrity_check")).To(Equal("ok"))
		Expect(queryInt(external, "SELECT count(*) FROM t")).To(Equal(int64(2)))
	})

	It("surfaces a follower rejection via LastRejection", func() {
		c := testutils.NewInmemCluster(GinkgoT(), 2)
		defer c.Shutdown()

		drivers := make(map[*testutils.Node]*driver.Driver, len(c.Nodes()))
		for _, n := range c.Nodes() {
			d := driver.New(n.Raft, n.FSM, driver.WithApplyTimeout(2*time.Second))
			defer d.Close()
			drivers[n] = d
		}

		leader := c.Leader()
		follower := c.Other(leader)
		followerDriver := drivers[follower]

		db := openDB(followerDriver)
		defer db.Close()

		_, err := db.ExecContext(context.Background(), "CREATE TABLE t (id INTEGER PRIMARY KEY)")
		Expect(err).To(HaveOccurred())

		var notLeader *rafterrors.NotLeaderError
		rejection := followerDriver.LastRejection()
		Expect(errors.As(rejection, &notLeader)).To(BeTrue(),
			"got %v (%T), not a NotLeaderError", rejection, rejection)
	})

	It("guarantees unique VFS names across Driver instances", func() {
		c1 := testutils.NewInmemCluster(GinkgoT(), 1)
		defer c1.Shutdown()
		c2 := testutils.NewInmemCluster(GinkgoT(), 1)
		defer c2.Shutdown()

		drv1 := driver.New(c1.Nodes()[0].Raft, c1.Nodes()[0].FSM)
		defer drv1.Close()
		drv2 := driver.New(c2.Nodes()[0].Raft, c2.Nodes()[0].FSM)
		defer drv2.Close()

		Expect(drv1.VFSName()).NotTo(Equal(drv2.VFSName()))

		db1 := openDB(drv1)
		defer db1.Close()
		db2 := openDB(drv2)
		defer db2.Close()

		Eventually(drv1.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())
		Eventually(drv2.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		ctx := context.Background()
		_, err := db1.ExecContext(ctx, "CREATE TABLE only1 (id INTEGER PRIMARY KEY)")
		Expect(err).NotTo(HaveOccurred())

		_, err = db2.ExecContext(ctx, "SELECT * FROM only1")
		Expect(err).To(HaveOccurred())
	})

	It("Close is idempotent, cleans up the VFS registration, and isn't triggered by sql.DB.Close", func() {
		c := testutils.NewInmemCluster(GinkgoT(), 1)
		defer c.Shutdown()
		n := c.Nodes()[0]

		drv := driver.New(n.Raft, n.FSM, driver.WithApplyTimeout(2*time.Second))

		db := openDB(drv)

		Eventually(drv.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		vfsName := drv.VFSName()
		Expect(sqlite3vfs.Find(vfsName)).NotTo(BeNil())

		// db.Close() alone must not tear down the Driver.
		Expect(db.Close()).To(Succeed())
		Expect(sqlite3vfs.Find(vfsName)).NotTo(BeNil())
		Expect(drv.Ready()).To(BeTrue())

		drv.Close()
		Expect(sqlite3vfs.Find(vfsName)).To(BeNil())

		// Idempotent: must not panic.
		Expect(drv.Close).NotTo(Panic())
	})

	It("applies PRAGMA synchronous=NORMAL to every pooled connection, not just the first", func() {
		c := testutils.NewInmemCluster(GinkgoT(), 1)
		defer c.Shutdown()
		n := c.Nodes()[0]

		drv := driver.New(n.Raft, n.FSM, driver.WithApplyTimeout(2*time.Second))
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

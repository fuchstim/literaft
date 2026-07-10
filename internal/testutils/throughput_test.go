package testutils_test

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncruces/go-sqlite3"
	ncrdriver "github.com/ncruces/go-sqlite3/driver"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
)

// This suite measures read and write throughput, not correctness. Leader
// write throughput is capped at one txn per RAFT round-trip, so it should
// fall off sharply from the bare baseline and further as node count grows;
// read throughput should stay roughly flat across scenarios, since only
// writes are gated and reads never touch RAFT. Numbers land in a gmeasure
// report entry per scenario rather than a pass/fail threshold, since
// absolute throughput is machine-dependent.

const (
	throughputWriteDuration = 20 * time.Second
	throughputReadDuration  = 20 * time.Second
	throughputReadWorkers   = 8
)

// openBareDB opens a plain ncruces/go-sqlite3 connection through its own
// database/sql driver -- no literaft VFS, gate, or RAFT involved -- as the
// baseline every TCP cluster size is measured against. Same pragmas (WAL +
// synchronous=NORMAL) as a real literaft node, so the baseline isolates
// RAFT/gate overhead rather than differing on SQLite settings.
func openBareDB(t testutils.TB) *sql.DB {
	t.Helper()
	dbPath := t.TempDir() + "/bare.db"
	db, err := ncrdriver.Open("file:"+dbPath, func(c *sqlite3.Conn) error {
		if err := c.Exec("PRAGMA journal_mode=WAL"); err != nil {
			return err
		}
		return c.Exec("PRAGMA synchronous=NORMAL")
	})
	if err != nil {
		t.Fatalf("testutils: openBareDB: %v", err)
	}
	return db
}

// measureThroughput spends throughputWriteDuration doing sequential
// single-row inserts (each its own implicit transaction), counting how many
// complete, then spends throughputReadDuration doing point-selects across
// throughputReadWorkers concurrent goroutines, counting how many complete.
// Both are recorded as a txn/s value on experiment.
func measureThroughput(experiment *gmeasure.Experiment, db *sql.DB) {
	GinkgoHelper()

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	Expect(err).NotTo(HaveOccurred())

	var writeCount int
	start := time.Now()
	deadline := start.Add(throughputWriteDuration)
	for time.Now().Before(deadline) {
		_, err := db.Exec("INSERT INTO t (v) VALUES (?)", fmt.Sprintf("row%d", writeCount))
		Expect(err).NotTo(HaveOccurred())
		writeCount++
	}
	writeElapsed := time.Since(start)
	experiment.RecordValue(
		"write throughput", float64(writeCount)/writeElapsed.Seconds(),
		gmeasure.Units("txn/s"),
	)

	var rowCount int64
	Expect(db.QueryRow("SELECT count(*) FROM t").Scan(&rowCount)).To(Succeed())
	Expect(rowCount).To(Equal(int64(writeCount)))

	var readCount atomic.Int64
	var wg sync.WaitGroup
	start = time.Now()
	deadline = start.Add(throughputReadDuration)
	for w := 0; w < throughputReadWorkers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			defer GinkgoRecover()
			for i := 0; time.Now().Before(deadline); i++ {
				id := (worker+i)%writeCount + 1
				var v string
				Expect(db.QueryRow("SELECT v FROM t WHERE id = ?", id).Scan(&v)).To(Succeed())
				readCount.Add(1)
			}
		}(w)
	}
	wg.Wait()
	readElapsed := time.Since(start)
	experiment.RecordValue(
		"read throughput", float64(readCount.Load())/readElapsed.Seconds(),
		gmeasure.Units("txn/s"),
	)
}

var _ = Describe("throughput", func() {
	It("measures a bare ncruces/go-sqlite3 connection", func() {
		experiment := gmeasure.NewExperiment("bare sqlite")
		AddReportEntry(experiment.Name, experiment)

		db := openBareDB(GinkgoT())
		defer db.Close()

		measureThroughput(experiment, db)
	})

	for _, n := range []int{1, 2, 3, 5} {
		It(fmt.Sprintf("measures a %d-node TCP cluster", n), func() {
			experiment := gmeasure.NewExperiment(fmt.Sprintf("%d-node cluster", n))
			AddReportEntry(experiment.Name, experiment)

			// On-disk raft store: this benchmark sustains heavy write
			// volume, and the default in-memory raftsqlite.Store has
			// nothing to spill to -- a long enough burst exhausts the WASM
			// SQLite engine's own memory (see WithOnDiskRaftStore's doc).
			c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), n, testutils.WithOnDiskRaftStore(), testutils.WithSnapshotInterval(time.Second))
			defer c.Shutdown()
			leader := c.ReadyLeader()

			measureThroughput(experiment, leader.DB)
		})
	}
})

package integration_test

import (
	"database/sql"
	_ "embed"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/ncruces/go-sqlite3"
	ncrdriver "github.com/ncruces/go-sqlite3/driver"

	rafterrors "github.com/fuchstim/literaft/internal/raft/gate/errors"
	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This suite proves RAFT's physical-redo replication produces results
// identical to plain, unreplicated SQLite: the same sequence of writes goes
// to both a bare ncruces/go-sqlite3 connection and a testutils.TCPCluster
// leader, against a schema complex enough for triggers to cascade off one
// another (an append-only, version-stamped table plus a fan-out outbox),
// then every node's data and PRAGMA integrity_check are compared.
//
// Every random choice and "timestamp" the workload makes is decided once in
// Go -- with a math/rand.Rand seeded from Ginkgo's own run seed, so a
// failure is reproducible via `ginkgo --seed` -- and applied as bound
// parameters to both connections identically. Never left to SQL-side
// RANDOM() or datetime('now'), which run independently per connection and
// would let the two databases pick different rows/timestamps for reasons
// unrelated to replication correctness.

//go:embed schema.sql
var correctnessSchema string

func correctnessDir(t testutils.TB) string {
	t.Helper()
	if dir := os.Getenv("LITERAFT_CORRECTNESS_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("integration: correctnessDir: %v", err)
		}
		return dir
	}
	return t.TempDir()
}

func correctnessDuration() time.Duration {
	if s := os.Getenv("LITERAFT_CORRECTNESS_DURATION"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
	}
	return 30 * time.Second
}

func correctnessIterations() int {
	if s := os.Getenv("LITERAFT_CORRECTNESS_ITERATIONS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

// openPlainDB mirrors openBareDB in throughput_test.go: a plain
// ncruces/go-sqlite3 connection -- no literaft VFS, gate, or RAFT involved
// -- with the same pragmas a real literaft node applies.
func openPlainDB(t testutils.TB, dir string) *sql.DB {
	t.Helper()
	db, err := ncrdriver.Open("file:"+dir+"/plain.db", func(c *sqlite3.Conn) error {
		if err := c.Exec("PRAGMA journal_mode=WAL"); err != nil {
			return err
		}
		return c.Exec("PRAGMA synchronous=NORMAL")
	})
	if err != nil {
		t.Fatalf("integration: openPlainDB: %v", err)
	}
	return db
}

// applySchema runs the embedded schema once against each db. sqlite3_exec
// (which a driver.ExecContext with no args routes to) runs every statement
// in the string, so the whole file lands in a single Exec call.
func applySchema(dbs ...*sql.DB) {
	GinkgoHelper()
	for _, db := range dbs {
		_, err := db.Exec(correctnessSchema)
		Expect(err).NotTo(HaveOccurred())
	}
}

// op is one generated write: a statement plus the exact bound parameters to
// apply, identically, to every database under test.
type op struct {
	sql  string
	args []any
	// conditional is true when the statement's effect depends on a
	// WHERE-clause read of current state (an UPDATE or DELETE), so a zero-row
	// result may be a stale-read no-op rather than a genuine one -- as opposed
	// to an INSERT, which always writes a row.
	conditional bool
}

// generator produces a deterministic sequence of ops against the
// records/pending_changes schema. It tracks known keys and a monotonic
// logical clock (standing in for wall-clock time) purely in Go, so "which
// row" and "what time" are never delegated to SQL.
type generator struct {
	rng   *rand.Rand
	clock int64
	keys  []string
	next  int
}

func newGenerator(seed int64) *generator {
	return &generator{rng: rand.New(rand.NewSource(seed))}
}

func (g *generator) randLabel() string { return fmt.Sprintf("label-%d", g.rng.Intn(100)) }

func (g *generator) randPayload() []byte { return fmt.Appendf(nil, `{"n":%d}`, g.rng.Intn(1000)) }

// step returns the next op to apply and advances the generator's own
// bookkeeping as though it had already been applied -- callers must
// actually apply it (to both databases) to keep that bookkeeping honest.
func (g *generator) step() op {
	g.clock++

	choice := g.rng.Intn(4)
	if len(g.keys) == 0 {
		choice = 0 // nothing to bump/delete/drain until something exists
	}

	switch choice {
	case 0: // insert a brand new key at rev 1
		key := fmt.Sprintf("key-%d", g.next)
		g.next++
		g.keys = append(g.keys, key)
		return op{
			sql:  `INSERT INTO records (key, rev, written_at, label, payload, enabled) VALUES (?, 1, ?, ?, ?, ?)`,
			args: []any{key, g.clock, g.randLabel(), g.randPayload(), g.rng.Intn(2)},
		}
	case 1: // bump an existing key to the next revision; MAX(rev) is a
		// deterministic aggregate over logical row content, so this stays
		// identical across databases regardless of physical row order.
		key := g.keys[g.rng.Intn(len(g.keys))]
		return op{
			sql: `INSERT INTO records (key, rev, written_at, label, payload, enabled)
				SELECT ?, COALESCE(MAX(rev), 0) + 1, ?, ?, ?, ? FROM records WHERE key = ?`,
			args: []any{key, g.clock, g.randLabel(), g.randPayload(), g.rng.Intn(2), key},
		}
	case 2: // soft-delete an existing key
		key := g.keys[g.rng.Intn(len(g.keys))]
		return op{
			sql:         `UPDATE records SET removed_at = ? WHERE key = ? AND removed_at IS NULL`,
			args:        []any{g.clock, key},
			conditional: true,
		}
	default: // drain due outbox entries, deleting their target rows
		return op{
			sql: `DELETE FROM records WHERE id IN (
				SELECT record_id FROM pending_changes
				WHERE record_table = 'records' AND change_kind = 'REMOVE' AND not_before <= ?
			)`,
			args:        []any{g.clock},
			conditional: true,
		}
	}
}

// runWorkload applies generated ops to plainDB and leaderDB, in the same
// order with the same parameters, for roughly duration.
func runWorkload(gen *generator, plainDB, leaderDB *sql.DB, duration time.Duration) {
	GinkgoHelper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		o := gen.step()
		_, err := plainDB.Exec(o.sql, o.args...)
		Expect(err).NotTo(HaveOccurred(), "plain db: %s", o.sql)
		_, err = leaderDB.Exec(o.sql, o.args...)
		Expect(err).NotTo(HaveOccurred(), "leader db: %s", o.sql)
	}
}

func tableCountsMatch(plainDB, nodeDB *sql.DB, tables ...string) bool {
	for _, table := range tables {
		var want, got int64
		if err := plainDB.QueryRow("SELECT count(*) FROM " + table).Scan(&want); err != nil {
			return false
		}
		if err := nodeDB.QueryRow("SELECT count(*) FROM " + table).Scan(&got); err != nil {
			return false
		}
		if want != got {
			return false
		}
	}
	return true
}

func assertIntegrityOK(db *sql.DB) {
	GinkgoHelper()
	var result string
	Expect(db.QueryRow("PRAGMA integrity_check").Scan(&result)).To(Succeed())
	Expect(result).To(Equal("ok"))
}

// assertExternalIntegrityOK checks dbPath through a completely plain,
// unmodified-VFS connection (no "?vfs=" at all) -- mirrors the
// externalQueryText idiom in internal/testutils/cluster_test.go, so a
// corruption that's somehow only visible through literaft's own gate/VFS
// doesn't go unnoticed.
func assertExternalIntegrityOK(dbPath string) {
	GinkgoHelper()
	c, err := sqlite3.Open(dbPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	stmt, _, err := c.Prepare("PRAGMA integrity_check")
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue())
	Expect(stmt.ColumnText(0)).To(Equal("ok"))
}

// dumpTables reads every row of each table, ordered by id, into a
// comparable value -- so two databases can be asserted equal with gomega's
// own Equal (which reports a readable diff on mismatch) instead of a
// bespoke comparison.
func dumpTables(db *sql.DB, tables ...string) map[string][]map[string]any {
	GinkgoHelper()
	dump := make(map[string][]map[string]any, len(tables))
	for _, table := range tables {
		dump[table] = dumpTable(db, table)
	}
	return dump
}

func dumpTable(db *sql.DB, table string) []map[string]any {
	GinkgoHelper()
	rows, err := db.Query("SELECT * FROM " + table + " ORDER BY id")
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()

	cols, err := rows.Columns()
	Expect(err).NotTo(HaveOccurred())

	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		Expect(rows.Scan(ptrs...)).To(Succeed())

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		out = append(out, row)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

// execForwarded applies o through a follower connection, retrying *only*
// while the leader keeps rejecting it as retryable (all pre-propose, so
// re-running never double-applies), read from the node's own gate. A
// non-retryable outcome (a possibly-committed forward, or a hard error) fails
// the spec loudly rather than being blindly re-run, which could double-apply
// and diverge from the plain db. It returns the number of rows the accepted
// statement changed.
func execForwarded(t testutils.TB, n *testutils.Node, o op) int64 {
	GinkgoHelper()
	var rows int64
	testutils.Eventually(t, 20*time.Second, 10*time.Millisecond, func() bool {
		res, err := n.DB.Exec(o.sql, o.args...)
		if err == nil {
			rows, _ = res.RowsAffected()
			return true
		}
		rej := n.Driver.LastRejection()
		Expect(rafterrors.SafeToRetry(rej)).To(BeTrue(),
			"non-retryable follower-write outcome (not safe to re-run): %v", rej)
		return false
	}, fmt.Sprintf("follower write to be accepted: %s", o.sql))
	return rows
}

// waitFollowerCurrent blocks until n has materialized every committed entry
// the cluster's most-advanced node has -- i.e. n's local database reflects all
// prior committed writes.
func waitFollowerCurrent(t testutils.TB, c *testutils.TCPCluster, n *testutils.Node) {
	testutils.Eventually(t, 10*time.Second, 5*time.Millisecond, func() bool {
		var maxApplied uint64
		for _, m := range c.Nodes() {
			if a := m.FSM.LastApplied(); a > maxApplied {
				maxApplied = a
			}
		}
		return n.FSM.LastApplied() >= maxApplied
	}, fmt.Sprintf("follower %s to catch up before re-running a no-op conditional write", n.ID))
}

var _ = Describe("correctness", func() {
	It("keeps a plain SQLite db and a 3-node cluster identical under mixed trigger-driven writes issued through follower connections", func() {
		t := GinkgoT()
		dir := correctnessDir(t)

		plainDB := openPlainDB(t, dir)
		defer plainDB.Close()

		// A generous apply timeout (which also bounds the forward round-trip
		// budget in testutils) keeps forwards from timing out into ambiguous
		// outcomes under load, so the base check is the only thing that ever
		// stales a write here.
		c := testutils.NewTCPCluster(t, dir, 3,
			testutils.WithOnDiskRaftStore(), testutils.WithForwarding(),
			testutils.WithApplyTimeout(10*time.Second), testutils.WithSnapshotInterval(time.Second))
		defer c.Shutdown()
		leader := c.ReadyLeader()

		applySchema(plainDB, leader.DB)

		var followerNodes []*testutils.Node
		for _, n := range c.Nodes() {
			if n == leader {
				continue
			}
			node := n
			// Wait for the follower to materialize the schema before it can
			// compute writes on top of it.
			testutils.Eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
				var count int64
				return node.DB.QueryRow("SELECT count(*) FROM records").Scan(&count) == nil
			}, "follower to materialize the schema")
			followerNodes = append(followerNodes, node)
		}

		gen := newGenerator(GinkgoRandomSeed())
		deadline := time.Now().Add(correctnessDuration())
		for i := 0; time.Now().Before(deadline); i++ {
			o := gen.step()
			_, err := plainDB.Exec(o.sql, o.args...)
			Expect(err).NotTo(HaveOccurred(), "plain db: %s", o.sql)
			// Rotate through followers so both forward through the leader.
			fn := followerNodes[i%len(followerNodes)]
			rows := execForwarded(t, fn, o)

			// A conditional write that changed no rows may be a false no-op: its
			// WHERE clause read this follower's snapshot, which can lag another
			// node's just-committed write. A no-op produces no WAL frames, so it
			// never reaches the gate, so the base-index check that rejects a
			// stale frame-producing write never runs -- and the op can diverge
			// from the up-to-date reference silently. Wait for the follower to
			// catch up and re-run against fresh state; safe because the first
			// run committed nothing. Frame-producing writes are already caught
			// by the base check, so they need no wait.
			if rows == 0 && o.conditional {
				waitFollowerCurrent(t, c, fn)
				execForwarded(t, fn, o)
			}
		}

		for _, n := range c.Nodes() {
			testutils.Eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
				return tableCountsMatch(plainDB, n.DB, "records", "pending_changes")
			}, fmt.Sprintf("node %s to converge with the plain db", n.ID))
		}

		assertIntegrityOK(plainDB)
		want := dumpTables(plainDB, "records", "pending_changes")
		for _, n := range c.Nodes() {
			assertIntegrityOK(n.DB)
			assertExternalIntegrityOK(n.DBPath)
			Expect(dumpTables(n.DB, "records", "pending_changes")).To(Equal(want),
				"node %s diverged from the plain db", n.ID)
		}
	})

	It("keeps a plain SQLite db and a 3-node cluster identical under mixed trigger-driven writes", func() {
		t := GinkgoT()
		dir := correctnessDir(t)

		plainDB := openPlainDB(t, dir)
		defer plainDB.Close()

		c := testutils.NewTCPCluster(t, dir, 3, testutils.WithOnDiskRaftStore(), testutils.WithSnapshotInterval(time.Second))
		defer c.Shutdown()
		leader := c.ReadyLeader()

		applySchema(plainDB, leader.DB)

		gen := newGenerator(GinkgoRandomSeed())
		for i := 0; i < correctnessIterations(); i++ {
			By(fmt.Sprintf("running workload iteration %d", i+1))

			runWorkload(gen, plainDB, leader.DB, correctnessDuration())

			By("verifying table counts converge across all nodes and the plain db")

			for _, n := range c.Nodes() {
				testutils.Eventually(t, 10*time.Second, 50*time.Millisecond, func() bool {
					return tableCountsMatch(plainDB, n.DB, "records", "pending_changes")
				}, fmt.Sprintf("node %s to converge with the plain db", n.ID))
			}

			By("verifying PRAGMA integrity_check is ok on all nodes and the plain db")

			assertIntegrityOK(plainDB)
			for _, n := range c.Nodes() {
				assertIntegrityOK(n.DB)
				assertExternalIntegrityOK(n.DBPath)
			}

			By("verifying the full table contents are identical across all nodes and the plain db")

			want := dumpTables(plainDB, "records", "pending_changes")
			for _, n := range c.Nodes() {
				Expect(dumpTables(n.DB, "records", "pending_changes")).To(Equal(want),
					"node %s diverged from the plain db", n.ID)
			}
		}
	})
})

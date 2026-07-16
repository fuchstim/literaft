# literaft

![literaft interactive TUI](res/screenshot.png)

**Replicate a SQLite database across a cluster — without patching SQLite and
without giving up on-disk file compatibility.**

literaft is a pure-Go (no cgo) `database/sql` driver that turns an ordinary
single-node SQLite database into a RAFT-replicated one. Your application keeps
talking to `database/sql` exactly as before; literaft replicates every
committed write across the cluster and keeps each replica byte-for-byte
identical. It's built on
[`ncruces/go-sqlite3`](https://github.com/ncruces/go-sqlite3) and
[`hashicorp/raft`](https://github.com/hashicorp/raft).

- **Drop-in `database/sql` driver.** No custom query API — use it like any
  other SQL driver.
- **Strongly consistent writes.** Every write is serialized through a single
  RAFT leader and becomes visible only after it reaches quorum, so replicas
  never diverge and there are no conflicts to merge.
- **Writes on any node.** The leader handles writes, but followers can accept
  them too and forward them to the leader using the provided `log.ForwardingLog`.
- **Concurrent connections.** Open as many read-write connections as you like
  from a process, with the same concurrency as stock SQLite in WAL mode — reads
  never block on replication, only commits do.
- **External read-only access.** The `.db`, `-wal`, and `-shm` files stay
  byte-for-byte SQLite-compatible, so an *unmodified* SQLite process (e.g. the
  `sqlite3` CLI) can open the database read-only while a node is running.

See [Consistency guarantees](#consistency-guarantees) below for exactly what
literaft does and does not promise.

## Why literaft

literaft grew out of a specific frustration: wanting a small, strongly-consistent
database shared across the pods of a single Kubernetes deployment, without
standing up any additional infrastructure to get it. The usual answer — run (or
pay for) an external Postgres/MySQL, or a separate database cluster — is a lot
of moving parts and operational surface for what is often a modest amount of
shared state.

SQLite is the obvious fit for "modest amount of state," but it's single-node: a
file on one pod isn't reachable by the others, and there's no failover if that
pod dies. literaft closes that gap. Each pod embeds the library and opens the
database in-process; the pods form a Raft cluster among *themselves* over the
cluster network, replicating every write so all of them converge on identical
data and any of them can take over if the leader's pod is rescheduled. There's
no separate database process to deploy, no external service to depend on, and
nothing on the network but the pods talking directly to one another — the
database *is* the application, replicated. It stays plain SQLite on disk, so a
sidecar or debug shell can still open the file read-only with the stock
`sqlite3` tooling.

## Project status

literaft already replicates writes across a real multi-node cluster: followers
serve reads, nodes can be added and removed while the cluster is live, and a
node that falls too far behind catches up automatically via snapshots. It's
still in early development and should be treated as experimental.
See [`the roadmap`]([docs/ROADMAP.md](https://github.com/fuchstim/literaft/milestones)) for more details.

## Install

```sh
go get github.com/fuchstim/literaft
```

## Quickstart

The example below runs a single-node cluster in one process — a minimal
version of [`cmd/literaft`](cmd/literaft). It wires the pieces a node needs: a
gRPC server hosting the raft transport and the write-forwarding service, a
[`fsm.FSM`](fsm/fsm.go) (owns the replicated SQLite database), an `*hraft.Raft`
(standard `hashicorp/raft`, here with in-memory stores), a
[`log.ForwardingLog`](log/forward.go) wrapping a
[`log.SingleWriterLog`](log/singlewriter.go) (adapts raft to the gate and
forwards follower writes to the leader), and
[`driver.New`](driver/driver.go) (wires it all into a `database/sql` driver).

```go
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"time"

	grpctransport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fuchstim/literaft/cmd/literaft/forward"
	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/fsm"
	"github.com/fuchstim/literaft/log"
)

func main() {
	const (
		nodeID   = "node-1"
		bindAddr = "127.0.0.1:9001"
	)

	logger := hclog.New(&hclog.LoggerOptions{Level: hclog.Info})

	// fsm.New enables WAL mode on the database file and takes ownership of its
	// lifecycle (checkpointing, follower-apply, snapshots).
	f, err := fsm.New("node.db", fsm.WithLogger(logger))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// One gRPC server per node hosts both the raft transport and the
	// write-forwarding service, so bindAddr is the only address a node exposes.
	lis, err := net.Listen("tcp", bindAddr)
	if err != nil {
		panic(err)
	}
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	tm := grpctransport.New(hraft.ServerAddress(bindAddr), dialOptions)
	grpcServer := grpc.NewServer()
	tm.Register(grpcServer)

	// The forwarding transport shares that same server, so the leader's forward
	// address is just its raft address -- an identity resolver.
	fwd := forward.New(func(a hraft.ServerAddress) string { return string(a) }, dialOptions)
	fwd.Register(grpcServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server stopped", "error", err)
		}
	}()
	defer grpcServer.Stop()

	// In-memory stores keep the example self-contained. A real deployment uses
	// a persistent LogStore/StableStore (this repo's raftsqlite package) and an
	// on-disk snapshot store instead -- see cmd/literaft.
	store := hraft.NewInmemStore()
	snaps := hraft.NewInmemSnapshotStore()

	config := hraft.DefaultConfig()
	config.LocalID = hraft.ServerID(nodeID)
	config.Logger = logger.Named("raft")

	r, err := hraft.NewRaft(config, f, store, store, snaps, tm.Transport())
	if err != nil {
		panic(err)
	}
	defer r.Shutdown()

	// Bootstrap a new single-node cluster. Additional nodes would join through
	// an existing member instead (see "Running a real cluster" below).
	err = r.BootstrapCluster(hraft.Configuration{
		Servers: []hraft.Server{{ID: config.LocalID, Address: hraft.ServerAddress(bindAddr)}},
	}).Error()
	if err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
		panic(err)
	}

	// log.SingleWriterLog owns leader/ready/drain state; wrapping it in a
	// log.ForwardingLog lets follower connections forward writes to the leader
	// (under a base-index check) rather than rejecting them.
	l := log.NewSingleWriterLog(r, log.WithLogger(logger))
	defer l.Close()
	adapter := log.NewForwardingLog(l, fwd, f, log.WithLogger(logger))

	// driver.New registers a process-unique gated VFS and returns a
	// database/sql-compatible driver. journal_mode=WAL is already set by
	// fsm.New; the driver applies synchronous=NORMAL to every connection.
	d := driver.New(f, adapter, driver.WithLogger(logger))
	defer d.Close()

	sql.Register("literaft", d)
	db, err := sql.Open("literaft", "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// A freshly bootstrapped node needs a moment to elect itself leader before
	// it can accept writes.
	for r.State() != hraft.Leader {
		time.Sleep(50 * time.Millisecond)
	}

	// A committed write has been replicated to a RAFT quorum; the commit blocks
	// for exactly that one round-trip. Reads never do.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		panic(err)
	}
	fmt.Println("committed and replicated")
}
```

Every other connection this process opens against the same registered VFS
name — more `*sql.DB` handles, or another goroutine's `db.Conn()` — gets the
same concurrent read-write semantics as stock SQLite in WAL mode. Reads never
block on the RAFT round-trip; only commits do.

### Rejecting writes on followers

The example above enables write forwarding by wrapping the
`log.SingleWriterLog` in a `log.ForwardingLog`: a write on a follower
connection is shipped to the leader and accepted only if it was computed on the
leader's current applied state (otherwise the client re-runs it against fresher
state). To reject follower writes outright instead — returning a leader hint
the client redirects on — hand the plain `log.SingleWriterLog` straight to
`driver.New` and drop the forwarding transport. `cmd/literaft` forwards by
default and switches to rejection with `-forward-writes=false`; see
[`docs/FOLLOWER_WRITES.md`](docs/FOLLOWER_WRITES.md).

## Running a real cluster

[`cmd/literaft`](cmd/literaft) is a complete node process built on the same
pieces as the example above, adding what a real deployment needs: an on-disk
[`raftsqlite`](raftsqlite) log/stable store, file-based snapshots, a cluster
join/leave control plane, and an interactive SQL REPL for exercising a running
node by hand:

```sh
go build -o literaft ./cmd/literaft

# The first node bootstraps a new single-node cluster (no -join).
./literaft -id node1 -bind 127.0.0.1:9001 -data-dir ./data/node1 -db ./data/node1/db.sqlite

# Each additional node joins through any existing member's address. The join
# request is forwarded to the current leader, which adds the node as a voter.
./literaft -id node2 -bind 127.0.0.1:9002 -data-dir ./data/node2 -db ./data/node2/db.sqlite \
  -join 127.0.0.1:9001

# Decommission a node for good (removes it from the configuration via any
# member, forwarded to the leader). This is separate from stopping a node.
./literaft -leave -id node2 -join 127.0.0.1:9001
```

The raft transport and the membership control plane share one gRPC server per
node, so `-bind` is the only address a node exposes. Membership is **durable**:
an ordinary shutdown leaves the configuration untouched, so restarting a node
(a new binary, a crash) brings it back automatically — it's still a voter, and
the leader resumes replicating to it. If a node comes back at a *different*
address, it re-announces itself on startup — through any reachable member, or
the `-join` hint if given — so the leader learns the new address and
reconnects. Removing a node is therefore an explicit act (`-leave`, or
`RemoveVoter` over gRPC), not a side effect of stopping the process. The
leader's REPL also has `.addvoter <id> <address>` for adding a member by hand.

Follower connections accept writes by default, forwarded to the leader under
the base-index check; pass `-forward-writes=false` to reject them with a leader
hint instead.

## Consistency guarantees

literaft replicates **physical page images** through RAFT, applied in the same
total order on every node, so all replicas converge to byte-for-byte identical
`.db`/`-wal`/`-shm` files. What that does and does not guarantee:

- **Totally-ordered writes, no merges.** Every committed write is serialized
  through RAFT's single-leader log. There is no page-level conflict resolution
  anywhere — conflicts are *prevented* by that total order, never merged.
- **Quorum-gated visibility and durability.** A write becomes visible (to local
  readers and to external SQLite processes) only after it reaches RAFT quorum:
  the commit-frame gate withholds the transaction's commit frame until RAFT
  commits. A rejected write's commit frame never reaches disk, so it isn't even
  crash-recoverable. Durability comes from the quorum, not from local `fsync`
  (`synchronous=NORMAL` is expected).
- **Read-your-writes on the node that wrote.** Once `COMMIT` returns, every
  subsequent read on that node — including an external, unmodified SQLite
  process reading its files — observes the write.
- **Follower reads can be stale.** A follower serves reads from its own
  most-recently-applied state, which may lag the leader. literaft does **not**
  provide linearizable cross-node reads: a read on one node is not guaranteed to
  see a write another node's `COMMIT` just returned.
- **Writes are leader-only.** A write attempted on a follower is rejected with a
  leader hint (the client redirects), *unless* the node is configured with the
  opt-in write-forwarding transport (`cmd/literaft` enables it by default). A
  forwarded write is accepted only if its page images were computed on exactly
  the leader's current applied state; otherwise it is rejected as stale and the
  client re-runs it against fresher state.

### The stale-read no-op caveat

Follower reads being stale-able has one sharp edge worth calling out. A
*read-modify-write* issued on a follower — an `UPDATE`/`DELETE` with a `WHERE`
clause, or an `INSERT ... SELECT` — evaluates its condition against the
follower's local, possibly-stale snapshot.

- If the statement **changes rows**, its page images are validated against the
  leader's current state before being accepted; if the follower was stale, the
  write is rejected and re-run against fresher state, so correctness holds.
- But if the stale read makes the statement **match nothing**, it produces no
  changes at all — so it never enters the RAFT log, is never validated, and
  returns success as a local no-op. Against the up-to-date cluster state the
  same statement might have modified rows.

So a conditional write on a follower can silently do nothing based on a stale
view. An application that needs a read-modify-write evaluated against the latest
committed cluster state should issue it on the leader (or ensure the follower
has caught up first). See [`docs/FOLLOWER_WRITES.md`](docs/FOLLOWER_WRITES.md)
for the full treatment.

## How literaft compares

Several projects add replication to SQLite; they make different trade-offs.
The short version: literaft is an *embedded* library (a `database/sql` driver,
no separate server or sidecar) that replicates *physical page images* through
Raft and keeps the on-disk files byte-for-byte stock-SQLite-compatible, all in
pure Go.

|                            | **literaft**                          | [Litestream](https://litestream.io) | [dqlite](https://dqlite.io) | [rqlite](https://rqlite.io) |
| -------------------------- | ------------------------------------- | ------------------------------------ | --------------------------- | --------------------------- |
| **What it is**             | Embedded replicated SQLite            | Streaming backup / disaster recovery | Embedded replicated SQL engine | Standalone replicated SQL database |
| **How you use it**         | `database/sql` driver, in-process     | Sidecar process beside your DB       | C library (Go via cgo)      | Separate server, HTTP/JSON API |
| **Consensus**              | Raft (synchronous quorum)             | None — async ship to object storage  | Raft (synchronous quorum)   | Raft (synchronous quorum)   |
| **Replicates**             | Physical page images                  | WAL pages → S3/Azure/etc.            | WAL frames                  | SQL statements              |
| **Data storage**           | On disk                               | On disk                              | In memory (Raft log on disk) | On disk                    |
| **Interactive transactions**| Yes                                  | Yes (plain SQLite)                   | Yes                         | No — batched statements only |
| **HA / automatic failover**| Yes (multi-node)                      | No — restore from a backup           | Yes                         | Yes                         |
| **External SQLite readers**| Yes — files stay stock-compatible     | Yes — it's your normal file          | No — patched SQLite + custom storage | No — the server owns the file |
| **Runtime**                | Pure Go, no cgo                       | Go                                   | C                           | Go                          |

- **[Litestream](https://litestream.io)** solves a different problem:
  continuous, asynchronous backup of a single SQLite database to object storage
  for point-in-time recovery. It isn't a consensus system — there's no quorum
  and no live multi-node failover, so a crash can lose the writes not yet
  shipped. literaft instead gates each commit on a synchronous Raft quorum.
- **[dqlite](https://dqlite.io)** is architecturally the closest — it also
  replicates at the WAL/page level over Raft — but it's a C library built on a
  *patched* SQLite with its own storage format and wire protocol, reached from
  Go through cgo. It also keeps the *entire database in memory*, persisting only
  the Raft log, so the dataset has to fit in RAM. literaft is pure Go on stock
  SQLite and keeps the database on disk, so the `.db`/`-wal`/`-shm` files stay
  readable by any unmodified SQLite process and the working set isn't bounded by
  memory.
- **[rqlite](https://rqlite.io)** is a standalone server you talk to over
  HTTP/JSON, replicating SQL *statements* through Raft. It has no interactive
  transactions — statements are batched into a single request, so you can't
  `BEGIN`, read a result, and then decide whether to `COMMIT` or `ROLLBACK`.
  literaft is an embedded driver your process links directly (no network hop, no
  separate service) and, being a real `database/sql` driver over on-disk SQLite,
  supports interactive transactions and replicates page images (so
  non-deterministic SQL replicates correctly).

Litestream, dqlite, and rqlite are all mature, widely deployed projects;
literaft is younger (pre-1.0). Pick the one whose model fits — literaft's niche
is *strongly-consistent, embedded, stock-compatible* replication in a pure-Go
process.

## Further reading

- [`docs/DESIGN.md`](docs/DESIGN.md) — the complete design: write/read/
  checkpoint/follower-apply paths, external-reader safety, conflict handling.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) — ADR log of what was rejected and
  why.
- [`docs/FOLLOWER_WRITES.md`](docs/FOLLOWER_WRITES.md) — the write-forwarding
  protocol: base-index check, the skip-marker state machine, and the failure
  matrix.
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — milestone status, mirrored from
  GitHub issues.
- [`docs/WAL_FORMAT.md`](docs/WAL_FORMAT.md) — on-disk byte layout reference.
- [`docs/NCRUCES_NOTES.md`](docs/NCRUCES_NOTES.md) — notes specific to
  building on `ncruces/go-sqlite3`.

## Contributing

The engine is pure Go (wazero) — no cgo, so a plain checkout builds and tests
without extra toolchain setup:

```sh
go build ./...
```

The test suites are [Ginkgo](https://onsi.github.io/ginkgo/)/Gomega; run them
with the `ginkgo` CLI:

```sh
ginkgo -r --procs=20 ./...
```

Start with [`docs/DESIGN.md`](docs/DESIGN.md) for the architecture, and
[`docs/DECISIONS.md`](docs/DECISIONS.md) for the reasoning behind the design.

# literaft

A `database/sql` driver that turns a single-node SQLite database into a
RAFT-replicated one — without patching SQLite and without giving up
on-disk file compatibility. Pure Go (no cgo), built on
[`github.com/ncruces/go-sqlite3`](https://github.com/ncruces/go-sqlite3) and
[`github.com/hashicorp/raft`](https://github.com/hashicorp/raft).

- **WAL mode**, gated at the WAL commit frame: a write becomes visible only
  after it reaches RAFT quorum.
- **Multiple read-write connections from the same process**, with the same
  concurrency as stock SQLite in WAL mode. The only added cost is that each
  commit blocks for one RAFT round-trip.
- **Read-only connections from other processes.** The `.db`, `-wal`, and
  `-shm` files stay byte-for-byte SQLite-compatible, so an *unmodified*
  SQLite process (e.g. the `sqlite3` CLI) can open the database read-only
  while a node is running.

Writes are **leader-only**, and there is no conflict merging anywhere: a write
on a follower is either rejected with a leader hint, or — with the opt-in
forwarding transport — sent to the leader and accepted only if it was computed
on the leader's current state. See [Consistency
guarantees](#consistency-guarantees) below for the full picture,
[`docs/DESIGN.md`](docs/DESIGN.md) for the design, and
[`docs/DECISIONS.md`](docs/DECISIONS.md) for why alternatives were rejected.

## Status

M0–M6 are done: wrapper VFS, external-reader compatibility, the commit-frame
gate, follower apply, real `hashicorp/raft` integration, leadership-churn
correctness, and snapshot-based catch-up for far-behind followers. Opt-in
follower write-forwarding (M9) is also built, and `cmd/literaft` turns it on by
default. The current milestone is M7 hardening — fault injection, fuzzing, and
a few known flakes. See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the
up-to-date, per-issue status.

## Install

```sh
go get github.com/fuchstim/literaft
```

## Quickstart

The pieces: an `*hraft.Raft` (transport + log/stable/snapshot stores, all
standard `hashicorp/raft` types), a [`fsm.FSM`](fsm/fsm.go) (owns the
replicated SQLite database), a [`log.SingleWriterLog`](log/singlewriter.go)
(adapts `*hraft.Raft` to the interface the gate needs), and
[`driver.New`](driver/driver.go) (wires the two into a `database/sql`
driver).

```go
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/fsm"
	"github.com/fuchstim/literaft/log"
)

func main() {
	// fsm.New enables WAL mode on dbPath and takes ownership of the
	// database's lifecycle (checkpointing, follower-apply, snapshots).
	f, err := fsm.New("node.db")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Standard hraft wiring. A real deployment uses a network transport and
	// a persistent LogStore/StableStore (this repo's own raftsqlite package,
	// or raft-boltdb) instead of the in-memory ones below -- see
	// cmd/literaft/main.go, which runs raft over a gRPC transport.
	addr, transport := hraft.NewInmemTransport("")
	config := hraft.DefaultConfig()
	config.LocalID = hraft.ServerID("node-1")

	stableLog := hraft.NewInmemStore()
	snaps := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(config, f, stableLog, stableLog, snaps, transport)
	if err != nil {
		panic(err)
	}

	err = r.BootstrapCluster(hraft.Configuration{
		Servers: []hraft.Server{{ID: config.LocalID, Address: addr}},
	}).Error()
	if err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
		panic(err)
	}

	// log.SingleWriterLog owns leader/ready/drain state and translates
	// hraft's failure modes into *log.NotLeaderError / log.CatchingUpError.
	singleWriterLog := log.NewSingleWriterLog(r)
	defer singleWriterLog.Close()

	// driver.New registers a process-unique gated VFS and returns a
	// database/sql-compatible driver.Driver. journal_mode=WAL is already
	// set by fsm.New; driver.Driver applies synchronous=NORMAL to every
	// pooled connection it opens.
	d := driver.New(f, singleWriterLog)
	defer d.Close()

	sql.Register("literaft", d)
	db, err := sql.Open("literaft", "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := writeWithRetry(db, d, `CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		panic(err)
	}
}

// writeWithRetry demonstrates the leader-only write contract: a write on a
// follower, or on a leader still draining its apply backlog after just
// winning an election, fails fast rather than blocking or resolving via
// conflict merging (there is no conflict merging anywhere in this system).
// d.LastRejection() recovers the concrete rejection reason, since it doesn't
// reliably survive the round trip back through database/sql.
func writeWithRetry(db *sql.DB, d *driver.Driver, query string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := db.Exec(query)
		if err == nil {
			return nil
		}

		var notLeader *log.NotLeaderError
		var catchingUp log.CatchingUpError // returned by value, not by pointer
		rejection := d.LastRejection()
		switch {
		case errors.As(rejection, &notLeader):
			// Not the leader. In a real cluster, redirect the client to
			// notLeader.Leader instead of retrying here.
		case errors.As(rejection, &catchingUp):
			// Elected leader, still draining a backlog from before this
			// term. Safe to retry shortly.
		default:
			return err
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for a leader: %w", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
```

Every other connection this process opens against the same registered VFS
name — more `*sql.DB` handles, or another goroutine's `db.Conn()` — gets the
same concurrent read-write semantics as stock SQLite in WAL mode. Reads never
block on the RAFT round-trip; only commits do.

### Accepting writes on followers

The quickstart above wires the leader-only contract: a `log.SingleWriterLog`
handed straight to `driver.New`, so a write on a follower fails fast. To let
follower connections accept writes instead, wrap that log in a
[`log.ForwardingLog`](log/forward.go) with a `log.LeaderTransport` and pass
*that* to `driver.New`. A follower then forwards its captured write to the
leader, which accepts it only if it was computed on the leader's current
applied state (otherwise the client re-runs it against fresher state).
`cmd/literaft` sets this up by default; see
[`docs/FOLLOWER_WRITES.md`](docs/FOLLOWER_WRITES.md).

## Running a real cluster

[`cmd/literaft`](cmd/literaft) is a complete node process built on the same
four pieces above, with a gRPC transport
([`raft-grpc-transport`](https://github.com/Jille/raft-grpc-transport)), an
on-disk [`raftsqlite`](raftsqlite) log/stable store, file-based snapshots, and
an interactive SQL REPL for exercising a running node by hand:

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

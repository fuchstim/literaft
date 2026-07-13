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

Writes are **leader-only**: a write attempted on a follower is rejected with
a leader hint rather than resolved via conflict merging. See
[`docs/DESIGN.md`](docs/DESIGN.md) for the full design and
[`docs/DECISIONS.md`](docs/DECISIONS.md) for why alternatives were rejected.

## Status

M0–M6 are done: wrapper VFS, external-reader compatibility, the commit-frame
gate, follower apply, real `hashicorp/raft` integration, leadership-churn
correctness, and snapshot-based catch-up for far-behind followers. Current
work is M7 hardening (fault injection, fuzzing, a couple of known flakes) —
see [`docs/ROADMAP.md`](docs/ROADMAP.md) for the up-to-date list.

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

## Further reading

- [`docs/DESIGN.md`](docs/DESIGN.md) — the complete design: write/read/
  checkpoint/follower-apply paths, external-reader safety, conflict handling.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) — ADR log of what was rejected and
  why.
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — milestone status, mirrored from
  GitHub issues.
- [`docs/WAL_FORMAT.md`](docs/WAL_FORMAT.md) — on-disk byte layout reference.
- [`docs/NCRUCES_NOTES.md`](docs/NCRUCES_NOTES.md) — notes specific to
  building on `ncruces/go-sqlite3`.

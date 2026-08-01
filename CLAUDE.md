# CLAUDE.md

Context for a RAFT-backed SQLite VFS, implemented in Go on top of
`github.com/ncruces/go-sqlite3`. This file is the entry point. There is no
separate design-doc corpus: `README.md` carries the high-level overview, and
the code is the reference for everything below that. Read the relevant
package before touching the write or follower-apply paths (start from
`internal/vfs`, `internal/fsm/walappender`, and `raft/gate`).

The system replicates a single-node SQLite database over RAFT without
patching SQLite and without breaking on-disk file compatibility: WAL mode
throughout, multiple read-write connections from the same process at stock
SQLite concurrency, and read-only connections from other (unmodified SQLite)
processes. RAFT itself is an existing library; this project is the VFS, not
the consensus algorithm.

---

## Commit-frame gating

A WAL transaction becomes visible when the wal-index header's `mxFrame`
advances, not when frames land in the `-wal` file. The gate intercepts the
write of the transaction's **commit frame** one step earlier (24-byte frame
header, bytes 4–7 non-zero identifies it), holds it while RAFT reaches
quorum, and only lets SQLite proceed to publish if RAFT commits. A rejected
txn is not crash-recoverable: its commit frame never reached disk. WAL is
single-writer, RAFT is single-leader: the WAL write lock is the per-node
serializer, RAFT is the cross-node one.

---

## Invariants (do not break these)

- **Writes are leader-only.** A write attempted on a follower fails the gate.
  There is *no* page-level conflict resolution anywhere in the system:
  conflicts are *prevented* by RAFT's total order, not reconciled after the
  fact. Merge logic has no place in this codebase.
- **RAFT log entries are physical redo:** an ordered list of `(pgno, page_image)`
  plus `nTruncate` (post-commit db size / commit marker). Not raw WAL bytes
  (salts + checksums are per-node), not SQL statements (non-determinism).
- **Apply is strictly in-order and gapless.** A full page image is only valid
  against the exact base state it was computed on. Never reorder, skip, or
  parallelize apply, even across entries touching disjoint pages. Total
  ordering is a *correctness precondition* here, not just a consistency nicety.
- **Fixed cluster-wide page size.** Enforce it at open.
- **The leader publishes via SQLite itself.** On the leader the shm is never
  poked directly on the commit path: the withheld commit frame is released
  and SQLite runs its normal `walIndexWriteHdr`. The shm is manipulated
  directly only on the **follower-apply** path.
- **Keep ≥1 RW connection open per node process** so the `-shm` wal-index stays
  live; otherwise a lone external reader hits the read-only-shm fallback.

---

## GitHub workflow

GitHub issues and milestones are the source of truth for outstanding work.
There is no roadmap document mirroring them; check GitHub directly.

**GitHub project tracking**

- Every tracked issue lives in project 3
  (https://github.com/users/fuchstim/projects/3, repo `fuchstim/literaft`).
- Status: the project's `Status` field is `Done` for completed items (issue
  also closed) or `Backlog` otherwise.
- IDs for scripting this via `gh api`/`gh project`: project id
  `PVT_kwHOATlZK84Bc32q`, `Status` field id `PVTSSF_lAHOATlZK84Bc32qzhXdftE`,
  options `Backlog`=`f75ad846`, `Done`=`98236657`. `gh project item-edit`
  passes single-select option IDs via `-f` (not `-F`): `-F` auto-coerces a
  numeric-looking ID into a JSON number and the mutation rejects it.

**Implementing a ticket**

- Check out a new branch before starting work on an issue; never commit
  straight to `master`. Branch names are prefixed with the local user's handle or GitHub username
  (e.g. `tfuchs/m7-fault-injection`).
- Open a PR against `master` that links back to the issue (e.g. `Closes #<n>`
  in the PR body) so merging it auto-closes the issue.
- Exception: if the user explicitly asks in the current turn to commit/push
  straight to `master` (not just approves a normal git action in passing),
  that instruction overrides the branch+PR rule above.

---

## Build / test

Standard Go. The engine is pure Go (wazero), no cgo.

```
go build ./...
```

**Protobuf / gRPC codegen lives in the `Makefile`.** The `.pb.go` and
`_grpc.pb.go` files under each `**/proto/` directory (`raft/proto`,
`cmd/literaft/forward/proto`, `cmd/literaft/membership/proto`) are generated
from the sibling `.proto` with `buf` (each proto package carries a
`//go:generate buf generate` and its own `buf.gen.yaml`); don't hand-edit
them. Regenerate everything with `make generate`, the single codegen entry
point: it installs the pinned toolchain (`buf`, `protoc-gen-go`, and
`protoc-gen-go-grpc`, the last required for the gRPC *service* stubs, e.g.
`cmd/literaft/membership`) and then runs `go generate ./...` with that
toolchain on `PATH`. Bump a tool by editing its version variable in the
`Makefile`. A `buf.gen.yaml` that emits gRPC stubs must list the
`protoc-gen-go-grpc` plugin in addition to `protoc-gen-go`.

Validate changes with `make test/unit`, not `go test ./...` or a bare
`ginkgo` invocation directly: this repo's suites are Ginkgo/Gomega, and the
target wires up `vet`, `--race`, coverage, and the `./integration` skip
consistently. Pass extra ginkgo flags through the `GINKGO_ARGS` make
variable, and narrow to specific packages with `GINKGO_PACKAGES` (default
`./...`):

```
make test/unit GINKGO_ARGS="--focus focused-test"
make test/unit GINKGO_PACKAGES=./internal/vfs/...
```

To check whether a test is flaky, use `--repeat N` (runs the suite N+1
times, must pass every time) rather than `--until-it-fails` wrapped in a
shell timeout: `--repeat` has a real, deterministic stopping point instead
of needing an external time bound:

```
make test/unit GINKGO_ARGS="--repeat 10" GINKGO_PACKAGES=./internal/vfs/...
```

Before opening a PR, run `make build`: it runs `go mod tidy`, `go mod
vendor`, `make generate`, and both `make test/unit test/correctness` (via
goreleaser's `before.hooks`) ahead of the actual build, vetting and testing
everything. This takes a while, so don't run it after every commit; run it
once changes are code-complete and ready for review.

Platform: develop on Linux (amd64/arm64) or macOS; both have full file-lock
and shm support in ncruces. Linux uses **OFD locks**; macOS too. Do **not**
rely on the `sqlite3_dotlk` build tag for anything user-facing: it makes shm
in-process only, which breaks external-reader compatibility. Default build
tags only.

Recommended PRAGMAs on every connection: `journal_mode=WAL`,
`synchronous=NORMAL` (durability comes from the RAFT quorum, not local fsync).

**Reading Go package source.** When you need to read the source of a
dependency (e.g. `github.com/ncruces/go-sqlite3`), read it from the local
`./vendor/` directory, not the system-wide Go module cache
(`$GOPATH/pkg/mod`). `./vendor/` is a plain `go mod vendor` mirror of the full
dependency tree kept around locally for exactly this purpose; refresh it
with `go mod vendor` if it looks stale rather than falling back to the module
cache.

---

## Public API style

Every public constructor (`New`, `Open`, `Start`, ...) on a public interface
takes its required arguments directly as positional parameters, never
bundled into a config/options struct alongside optional ones. Optional
arguments use the **functional options pattern** instead: an unexported
`options` struct carrying the defaults, `type Option func(*options)`,
exported `WithXxx(...) Option` constructors, and a trailing `...Option`
parameter on the real constructor. Rationale: a struct field can't
distinguish "caller explicitly set the zero value" from "caller didn't set
it at all," and a variadic `...Option` list can grow new options without
breaking existing call sites, unlike adding a field to a struct that already
has callers relying on its zero value. `raft/gate/leader/` (`options.go`,
`leadergate.New`) is the reference example.

---

## Code comments

Keep comments brief, factual, and technical: state what's non-obvious, not a
mini-essay. No fluff, no filler, no em-dashes, no narrative framing. Don't
reference other functions, files, issues, or PRs by name; such a pointer
goes stale the moment the referenced thing is renamed or moved, and the
comment becomes wrong. Say what's true right here.

---

## Documentation style

There is no `docs/` corpus; `README.md` is the only prose documentation and
the code is the reference for everything else. Keep `README.md`, and this
file, as a neutral technical reference: factual, technical, and
informational. No fluff, no filler, no em-dashes, no narrative framing (no
dramatized section titles, rhetorical questions, storytelling asides, or
emphasis-for-drama). Use plain declarative statements, and prefer commas,
colons, semicolons, or parentheses over dashes. Preserve technical precision:
keep exact file paths, package and symbol names, cross-references, tables,
and code blocks intact. Numeric-range en-dashes (e.g. "bytes 4–7") are
acceptable.

---

## Gotchas

- **`SharedMemory` is opaque.** The exported interface has unexported methods;
  you cannot drive it. Follower-apply needs its own implementation of the
  concrete shm impl (`internal/fsm/walappender/shm`).
- **wal-index page-map ≠ the future OCC pagemap.** "Update the shm page map"
  during apply means the *wal-index's* pgno→frame hash slots (SQLite format,
  required so readers find frames). The OCC pgno→hash/version map from the
  forwarding discussion is a *different*, deferred structure. Don't conflate.
- **Header publish must be tear-safe.** When apply pokes the wal-index, replicate
  SQLite's two-copy header + barrier protocol exactly, or concurrent readers
  (incl. external processes) will see torn state. Highest-risk code in the repo.
- **External-reader compatibility is a claim to verify, not assume.** ncruces
  (OFD locks) vs stock SQLite (POSIX `F_SETLK`) interop needs an actual test
  when touched, not an assumption that it still holds.
- **Wrapping `File`/`VFS` must forward every optional capability interface.**
  `FileSharedMemory`, `FileLockState`, `FileCheckpoint`, `FileUnwrap`, and the
  rest are satisfied via type assertion, not declared on the base `File`
  interface. A wrapper that doesn't re-expose them (`internal/vfs/gatedwalfile.go`'s
  `vfsutil.WrapXxx` calls, one per capability) silently disables WAL/shm and
  other features for the wrapped file instead of erroring.
- **The main `.db` file's own SHARED lock stops external readers from deleting
  a live WAL out from under a node.** `raft/fsm/dblock.go` holds this lock
  (a plain OS byte-range lock on the main db file, unrelated to anything in
  `-shm`) for the node's whole lifetime. Without it, an ordinary transient
  external reader closing its own connection can correctly conclude it is the
  last connection with the database open anywhere, and checkpoint-and-delete
  `-wal`/`-shm`, orphaning every not-yet-checkpointed frame. It must be
  acquired only *after* `PRAGMA journal_mode=WAL` has already succeeded on
  that connection: acquired earlier, it collides with the one-time
  rollback-journal-to-WAL conversion and the connection gets `SQLITE_BUSY`.
- **The walappender shm write lock is an OFD lock: same-OFD lock requests
  convert instead of conflicting, and any unlock on that OFD releases the
  whole byte range, regardless of which goroutine acquired it.** A second
  in-process acquirer of that lock (a forwarded-write handler materializing
  under a loaned lock, on the leader) cannot just call the same lock/unlock
  calls concurrently with the FSM goroutine: it silently fails to exclude,
  and an early unlock lets a local writer interleave mid-protocol, tearing
  the WAL and wal-index. `raft/fsm/fsm.go`'s loan mechanism
  (`BeginHeldApply`/`loans`) exists to front the OS lock with an in-process
  mutex and suppress the appender's own lock/unlock while a lock is loaned out.
- **Own the apply-progress counter; don't read it off the RAFT library.**
  `raft/fsm/fsm.go`'s `lastApplied` is a separate atomic counter, advanced
  only once a page image is actually materialized (or, for a loaned apply,
  once the WAL append under that loan completes). The underlying RAFT
  library's own applied-index bookkeeping advances as soon as an entry is
  dispatched to the FSM goroutine, before `Apply` runs, so it can lead the
  local database; using it as a base-index or readiness check reopens a
  lost-update window.
- **A local publish failure after RAFT commit is fatal, not retryable.** If
  materializing an already-committed entry fails locally
  (`internal/vfs/publish_fatal_internal_test.go`), this node's disk has
  silently diverged from the cluster. Swallowing the error or retrying would
  let that divergence persist invisibly; crash instead.
- **A wrapped error type needs its own `Unwrap`.** Embedding an `error` field
  only promotes `Error() string`, not `Unwrap() error`. A custom error type
  without an explicit `Unwrap` method silently breaks `errors.As`/`errors.Is`
  discovery of what it wraps the moment it passes through another
  `fmt.Errorf("...: %w", ...)` layer (`raft/errors/errors.go`'s error types
  all implement it for this reason).
- **Ambiguous commit.** RAFT "proposed, outcome unknown" must be treated as
  failure by the gate, which means a txn can fail locally yet commit
  cluster-wide. Client-request-ID dedup in the apply path is how you avoid
  double-apply on retry. (Deferred with forwarding, but keep the hook in mind.)
- **The self-apply skip must stay transient, never permanent, and must
  survive a second consumer.** `FSM.Apply` (`raft/fsm/fsm.go`) skips
  materializing an entry only while a per-proposal skip marker for its
  `Header.Id` is `pending`; `CreateSkipMarker`/`DeleteSkipMarker` scope that
  marker to one in-flight proposal (set before, cleared right after, the
  proposal's own local `Apply` call), and `tryConsumeSkipMarker` flips it to
  `skipped` on the one materialization it's meant to cover. A static,
  permanent check (e.g. keying off a node ID instead of a per-proposal token)
  breaks replay: hraft's Figure-8 rule can retroactively commit a
  self-authored entry from an earlier, unfinished leadership stint, and
  `FSM.Restore` can reset local state to an older snapshot, after which every
  self-authored entry past that snapshot needs to replay normally since the
  restore just made it genuinely missing again. Both would silently and
  permanently diverge that node's local disk from the cluster. The marker is
  also a three-state CAS (`pending`/`skipped`/`abandoned`, not a plain bool)
  because `AwaitSkipMarkerConsumed` can time out waiting on a marker that a
  concurrent forwarded-write path never ends up consuming; that dual-wait
  timeout is load-bearing, not a nicety, or the waiter can hang behind a
  marker that will never resolve.

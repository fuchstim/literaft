# cmd/literaft

Runs one literaft node process and, once it's up, an interactive SQL REPL on
stdin/stdout for exercising it by hand.

## Starting a node

```
literaft -id <id> -bind <host:port> -data-dir <dir> -db <path> [flags]
```

| Flag         | Meaning                                                                 |
| ------------ | ------------------------------------------------------------------------ |
| `-id`        | This node's unique, stable ID (its `hraft.ServerID`).                    |
| `-bind`      | Address the raft transport listens on, e.g. `127.0.0.1:9001`.            |
| `-data-dir`  | Directory for this node's raft log/stable/snapshot store.                |
| `-db`        | Path to the SQLite database file this node serves.                       |
| `-page-size` | Cluster-wide fixed SQLite page size (default `4096`).                    |
| `-bootstrap` | Bootstrap a brand new cluster with `-peers` (only on the initial voters, only once). |
| `-peers`     | Comma-separated `id=addr` list of every initial voter, including this node; required with `-bootstrap`. |

Bootstrapping a single-node cluster (handy for local testing of the REPL
itself):

```
literaft -id n0 -bind 127.0.0.1:9001 \
  -data-dir ./data/n0 -db ./data/n0.db \
  -bootstrap -peers n0=127.0.0.1:9001
```

Joining a node to an already-running cluster leaves off `-bootstrap`/`-peers`:
start the node process on its own, then add it as a voter from the leader's
REPL with `.addvoter` (see below).

## Using the REPL

Once a node is up (watch stderr for `literaft: node "<id>" listening on
...`), it reads SQL from stdin and prints results to stdout:

```
literaft> CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
OK
literaft> INSERT INTO t (v) VALUES ('hello'), ('world');
OK
literaft> SELECT id, v FROM t;
id      v
1       hello
2       world
literaft> .exit
```

- A statement can span multiple lines; the REPL keeps prompting with `...>`
  until the accumulated input's last line ends in `;`. This is a simple
  end-of-line check, not a real SQL tokenizer -- it doesn't know about
  strings or comments, so e.g. a string literal ending in a literal `;` on
  its own line would be treated as a terminator.
- Query results print as a tab-separated header row followed by one row per
  result; `NULL` prints as `NULL`, and blobs print as `<blob N bytes>` rather
  than raw bytes. A statement with no result columns (`INSERT`, `CREATE
  TABLE`, ...) prints `OK`.
- `.exit` or `.quit` ends the session; so does EOF (Ctrl-D, or the input
  stream running out if you piped in a script file).
- `.addvoter <id> <address>` adds a new voter to the cluster (see below).
- Stdin doesn't have to be a terminal: `literaft ... < seed.sql` runs a whole
  script non-interactively, one statement at a time, same as typing it in.

### Adding a voter to the cluster

`.addvoter <id> <address>` wraps hraft's own `raft.AddVoter` directly -- the
same join path `docs/ROADMAP.md` describes for growing a running cluster.
Start the new node process first (so it's already listening on `<address>`
and can be reached once the configuration change commits), then run this
against the *current leader's* REPL:

```
literaft> .addvoter n1 127.0.0.1:9002
OK
```

A malformed command reports usage without touching raft at all:

```
literaft> .addvoter n1
usage: .addvoter <id> <address>
```

It only succeeds against the leader too, but since it calls `raft.AddVoter`
directly rather than going through the SQL commit-frame gate, the error text
is hraft's own rather than the `NotLeaderError`/`CatchingUpError` messages
described below:

```
literaft> .addvoter n1 127.0.0.1:9002
error: node is not the leader
```

### Writes only succeed on the leader

Every statement runs through the same kept-alive connection (and therefore
the same commit-frame gate, see the top-level `CLAUDE.md`) any other client
write would use. A write issued against a follower fails immediately:

```
literaft> INSERT INTO t (v) VALUES ('nope');
error: raft: not the leader (leader hint: 127.0.0.1:9001)
```

A node that just won an election but hasn't finished draining its apply
backlog yet reports the same kind of error instead of silently dropping or
misordering the write:

```
literaft> INSERT INTO t (v) VALUES ('soon');
error: raft: elected leader but still draining the apply backlog
```

In both cases the REPL keeps running -- reconnect to (or wait on) the actual
leader and retry. Reads (`SELECT`) work against any node, leader or
follower; a follower's data may just be slightly behind the leader's.

An unrelated error (bad SQL, a constraint violation, ...) is reported the
same way and likewise doesn't end the session:

```
literaft> SELECT * FROM nope;
error: sqlite3: SQL logic error: no such table: nope
```

// Command literaft runs one node process: a real hraft cluster member
// serving a RAFT-replicated SQLite database, plus an interactive SQL REPL
// on stdin/stdout for exercising a running node by hand.
package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/fsm"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "literaft:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id        = flag.String("id", "", "unique ID for this node (required)")
		bindAddr  = flag.String("bind", "", "raft transport address to listen on, e.g. 127.0.0.1:9000 (required)")
		dataDir   = flag.String("data-dir", "", "directory for this node's raft log/stable/snapshot store (required)")
		dbPath    = flag.String("db", "", "path to the SQLite database file this node serves (required)")
		bootstrap = flag.Bool("bootstrap", false, "bootstrap a new cluster with -peers (only on the initial voters, once)")
		peers     = flag.String("peers", "", "comma-separated id=addr list of every initial voter, including this node; required with -bootstrap")
	)
	flag.Parse()

	if *id == "" || *bindAddr == "" || *dataDir == "" || *dbPath == "" {
		return fmt.Errorf("-id, -bind, -data-dir, and -db are required")
	}

	var bootstrapServers []raft.Server
	if *bootstrap {
		var err error
		bootstrapServers, err = parsePeers(*peers)
		if err != nil {
			return fmt.Errorf("failed to parse peers: %w", err)
		}
	}

	transport, err := raft.NewTCPTransport(*bindAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to start raft transport on %s: %w", *bindAddr, err)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir %s: %w", *dataDir, err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		return errors.Join(transport.Close(), fmt.Errorf("failed to open raft log store: %w", err))
	}

	snapshotStore, err := raft.NewFileSnapshotStore(*dataDir, 2, os.Stderr)
	if err != nil {
		return errors.Join(boltStore.Close(), transport.Close(), fmt.Errorf("failed to open raft snapshot store: %w", err))
	}

	fsm, err := fsm.New(*dbPath)
	if err != nil {
		return errors.Join(boltStore.Close(), transport.Close(), fmt.Errorf("failed to create FSM: %w", err))
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(*id)
	raftConfig.LogOutput = os.Stderr

	r, err := raft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return errors.Join(fsm.Close(), boltStore.Close(), transport.Close(), fmt.Errorf("failed to start raft: %w", err))
	}

	if *bootstrap {
		err = r.BootstrapCluster(raft.Configuration{Servers: bootstrapServers}).Error()
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return errors.Join(r.Shutdown().Error(), fsm.Close(), boltStore.Close(), transport.Close(), fmt.Errorf("failed to bootstrap cluster: %w", err))
		}
	}

	fmt.Fprintf(os.Stderr, "literaft: node %q listening on %s (db %s)\n", *id, *bindAddr, *dbPath)

	d := driver.New(r, fsm)
	defer d.Close()

	sql.Register("literaft", d)

	db, err := sql.Open("literaft", "")
	if err != nil {
		return errors.Join(r.Shutdown().Error(), fsm.Close(), boltStore.Close(), transport.Close(), fmt.Errorf("failed to open database: %w", err))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// runREPL blocks reading os.Stdin, which can't be interrupted directly;
	// running it in its own goroutine lets a signal on sigCh win the race
	// and shut down immediately instead of waiting for stdin to produce
	// another line. A REPL goroutine left blocked on stdin in that case dies
	// with the process, same as any other in-flight work at signal time.
	replDone := make(chan bool, 1)
	go func() {
		replDone <- runREPL(r, db, os.Stdin, os.Stdout)
	}()

	// Only an explicit .exit/.quit means "shut this node down now". A bare
	// EOF -- stdin from /dev/null under a headless launch, or a piped
	// script finishing -- must not stop a node that's still supposed to
	// serve raft traffic and reads, so fall back to waiting on a real
	// signal instead.
	select {
	case <-sigCh:
	case explicitExit := <-replDone:
		if !explicitExit {
			<-sigCh
		}
	}

	fmt.Fprintln(os.Stderr, "literaft: shutting down")
	return errors.Join(db.Close(), r.Shutdown().Error(), fsm.Close(), boltStore.Close(), transport.Close())
}

// parsePeers parses a comma-separated "id=addr" list into the raft.Server
// configuration a bootstrapping node needs to list every initial voter,
// itself included.
func parsePeers(s string) ([]raft.Server, error) {
	if s == "" {
		return nil, fmt.Errorf("must list every initial voter as id=addr, comma-separated")
	}
	var servers []raft.Server
	for _, part := range strings.Split(s, ",") {
		idAddr := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(idAddr) != 2 || idAddr[0] == "" || idAddr[1] == "" {
			return nil, fmt.Errorf("invalid peer %q, want id=addr", part)
		}
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(idAddr[0]),
			Address:  raft.ServerAddress(idAddr[1]),
		})
	}
	return servers, nil
}

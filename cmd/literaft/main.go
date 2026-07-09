// Command literaft runs one node process: a real hraft cluster member
// serving a RAFT-replicated SQLite database. Wiring
// lives in internal/node; this is the flag parsing and process lifecycle
// around it, plus an interactive SQL REPL (repl.go) on stdin/stdout for
// exercising a running node by hand.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	hraft "github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/internal/node"
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
		pageSize  = flag.Uint("page-size", 4096, "cluster-wide fixed SQLite page size")
		bootstrap = flag.Bool("bootstrap", false, "bootstrap a new cluster with -peers (only on the initial voters, once)")
		peers     = flag.String("peers", "", "comma-separated id=addr list of every initial voter, including this node; required with -bootstrap")
	)
	flag.Parse()

	if *id == "" || *bindAddr == "" || *dataDir == "" || *dbPath == "" {
		return fmt.Errorf("-id, -bind, -data-dir, and -db are required")
	}

	cfg := node.Config{
		ID:        *id,
		BindAddr:  *bindAddr,
		DataDir:   *dataDir,
		DBPath:    *dbPath,
		PageSize:  uint32(*pageSize),
		LogOutput: os.Stderr,
	}

	if *bootstrap {
		servers, err := parsePeers(*peers)
		if err != nil {
			return fmt.Errorf("-peers: %w", err)
		}
		cfg.Bootstrap = servers
	}

	n, err := node.Start(cfg)
	if err != nil {
		return fmt.Errorf("starting node: %w", err)
	}
	fmt.Fprintf(os.Stderr, "literaft: node %q listening on %s (db %s)\n", *id, *bindAddr, *dbPath)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// runREPL blocks reading os.Stdin, which can't be interrupted directly;
	// running it in its own goroutine lets a signal on sigCh win the race
	// and shut down immediately instead of waiting for stdin to produce
	// another line. A REPL goroutine left blocked on stdin in that case dies
	// with the process, same as any other in-flight work at signal time.
	replDone := make(chan bool, 1)
	go func() {
		replDone <- runREPL(n, os.Stdin, os.Stdout)
	}()

	// Only an explicit .exit/.quit means "shut this node down now". A bare
	// EOF (runREPL's doc comment: stdin from /dev/null under a headless
	// launch, or a piped script finishing) must not stop a node that's
	// still supposed to serve raft traffic and reads -- fall back to
	// waiting on a real signal instead.
	select {
	case <-sigCh:
	case explicitExit := <-replDone:
		if !explicitExit {
			<-sigCh
		}
	}

	fmt.Fprintln(os.Stderr, "literaft: shutting down")
	return n.Shutdown()
}

// parsePeers parses a comma-separated "id=addr" list into the raft.Server
// configuration a bootstrapping node needs to list every initial voter,
// itself included.
func parsePeers(s string) ([]hraft.Server, error) {
	if s == "" {
		return nil, fmt.Errorf("must list every initial voter as id=addr, comma-separated")
	}
	var servers []hraft.Server
	for _, part := range strings.Split(s, ",") {
		idAddr := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(idAddr) != 2 || idAddr[0] == "" || idAddr[1] == "" {
			return nil, fmt.Errorf("invalid peer %q, want id=addr", part)
		}
		servers = append(servers, hraft.Server{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(idAddr[0]),
			Address:  hraft.ServerAddress(idAddr[1]),
		})
	}
	return servers, nil
}

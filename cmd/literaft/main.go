// Command literaft runs one node process: a real hraft cluster member
// serving a RAFT-replicated SQLite database over a gRPC transport, plus an
// interactive SQL REPL on stdin/stdout for exercising a running node by hand.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	hclogwrapper "github.com/zaffka/zap-to-hclog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/fsm"
	"github.com/fuchstim/literaft/internal/membership"
	"github.com/fuchstim/literaft/log"
	"github.com/fuchstim/literaft/raftsqlite"
)

// joinTimeout bounds the -join handshake: dialing an existing member and
// waiting for the leader to commit the AddVoter configuration change. It does
// not bound the subsequent catch-up (log replication or snapshot install),
// which proceeds asynchronously once this node is in the configuration.
const joinTimeout = 15 * time.Second

// leaveTimeout bounds the best-effort RemoveVoter a node sends on shutdown.
const leaveTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "literaft:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id       = flag.String("id", "", "unique ID for this node (required)")
		bindAddr = flag.String("bind", "", "gRPC transport address to listen on, e.g. 127.0.0.1:9000 (required)")
		dataDir  = flag.String("data-dir", "", "directory for this node's raft log/stable/snapshot store (required)")
		dbPath   = flag.String("db", "", "path to the SQLite database file this node serves (required)")
		joinAddr = flag.String("join", "", "address of any existing cluster member to join through; if empty, bootstrap a new single-node cluster")
		logLevel = flag.String("log-level", "info", "log level (debug, info, warn, error, fatal, panic)")
	)
	flag.Parse()

	if *id == "" || *bindAddr == "" || *dataDir == "" || *dbPath == "" {
		return fmt.Errorf("-id, -bind, -data-dir, and -db are required")
	}

	lvl, err := zapcore.ParseLevel(*logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level %q: %w", *logLevel, err)
	}

	logger, err := zap.NewDevelopment(zap.IncreaseLevel(lvl))
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer logger.Sync()

	lis, err := net.Listen("tcp", *bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", *bindAddr, err)
	}

	// Insecure creds are deliberate: the same options dial peers (raft
	// transport) and forward membership calls to the leader, so they must
	// match the server the gRPC listener runs.
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	tm := transport.New(raft.ServerAddress(*bindAddr), dialOptions)
	grpcServer := grpc.NewServer()
	tm.Register(grpcServer)

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return errors.Join(lis.Close(), fmt.Errorf("failed to create data dir %s: %w", *dataDir, err))
	}

	raftStore, err := raftsqlite.New(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		return errors.Join(lis.Close(), fmt.Errorf("failed to open raft store: %w", err))
	}

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(*dataDir, 2, hclogwrapper.Wrap(logger.Named("raft-snapshot")))
	if err != nil {
		return errors.Join(raftStore.Close(), lis.Close(), fmt.Errorf("failed to open raft snapshot store: %w", err))
	}

	// Whether this node already has persisted raft state decides first-start
	// vs restart: only a first start bootstraps or joins. A restart just comes
	// back up and lets hraft recover its configuration and log.
	hasState, err := raft.HasExistingState(raftStore, raftStore, snapshotStore)
	if err != nil {
		return errors.Join(raftStore.Close(), lis.Close(), fmt.Errorf("failed to check for existing raft state: %w", err))
	}

	f, err := fsm.New(*dbPath)
	if err != nil {
		return errors.Join(raftStore.Close(), lis.Close(), fmt.Errorf("failed to create FSM: %w", err))
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(*id)
	raftConfig.Logger = hclogwrapper.Wrap(logger.Named("raft"))

	r, err := raft.NewRaft(raftConfig, f, raftStore, raftStore, snapshotStore, tm.Transport())
	if err != nil {
		return errors.Join(f.Close(), raftStore.Close(), lis.Close(), fmt.Errorf("failed to start raft: %w", err))
	}

	// The membership service shares the raft transport's gRPC server, so a
	// joining node reaches it at the same -bind address. Both services must be
	// registered before Serve.
	membership.NewServer(r, dialOptions).Register(grpcServer)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server stopped", zap.Error(err))
		}
	}()

	shutdown := func(cause error) error {
		return errors.Join(cause, r.Shutdown().Error(), f.Close(), raftStore.Close(), grpcClose(grpcServer, tm))
	}

	if !hasState {
		if *joinAddr == "" {
			cfg := raft.Configuration{Servers: []raft.Server{{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(*id),
				Address:  raft.ServerAddress(*bindAddr),
			}}}
			if err := r.BootstrapCluster(cfg).Error(); err != nil {
				return shutdown(fmt.Errorf("failed to bootstrap cluster: %w", err))
			}
			logger.Info("bootstrapped new single-node cluster", zap.String("id", *id), zap.String("bind", *bindAddr))
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
			err := membership.Join(ctx, *joinAddr, *id, *bindAddr, dialOptions)
			cancel()
			if err != nil {
				return shutdown(fmt.Errorf("failed to join cluster via %s: %w", *joinAddr, err))
			}
			logger.Info("joined cluster", zap.String("id", *id), zap.String("bind", *bindAddr), zap.String("via", *joinAddr))
		}
	}

	logger.Info("node listening", zap.String("id", *id), zap.String("bind", *bindAddr), zap.String("db", *dbPath))

	l := log.NewSingleWriterLog(r)
	defer l.Close()

	d := driver.New(f, l)
	defer d.Close()

	sql.Register("literaft", d)

	db, err := sql.Open("literaft", "")
	if err != nil {
		return shutdown(fmt.Errorf("failed to open database: %w", err))
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

	logger.Info("shutting down")
	leaveCluster(r, *id, dialOptions, logger)
	return errors.Join(db.Close(), r.Shutdown().Error(), f.Close(), raftStore.Close(), grpcClose(grpcServer, tm))
}

// leaveCluster sends a best-effort RemoveVoter for this node to the current
// leader before shutdown, so the rest of the cluster drops it from the
// configuration promptly instead of waiting to time it out. Failure only
// warns: a node that can't reach a leader still needs to shut down, and the
// leader will eventually notice it gone. A node that is the cluster's only
// voter has no one to tell and nothing to leave, so it skips the call.
func leaveCluster(r *raft.Raft, id string, dialOptions []grpc.DialOption, logger *zap.Logger) {
	if cfg := r.GetConfiguration(); cfg.Error() == nil {
		servers := cfg.Configuration().Servers
		if len(servers) == 1 && servers[0].ID == raft.ServerID(id) {
			return
		}
	}

	leaderAddr, _ := r.LeaderWithID()
	if leaderAddr == "" {
		logger.Warn("no known leader; skipping graceful cluster leave")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), leaveTimeout)
	defer cancel()
	if err := membership.Leave(ctx, string(leaderAddr), id, dialOptions); err != nil {
		logger.Warn("failed to remove self from cluster on shutdown", zap.Error(err))
	}
}

// grpcClose tears down the gRPC server that hosts both the raft transport and
// the membership service. The transport must be closed first: its inbound RPC
// handlers park on the raft engine's RPC channel and only unblock when the
// transport's own shutdown channel closes (they don't watch the request
// context), so stopping the gRPC server first could wait forever on a handler
// the raft engine -- already shut down by now -- will never answer.
func grpcClose(grpcServer *grpc.Server, tm *transport.Manager) error {
	err := tm.Close()
	grpcServer.GracefulStop()
	return err
}

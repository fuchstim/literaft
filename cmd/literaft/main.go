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

	"github.com/fuchstim/literaft/cmd/literaft/forward"
	"github.com/fuchstim/literaft/cmd/literaft/membership"
	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/fsm"
	"github.com/fuchstim/literaft/log"
	"github.com/fuchstim/literaft/raftsqlite"
)

// joinTimeout bounds the -join handshake: dialing an existing member and
// waiting for the leader to commit the AddVoter configuration change. It does
// not bound the subsequent catch-up (log replication or snapshot install),
// which proceeds asynchronously once this node is in the configuration.
const joinTimeout = 15 * time.Second

// leaveTimeout bounds the RemoveVoter the -leave decommission command sends.
const leaveTimeout = 5 * time.Second

// reannounceTargetTimeout bounds each attempt to re-announce a changed address
// through one member before falling back to the next.
const reannounceTargetTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "literaft:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id            = flag.String("id", "", "unique ID for this node (required)")
		bindAddr      = flag.String("bind", "", "gRPC transport address to listen on, e.g. 127.0.0.1:9000 (required)")
		dataDir       = flag.String("data-dir", "", "directory for this node's raft log/stable/snapshot store (required)")
		dbPath        = flag.String("db", "", "path to the SQLite database file this node serves (required)")
		joinAddr      = flag.String("join", "", "address of any existing cluster member to join through; if empty, bootstrap a new single-node cluster")
		leave         = flag.Bool("leave", false, "decommission mode: remove -id from the cluster via -join, then exit (does not start a node)")
		logLevel      = flag.String("log-level", "info", "log level (debug, info, warn, error, fatal, panic)")
		forwardWrites = flag.Bool("forward-writes", true, "accept writes on follower connections by forwarding them to the leader under a base-index check; when false, follower writes are rejected with a leader hint")
	)
	flag.Parse()

	lvl, err := zapcore.ParseLevel(*logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level %q: %w", *logLevel, err)
	}

	logger, err := zap.NewDevelopment(zap.IncreaseLevel(lvl))
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer logger.Sync()

	// Insecure creds are deliberate: the same options dial peers (raft
	// transport) and forward membership calls to the leader, so they must
	// match the server every node's gRPC listener runs.
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// -leave is a one-shot admin command, not a node: it removes -id from the
	// cluster by asking any member (-join), which forwards to the leader.
	// Ordinary shutdown never removes a node -- membership is durable across
	// restarts -- so this is the explicit, deliberate way to decommission one.
	if *leave {
		if *id == "" || *joinAddr == "" {
			return fmt.Errorf("-leave requires -id and -join")
		}
		ctx, cancel := context.WithTimeout(context.Background(), leaveTimeout)
		defer cancel()
		if err := membership.Leave(ctx, *joinAddr, *id, dialOptions); err != nil {
			return fmt.Errorf("failed to decommission %s via %s: %w", *id, *joinAddr, err)
		}
		logger.Info("decommissioned node", zap.String("id", *id), zap.String("via", *joinAddr))
		return nil
	}

	if *id == "" || *bindAddr == "" || *dataDir == "" || *dbPath == "" {
		return fmt.Errorf("-id, -bind, -data-dir, and -db are required")
	}

	lis, err := net.Listen("tcp", *bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", *bindAddr, err)
	}

	tm := transport.New(raft.ServerAddress(*bindAddr), dialOptions)
	grpcServer := grpc.NewServer()
	tm.Register(grpcServer)

	// reg collects each resource's cleanup as it's constructed. Every fallible
	// step past this point returns reg.shutdown(err) rather than its own
	// errors.Join chain, so a failure tears down exactly what was built so far.
	var reg shutdownRegistry
	reg.add(func() error { return closeGRPC(grpcServer, tm, lis) })

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return reg.shutdown(fmt.Errorf("failed to create data dir %s: %w", *dataDir, err))
	}

	raftStore, err := raftsqlite.New(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		return reg.shutdown(fmt.Errorf("failed to open raft store: %w", err))
	}
	reg.add(raftStore.Close)

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(*dataDir, 2, hclogwrapper.Wrap(logger.Named("raft-snapshot")))
	if err != nil {
		return reg.shutdown(fmt.Errorf("failed to open raft snapshot store: %w", err))
	}

	// Whether this node already has persisted raft state decides first-start
	// vs restart: only a first start bootstraps or joins. A restart just comes
	// back up and lets hraft recover its configuration and log -- membership is
	// durable, so a node still in the cluster's configuration rejoins on its
	// own (the leader keeps replicating to it), and a node that was explicitly
	// decommissioned stays out rather than silently re-adding itself.
	hasState, err := raft.HasExistingState(raftStore, raftStore, snapshotStore)
	if err != nil {
		return reg.shutdown(fmt.Errorf("failed to check for existing raft state: %w", err))
	}

	f, err := fsm.New(*dbPath)
	if err != nil {
		return reg.shutdown(fmt.Errorf("failed to create FSM: %w", err))
	}
	reg.add(f.Close)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(*id)
	raftConfig.Logger = hclogwrapper.Wrap(logger.Named("raft"))

	r, err := raft.NewRaft(raftConfig, f, raftStore, raftStore, snapshotStore, tm.Transport())
	if err != nil {
		return reg.shutdown(fmt.Errorf("failed to start raft: %w", err))
	}
	reg.add(func() error { return r.Shutdown().Error() })

	// The membership service shares the raft transport's gRPC server, so a
	// joining node reaches it at the same -bind address. All services must be
	// registered before Serve.
	membership.NewServer(r, dialOptions).Register(grpcServer)

	// The write-forwarding transport shares that same gRPC server too, so a
	// leader's forward address is just its -bind address (identity resolver).
	// Registered here even though its handler is wired later; a request
	// arriving before then is answered Unavailable.
	var fwdTransport *forward.Transport
	if *forwardWrites {
		fwdTransport = forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOptions)
		fwdTransport.Register(grpcServer)
		reg.add(fwdTransport.Close)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server stopped", zap.Error(err))
		}
	}()

	if !hasState {
		if *joinAddr == "" {
			cfg := raft.Configuration{Servers: []raft.Server{{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(*id),
				Address:  raft.ServerAddress(*bindAddr),
			}}}
			if err := r.BootstrapCluster(cfg).Error(); err != nil {
				return reg.shutdown(fmt.Errorf("failed to bootstrap cluster: %w", err))
			}
			logger.Info("bootstrapped new single-node cluster", zap.String("id", *id), zap.String("bind", *bindAddr))
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
			err := membership.Join(ctx, *joinAddr, *id, *bindAddr, dialOptions)
			cancel()
			if err != nil {
				return reg.shutdown(fmt.Errorf("failed to join cluster via %s: %w", *joinAddr, err))
			}
			logger.Info("joined cluster", zap.String("id", *id), zap.String("bind", *bindAddr), zap.String("via", *joinAddr))
		}
	} else {
		// Restart: hraft has recovered our configuration and log. Re-announce
		// our address if it changed while we were down so the leader can reach
		// us again; an unchanged address (the common case) does nothing.
		if err := reannounceIfMoved(r, *id, *bindAddr, *joinAddr, dialOptions, logger); err != nil {
			logger.Warn("could not re-announce changed address on restart; the leader may not reach this node until it is re-added", zap.Error(err))
		}
	}

	logger.Info("node listening", zap.String("id", *id), zap.String("bind", *bindAddr), zap.String("db", *dbPath))

	l := log.NewSingleWriterLog(r)
	reg.add(func() error { l.Close(); return nil })

	// With forwarding enabled, the driver's adapter forwards a follower's
	// write to the leader over fwdTransport; otherwise l is used directly and
	// follower writes are rejected.
	d := driver.New(f, l)
	if *forwardWrites {
		d = driver.New(f, log.NewForwardingLog(l, fwdTransport, f))
	}
	reg.add(func() error { d.Close(); return nil })

	sql.Register("literaft", d)

	db, err := sql.Open("literaft", "")
	if err != nil {
		return reg.shutdown(fmt.Errorf("failed to open database: %w", err))
	}
	reg.add(db.Close)

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
	return reg.shutdown(nil)
}

// shutdownRegistry collects a node's cleanup funcs as they're constructed and,
// on teardown, runs them in reverse registration order -- a stack of deferred
// closes. Because only funcs registered before an early return have been
// added, each failure path cleans up exactly what it managed to build.
type shutdownRegistry struct {
	fns []func() error
}

// add registers a cleanup func. Later-registered funcs run first.
func (s *shutdownRegistry) add(fn func() error) { s.fns = append(s.fns, fn) }

// shutdown runs every registered func in reverse registration order and joins
// their errors with cause (nil on a clean exit). errors.Join drops the nils.
func (s *shutdownRegistry) shutdown(cause error) error {
	errs := []error{cause}
	for i := len(s.fns) - 1; i >= 0; i-- {
		errs = append(errs, s.fns[i]())
	}
	return errors.Join(errs...)
}

// reannounceIfMoved re-announces this node's address to the cluster when it
// changed across a restart. Raft stores (id -> address) in the replicated
// configuration and never rediscovers a moved node, so without this the leader
// would keep dialing our old address. AddVoter with our own ID and the new
// address updates it in place. The call is forwarded to the leader through an
// existing member -- the -join hint first (if given), then peers from our
// recovered configuration -- which works as long as at least one other node is
// still reachable at its recorded address. A restart at an unchanged address,
// the common case, does nothing.
func reannounceIfMoved(r *raft.Raft, id, bindAddr, joinAddr string, dialOptions []grpc.DialOption, logger *zap.Logger) error {
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Errorf("read configuration: %w", err)
	}

	changed, targets := reannounceTargets(future.Configuration().Servers, id, bindAddr, joinAddr)
	if !changed {
		return nil
	}
	if len(targets) == 0 {
		return fmt.Errorf("address changed to %s but no other member to re-announce through", bindAddr)
	}

	var errs []error
	for _, t := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), reannounceTargetTimeout)
		err := membership.Join(ctx, t, id, bindAddr, dialOptions)
		cancel()
		if err == nil {
			logger.Info("re-announced changed address", zap.String("id", id), zap.String("new", bindAddr), zap.String("via", t))
			return nil
		}
		errs = append(errs, fmt.Errorf("via %s: %w", t, err))
	}
	return fmt.Errorf("re-announce of new address %s failed through every member: %w", bindAddr, errors.Join(errs...))
}

// reannounceTargets reports whether a restarted node's address differs from
// what the recovered configuration records for it, and if so the ordered
// member addresses to re-announce through (joinAddr first when set, then the
// other servers). changed is false -- with a nil target list -- when the
// address is unchanged or the node isn't in the configuration (e.g. it was
// decommissioned), so nothing should be done.
func reannounceTargets(servers []raft.Server, id, bindAddr, joinAddr string) (changed bool, targets []string) {
	var recorded string
	inConfig := false
	var peers []string
	for _, s := range servers {
		if string(s.ID) == id {
			recorded, inConfig = string(s.Address), true
			continue
		}
		peers = append(peers, string(s.Address))
	}

	if !inConfig || recorded == bindAddr {
		return false, nil
	}
	if joinAddr != "" {
		return true, append([]string{joinAddr}, peers...)
	}
	return true, peers
}

// closeGRPC tears down the gRPC server that hosts both the raft transport and
// the membership service, plus its listener. The transport is closed first:
// its inbound RPC handlers park on the raft engine's RPC channel and only
// unblock when the transport's own shutdown channel closes (they don't watch
// the request context), so stopping the gRPC server first could wait forever
// on a handler the raft engine -- already shut down by now -- will never
// answer. GracefulStop closes the listener once it has been served; the
// explicit Close covers the early-teardown paths where Serve was never
// reached, tolerating the already-closed case.
func closeGRPC(grpcServer *grpc.Server, tm *transport.Manager, lis net.Listener) error {
	err := tm.Close()
	grpcServer.GracefulStop()
	if cerr := lis.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		err = errors.Join(err, cerr)
	}
	return err
}

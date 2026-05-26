package raftnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/fuchstim/literaft/internal/ctxlog"
	"github.com/fuchstim/literaft/internal/lifecycle"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftNode struct {
	mu sync.Mutex

	params *Params
	addr   string

	listener   net.Listener
	transport  *transport.Manager
	grpcServer *grpc.Server
	boltStore  *boltdb.BoltStore
	raft       *raft.Raft
}

func New(lc *lifecycle.Lifecycle, params *Params) (*RaftNode, error) {
	n := &RaftNode{
		params: params,
		addr:   fmt.Sprintf("%s:%d", params.Host, params.Port),
	}

	lc.Append(fx.StartStopHook(n.start, n.stop))

	return n, nil
}

func (n *RaftNode) start(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.raft != nil {
		return errors.New("raft node already started")
	}

	ctxlog.Info(ctx, "starting raft node", zap.String("addr", n.addr))

	listener, err := net.Listen("tcp", n.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", n.addr, err)
	}
	n.listener = listener

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(n.addr)
	cfg.HeartbeatTimeout = n.params.HeartbeatInterval
	cfg.LeaderLeaseTimeout = n.params.LeaderHeartbeatTimeout

	n.transport = transport.New(
		raft.ServerAddress(n.addr),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	)

	if err := os.MkdirAll(n.params.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir %s: %w", n.params.DataDir, err)
	}
	boltPath := filepath.Join(n.params.DataDir, "raft.dat")
	boltStore, err := boltdb.NewBoltStore(boltPath)
	if err != nil {
		return fmt.Errorf("open bolt store %s: %w", boltPath, err)
	}
	n.boltStore = boltStore

	snapshotStore, err := newFileSnapshotStore(filepath.Join(n.params.DataDir, "snapshots"))
	if err != nil {
		return err
	}

	r, err := raft.NewRaft(
		cfg,
		newEmptyFSM(),
		n.boltStore,
		n.boltStore,
		snapshotStore,
		n.transport.Transport(),
	)
	if err != nil {
		return fmt.Errorf("raft.NewRaft: %w", err)
	}
	n.raft = r

	if err := n.bootstrap(); err != nil {
		return err
	}

	n.grpcServer = grpc.NewServer()
	n.transport.Register(n.grpcServer)

	go func() {
		if err := n.grpcServer.Serve(listener); err != nil {
			ctxlog.Error(ctx, "grpc server stopped", zap.Error(err))
		}
	}()

	return nil
}

func (n *RaftNode) stop(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	ctxlog.Info(ctx, "stopping raft node")

	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			return fmt.Errorf("raft shutdown: %w", err)
		}
	}

	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}

	if n.boltStore != nil {
		if err := n.boltStore.Close(); err != nil {
			return fmt.Errorf("close bolt store: %w", err)
		}
	}

	return nil
}

func (n *RaftNode) bootstrap() error {
	servers := []raft.Server{
		{Suffrage: raft.Voter, ID: raft.ServerID(n.addr), Address: raft.ServerAddress(n.addr)},
	}
	for _, peer := range n.params.Peers {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(peer),
			Address:  raft.ServerAddress(peer),
		})
	}

	err := n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
	if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return fmt.Errorf("bootstrap cluster: %w", err)
	}
	return nil
}

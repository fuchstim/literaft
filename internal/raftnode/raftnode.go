package raftnode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/fuchstim/sqlite-raft/internal/ctxlog"
	"github.com/fuchstim/sqlite-raft/internal/lifecycle"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftNode struct {
	params *Params
	addr   string

	listener   net.Listener
	transport  *transport.Manager
	grpcServer *grpc.Server
	logStore   *boltdb.BoltStore
	raft       *raft.Raft
}

func New(lc *lifecycle.Lifecycle, params *Params) (*RaftNode, error) {
	n := &RaftNode{
		params: params,
		addr:   fmt.Sprintf("%s:%d", params.Host, params.Port),
	}

	lc.Append(fx.StartStopHook(n.Start, n.Stop))

	return n, nil
}

func (n *RaftNode) Start(ctx context.Context) error {
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
	logPath := filepath.Join(n.params.DataDir, "logs.bolt")
	logStore, err := boltdb.NewBoltStore(logPath)
	if err != nil {
		return fmt.Errorf("open log store %s: %w", logPath, err)
	}
	n.logStore = logStore

	r, err := raft.NewRaft(
		cfg,
		newEmptyFSM(),
		n.logStore,
		newEmptyStableStore(),
		newEmptySnapshotStore(),
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

func (n *RaftNode) Stop(ctx context.Context) error {
	ctxlog.Info(ctx, "stopping raft node")

	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			return fmt.Errorf("raft shutdown: %w", err)
		}
	}

	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}

	if n.logStore != nil {
		if err := n.logStore.Close(); err != nil {
			return fmt.Errorf("close log store: %w", err)
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

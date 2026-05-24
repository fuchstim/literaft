package raftnode

import "time"

type Params struct {
	Host                   string        `config:"name='raft.host',default='localhost',usage='Host for the Raft node'"`
	Port                   int           `config:"name='raft.port',default=8080,usage='Port for the Raft node'"`
	HeartbeatInterval      time.Duration `config:"name='raft.heartbeat_interval',default=5s,usage='Interval between heartbeats'"`
	LeaderHeartbeatTimeout time.Duration `config:"name='raft.leader_heartbeat_timeout',default=6s,usage='Timeout for leader heartbeats'"`
	Peers                  []string      `config:"name='raft.peers',default='[]',usage='Peer addresses in the format host:port'"`
}

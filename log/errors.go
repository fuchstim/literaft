package log

import (
	"fmt"

	"github.com/hashicorp/raft"
)

// NotLeaderError is returned by SingleWriterLog.Apply when this node isn't
// the raft leader. A follower rejects the write outright rather than
// forwarding it; Leader carries the current leader's address (empty if
// unknown) so a caller can redirect.
type NotLeaderError struct {
	Leader raft.ServerAddress
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "not the leader (leader unknown)"
	}
	return fmt.Sprintf("not the leader (leader hint: %s)", e.Leader)
}

// CatchingUpError is returned by SingleWriterLog.Apply when this node has
// just won an election but hasn't yet finished draining its apply backlog.
// The node is the raft leader but its local SQLite state may not yet
// reflect every entry the cluster has already committed, so serving a
// write now could silently drop or reorder causally-prior data. Callers
// should retry shortly rather than redirect.
type CatchingUpError struct{}

func (CatchingUpError) Error() string {
	return "elected leader but still draining the apply backlog"
}

package membership

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	membershippb "github.com/fuchstim/literaft/cmd/literaft/membership/proto"
)

// joinRetryDelay paces Join's retries while the cluster is still coming up.
const joinRetryDelay = 200 * time.Millisecond

// Join asks the member at joinAddr to add (id, selfAddr) to the cluster as a
// voter. joinAddr may be any existing member: it forwards to the leader if it
// isn't the leader itself.
//
// Join retries transient Unavailable errors until ctx is done: right after a
// seed node bootstraps, the join target may not be listening yet, or may not
// have elected a leader to forward to -- both are expected during startup and
// resolve on their own.
func Join(ctx context.Context, joinAddr, id, selfAddr string, dialOptions []grpc.DialOption) error {
	conn, err := grpc.NewClient(joinAddr, dialOptions...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", joinAddr, err)
	}
	defer conn.Close()

	client := membershippb.NewMembershipClient(conn)
	req := &membershippb.AddVoterRequest{Id: id, Address: selfAddr}
	for {
		_, err = client.AddVoter(ctx, req)
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Unavailable {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("join via %s timed out: %w", joinAddr, err)
		case <-time.After(joinRetryDelay):
		}
	}
}

// Leave asks the member at addr to remove id from the cluster, forwarded to
// the leader as needed.
func Leave(ctx context.Context, addr, id string, dialOptions []grpc.DialOption) error {
	conn, err := grpc.NewClient(addr, dialOptions...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	_, err = membershippb.NewMembershipClient(conn).RemoveVoter(ctx, &membershippb.RemoveVoterRequest{Id: id})
	return err
}

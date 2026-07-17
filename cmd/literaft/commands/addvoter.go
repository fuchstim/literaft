package commands

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

var _ = registerCommand(NewCommand(
	".addvoter",
	".addvoter <id> <address>",
	"Add a voter to the cluster",
	func(self Command, r *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
		if len(params) != 2 {
			return errors.New(self.Usage())
		}
		id := raft.ServerID(params[0])
		address := raft.ServerAddress(params[1])

		conf := r.GetConfiguration()
		if conf.Error() != nil {
			return fmt.Errorf("failed to get configuration: %w", conf.Error())
		}

		err := r.AddVoter(id, address, conf.Index(), 10*time.Second).Error()
		if err != nil {
			return fmt.Errorf("failed to add voter: %w", err)
		}
		fmt.Fprintln(out, "OK")

		return nil
	},
))

package commands

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/hashicorp/raft"
)

var _ = registerCommand(NewCommand(
	".servers",
	".servers",
	"List all servers in the cluster",
	func(self Command, raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
		if len(params) != 0 {
			return errors.New(self.Usage())
		}

		conf := raft.GetConfiguration()
		if conf.Error() != nil {
			return fmt.Errorf("failed to get configuration: %w", conf.Error())
		}

		servers := conf.Configuration().Servers

		w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		defer w.Flush()

		fmt.Fprintln(w, "ID\tAddress\tRole")
		for _, s := range servers {
			suffrage := s.Suffrage.String()
			if s.Address == raft.Leader() {
				suffrage += " (Leader)"
			}

			fmt.Fprintf(w, "%s\t%s\t%s\n", s.ID, s.Address, suffrage)
		}

		return nil
	},
))

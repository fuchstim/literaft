package commands

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/hashicorp/raft"
)

var commands map[string]Command = make(map[string]Command)

func registerCommand(cmd Command) bool {
	commands[cmd.Name()] = cmd
	return true
}

type CommandHandler struct {
	raft *raft.Raft
	db   *sql.DB
}

func NewCommandHandler(r *raft.Raft, db *sql.DB) *CommandHandler {
	return &CommandHandler{r, db}
}

func (h *CommandHandler) RunREPL(in io.Reader, out io.Writer) bool {
	scanner := bufio.NewScanner(in)

	var buf strings.Builder
	fmt.Fprint(out, "literaft> ")
	for scanner.Scan() {
		firstLine := buf.Len() == 0
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if firstLine {
			switch {
			case trimmed == "":
				fmt.Fprint(out, "literaft> ")
				continue
			case strings.HasPrefix(trimmed, "."):
				if h.Handle(trimmed, out) {
					return true
				}
				fmt.Fprint(out, "literaft> ")
				continue
			}
		}

		buf.WriteString(line)

		if !strings.HasSuffix(trimmed, ";") {
			buf.WriteByte('\n')
			fmt.Fprint(out, "   ...> ")
			continue
		}

		if h.Handle(buf.String(), out) {
			return true
		}

		buf.Reset()
		fmt.Fprint(out, "literaft> ")
	}
	if buf.Len() > 0 {
		fmt.Fprintln(out, "error: unexpected EOF mid-statement")
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
	}
	return false
}

func (h *CommandHandler) Handle(line string, out io.Writer) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}

	if !strings.HasPrefix(trimmed, ".") {
		if err := runStatement(h.db, trimmed, true, out); err != nil {
			fmt.Fprintf(out, "Error: %s\n", err.Error())
		}
		return false
	}

	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return false
	}

	cmdName := fields[0]
	cmd, ok := commands[cmdName]

	switch {
	case cmdName == ".help":
		h.runHelp(out)
		return false
	case cmdName == ".exit" || cmdName == ".quit":
		fmt.Fprintln(out, "Goodbye")
		return true
	case !ok:
		fmt.Fprintf(out, "Error: unknown command: %s\n", cmdName)
		h.runHelp(out)
		return false
	}

	if err := cmd.Run(h.raft, h.db, fields[1:], out); err != nil {
		fmt.Fprintf(out, "Error: %s\n", err.Error())
	}

	return false
}

func (h *CommandHandler) runHelp(out io.Writer) {
	fmt.Fprintln(out, "Available commands:")

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	defer w.Flush()
	for name, cmd := range commands {
		fmt.Fprintf(w, "  %s\t%s\n", name, cmd.Description())
	}
	fmt.Fprintf(w, "  %s\t%s\n", ".help", "Show this help message")
	fmt.Fprintf(w, "  %s\t%s\n", ".exit/.quit", "Exit literaft")
}

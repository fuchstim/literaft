package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/fuchstim/literaft/cmd/literaft/commands"
	"github.com/fuchstim/literaft/internal/version"
	"github.com/hashicorp/raft"
	zone "github.com/lrstanley/bubblezone"
)

// runInteractiveTUI runs the split-pane UI against a live node, returning once
// the operator quits it or a shutdown signal arrives. On a signal it asks the
// program to quit so Bubble Tea restores the terminal before the caller tears
// the node down.
func runInteractiveTUI(id string, r *raft.Raft, db *sql.DB, sink *logSink, sigCh <-chan os.Signal) {
	p := tea.NewProgram(
		newTUIModel(id, r, db, sink),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := p.Run(); err != nil {
			// The alt screen is torn down by the time Run returns, so a rare
			// program error is safe to surface on stderr.
			fmt.Fprintln(os.Stderr, "literaft: tui error:", err)
		}
	}()

	select {
	case <-sigCh:
		p.Quit()
		<-done
	case <-done:
	}
}

// Layout and buffering constants. The transcript and log ring buffers are
// bounded so a long-lived session doesn't grow without limit; the newest
// lines are always kept.
const (
	promptMain = "literaft> "
	promptCont = "   ...> "

	transcriptLimit = 5000
	logLimit        = 5000

	// logBatchMax caps how many buffered log lines one drain coalesces into a
	// single redraw, so a burst can't livelock the render loop.
	logBatchMax = 256

	// borderSize is the columns/rows a rounded border adds on each axis (two
	// sides).
	borderSize    = 2
	minPaneHeight = 3
)

var (
	focusColor = lipgloss.Color("205") // active border / accents
	blurColor  = lipgloss.Color("240") // inactive border

	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	promptStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	helpStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	spinnerStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	headerBarStyle  = lipgloss.NewStyle().Background(lipgloss.Color("236")).Foreground(lipgloss.Color("252"))
	headerNameStyle = lipgloss.NewStyle().Background(lipgloss.Color("205")).Foreground(lipgloss.Color("236")).Bold(true)

	logErrorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	logWarnStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	logDebugStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	logTraceStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))

	leaderStateStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("83")).Bold(true)
	otherStateStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
)

// focusArea is which of the three interactive regions currently has focus:
// the input line, the REPL transcript pane, or the log stream pane. Only the
// focused region receives non-global key and scroll events.
type focusArea int

const (
	focusInput focusArea = iota
	focusRepl
	focusLog
)

// logBatchMsg carries a coalesced batch of log lines drained from the sink.
// closed reports that the sink channel is closed, so the reader command is
// not re-issued.
type logBatchMsg struct {
	lines  []string
	closed bool
}

// stmtResultMsg is the formatted output of a statement or meta-command that
// ran off the UI goroutine.
type stmtResultMsg string

// statusTickMsg fires roughly once a second to refresh the raft-state header.
type statusTickMsg struct{}

// tuiModel is the Bubble Tea model backing the interactive node UI: a SQL
// REPL pane on the left and a live log stream on the right, sharing one
// terminal. SQL and cluster commands run off this goroutine so a blocking
// commit round-trip never freezes the UI.
type tuiModel struct {
	nodeID     string
	raft       *raft.Raft
	sink       *logSink
	cmdHandler *commands.CommandHandler

	input      textinput.Model
	logs, repl viewport.Model
	spinner    spinner.Model
	zone       *zone.Manager

	transcript []string // REPL prompts, echoes, and results
	logLines   []string // colorized log stream lines
	stmtBuf    string   // statement text accumulated across continuation lines

	history    []string
	historyIdx int    // cursor into history; == len(history) means "current draft"
	draft      string // input saved when history navigation begins

	focus    focusArea
	width    int
	height   int
	ready    bool // a WindowSizeMsg has arrived, so sizes are known
	running  bool // a statement/command is executing off-goroutine
	quitting bool
}

func newTUIModel(id string, r *raft.Raft, db *sql.DB, sink *logSink) tuiModel {
	ti := textinput.New()
	ti.Prompt = promptStyle.Render(promptMain)
	ti.Focus()

	sp := spinner.New(spinner.WithSpinner(spinner.MiniDot), spinner.WithStyle(spinnerStyle))

	return tuiModel{
		nodeID:     id,
		raft:       r,
		cmdHandler: commands.NewCommandHandler(r, db),
		sink:       sink,
		input:      ti,
		repl:       viewport.New(0, 0),
		logs:       viewport.New(0, 0),
		spinner:    sp,
		zone:       zone.New(),
		focus:      focusInput,
		historyIdx: 0,
	}
}

func (m tuiModel) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, waitForLog(m.sink.lines()), statusTick())
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.ready = true
		m.recalcLayout()
		return m, nil

	case logBatchMsg:
		m.appendLog(msg.lines)
		if msg.closed {
			return m, nil
		}
		return m, waitForLog(m.sink.lines())

	case stmtResultMsg:
		m.running = false
		m.appendTranscript(string(msg))
		return m, nil

	case statusTickMsg:
		return m, statusTick()

	case spinner.TickMsg:
		if !m.running {
			return m, nil
		}
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case tea.MouseMsg:
		return m.handleMouse(msg)

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	// Anything else (cursor blink, etc.) belongs to the text input.
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m tuiModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit
	case "tab":
		return m, m.cycleFocus(1)
	case "shift+tab":
		return m, m.cycleFocus(-1)
	}

	switch m.focus {
	case focusInput:
		return m.handleInputKey(msg)
	case focusRepl:
		var cmd tea.Cmd
		m.repl, cmd = m.repl.Update(msg)
		return m, cmd
	case focusLog:
		var cmd tea.Cmd
		m.logs, cmd = m.logs.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m tuiModel) handleInputKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		return m, m.submit()
	case "up":
		m.historyPrev()
		return m, nil
	case "down":
		m.historyNext()
		return m, nil
	case "pgup":
		m.repl.ViewUp()
		return m, nil
	case "pgdown":
		m.repl.ViewDown()
		return m, nil
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m tuiModel) handleMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	if msg.Action == tea.MouseActionPress && msg.Button == tea.MouseButtonLeft {
		switch {
		case m.zone.Get("repl").InBounds(msg):
			return m, m.setFocus(focusRepl)
		case m.zone.Get("logs").InBounds(msg):
			return m, m.setFocus(focusLog)
		case m.zone.Get("input").InBounds(msg):
			return m, m.setFocus(focusInput)
		}
		return m, nil
	}

	// Wheel scrolls whichever pane the pointer is over, regardless of focus.
	if msg.Button == tea.MouseButtonWheelUp || msg.Button == tea.MouseButtonWheelDown {
		var cmd tea.Cmd
		switch {
		case m.zone.Get("repl").InBounds(msg):
			m.repl, cmd = m.repl.Update(msg)
		case m.zone.Get("logs").InBounds(msg):
			m.logs, cmd = m.logs.Update(msg)
		}
		return m, cmd
	}
	return m, nil
}

// submit consumes the current input line and either runs it or folds it into
// the in-progress statement. It returns the command that runs a completed
// statement or meta-command off-goroutine, or nil.
func (m *tuiModel) submit() tea.Cmd {
	if m.running {
		return nil
	}
	line := m.input.Value()
	m.input.Reset()
	if strings.TrimSpace(line) != "" {
		m.pushHistory(line)
	}
	m.historyIdx = len(m.history)
	m.draft = ""
	return m.processLine(line)
}

// processLine handles one entered line: meta-commands are recognized only
// when no statement is being accumulated, and a statement is complete once
// its trimmed text ends in ";".
func (m *tuiModel) processLine(line string) tea.Cmd {
	firstLine := m.stmtBuf == ""
	trimmed := strings.TrimSpace(line)

	m.appendTranscript(m.prompt() + line)

	if firstLine && trimmed == "" {
		return nil
	}

	m.stmtBuf += line + "\n"

	if !(firstLine && strings.HasPrefix(trimmed, ".")) && !strings.HasSuffix(trimmed, ";") {
		m.updatePrompt()
		return nil
	}

	stmt := m.stmtBuf
	m.stmtBuf = ""
	m.updatePrompt()
	m.running = true

	cmd := func() tea.Msg {
		var b strings.Builder
		if m.cmdHandler.Handle(stmt, &b) {
			m.quitting = true
			return tea.Quit()
		}
		return stmtResultMsg(b.String())
	}

	return tea.Batch(cmd, m.spinner.Tick)
}

func (m *tuiModel) pushHistory(line string) {
	if n := len(m.history); n > 0 && m.history[n-1] == line {
		return
	}
	m.history = append(m.history, line)
}

// historyPrev walks toward older history entries (the up arrow), saving the
// live draft the first time it leaves the newest position.
func (m *tuiModel) historyPrev() {
	if m.historyIdx == len(m.history) {
		m.draft = m.input.Value()
	}
	if m.historyIdx > 0 {
		m.historyIdx--
		m.input.SetValue(m.history[m.historyIdx])
		m.input.CursorEnd()
	}
}

// historyNext walks toward newer history entries (the down arrow), restoring
// the saved draft once it returns past the newest entry.
func (m *tuiModel) historyNext() {
	if m.historyIdx >= len(m.history) {
		return
	}
	m.historyIdx++
	if m.historyIdx == len(m.history) {
		m.input.SetValue(m.draft)
	} else {
		m.input.SetValue(m.history[m.historyIdx])
	}
	m.input.CursorEnd()
}

func (m *tuiModel) setFocus(f focusArea) tea.Cmd {
	m.focus = f
	if f == focusInput {
		return m.input.Focus()
	}
	m.input.Blur()
	return nil
}

func (m *tuiModel) cycleFocus(dir int) tea.Cmd {
	const n = 3 // focusInput, focusRepl, focusLog
	return m.setFocus(focusArea((int(m.focus) + dir + n) % n))
}

func (m tuiModel) prompt() string {
	if m.stmtBuf == "" {
		return promptMain
	}
	return promptCont
}

func (m *tuiModel) updatePrompt() {
	m.input.Prompt = promptStyle.Render(m.prompt())
}

// appendTranscript splits s into lines, appends them to the REPL transcript
// (trimming the oldest past the limit), and follows the bottom unless the
// operator has scrolled up.
func (m *tuiModel) appendTranscript(s string) {
	for _, ln := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		m.transcript = append(m.transcript, ln)
	}
	if len(m.transcript) > transcriptLimit {
		m.transcript = m.transcript[len(m.transcript)-transcriptLimit:]
	}
	m.syncViewport(&m.repl, m.transcript)
}

func (m *tuiModel) appendLog(lines []string) {
	for _, ln := range lines {
		m.logLines = append(m.logLines, colorizeLog(ln))
	}
	if len(m.logLines) > logLimit {
		m.logLines = m.logLines[len(m.logLines)-logLimit:]
	}
	m.syncViewport(&m.logs, m.logLines)
}

// syncViewport re-renders a pane's content, preserving the operator's manual
// scroll position but auto-following the bottom when already there.
func (m *tuiModel) syncViewport(vp *viewport.Model, lines []string) {
	atBottom := vp.AtBottom()
	vp.SetContent(strings.Join(lines, "\n"))
	if atBottom {
		vp.GotoBottom()
	}
}

func (m *tuiModel) recalcLayout() {
	if !m.ready {
		return
	}
	// Reserve one row each for the header, the input line, and the help line.
	panesH := m.height - 3
	if panesH < minPaneHeight {
		panesH = minPaneHeight
	}
	// A pane box is a bordered viewport with one title row inside it.
	vpH := panesH - borderSize - 1
	if vpH < 1 {
		vpH = 1
	}

	leftBox := m.width / 2
	rightBox := m.width - leftBox
	lvpW := leftBox - borderSize
	rvpW := rightBox - borderSize
	if lvpW < 1 {
		lvpW = 1
	}
	if rvpW < 1 {
		rvpW = 1
	}

	m.repl.Width, m.repl.Height = lvpW, vpH
	m.logs.Width, m.logs.Height = rvpW, vpH

	if w := m.width - lipgloss.Width(m.prompt()) - 1; w > 0 {
		m.input.Width = w
	}

	m.syncViewport(&m.repl, m.transcript)
	m.syncViewport(&m.logs, m.logLines)
}

func (m tuiModel) View() string {
	if m.quitting {
		return ""
	}
	if !m.ready {
		return "starting literaft TUI…"
	}

	panes := lipgloss.JoinHorizontal(
		lipgloss.Top,
		m.renderPane("REPL", m.repl, m.focus == focusRepl, "repl"),
		m.renderPane("Logs", m.logs, m.focus == focusLog, "logs"),
	)

	view := lipgloss.JoinVertical(
		lipgloss.Left,
		m.renderHeader(),
		panes,
		m.zone.Mark("input", m.input.View()),
		m.renderHelp(),
	)
	return m.zone.Scan(view)
}

func (m tuiModel) renderPane(title string, vp viewport.Model, focused bool, zoneID string) string {
	border := blurColor
	if focused {
		border = focusColor
	}
	inner := titleStyle.Render(title) + "\n" + vp.View()
	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(border).
		Width(vp.Width).
		Height(vp.Height + 1) // +1 for the title row
	return m.zone.Mark(zoneID, box.Render(inner))
}

func (m tuiModel) renderHeader() string {
	stateStyle := otherStateStyle
	state := m.raft.State()
	if state == raft.Leader {
		stateStyle = leaderStateStyle
	}
	_, leaderID := m.raft.LeaderWithID()
	leader := string(leaderID)
	if leader == "" {
		leader = "(none)"
	}

	stats := m.raft.Stats()
	term := stats["term"]
	commitIndex := stats["commit_index"]
	appliedIdx := stats["applied_index"]
	fsmPending := stats["fsm_pending"]
	snapshotTerm := stats["last_snapshot_term"]
	snapshotIndex := stats["last_snapshot_index"]

	name := headerNameStyle.Render(fmt.Sprintf(" literaft %s ", version.Version))
	info := fmt.Sprintf(" Node: %s  •  State: %s  •  Leader: %s  •  Term: %s  •  Log Index (committed/applied/pending): %s/%s/%s  •  Snapshot (term/index): %s/%s ", m.nodeID, stateStyle.Render(state.String()), leader, term, commitIndex, appliedIdx, fsmPending, snapshotTerm, snapshotIndex)
	return headerBarStyle.Width(m.width).Render(name + info)
}

func (m tuiModel) renderHelp() string {
	var parts []string
	if m.running {
		parts = append(parts, m.spinner.View()+" running…")
	}
	switch m.focus {
	case focusInput:
		parts = append(parts, "tab: focus", "↑/↓: history", "enter: run", ".help", "ctrl+c: quit")
	default:
		parts = append(parts, "tab: focus", "↑/↓/pgup/pgdn: scroll", "ctrl+c: quit")
	}
	return helpStyle.Render(strings.Join(parts, "  •  "))
}

// colorizeLog tints a whole hclog line by its level token. INFO and anything
// unrecognized keep the default foreground.
func colorizeLog(line string) string {
	switch {
	case strings.Contains(line, "[ERROR]"):
		return logErrorStyle.Render(line)
	case strings.Contains(line, "[WARN]"):
		return logWarnStyle.Render(line)
	case strings.Contains(line, "[DEBUG]"):
		return logDebugStyle.Render(line)
	case strings.Contains(line, "[TRACE]"):
		return logTraceStyle.Render(line)
	default:
		return line
	}
}

// waitForLog blocks for the next log line, then non-blockingly drains any
// others already queued so a burst collapses into one redraw.
func waitForLog(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		first, ok := <-ch
		if !ok {
			return logBatchMsg{closed: true}
		}
		batch := []string{first}
		for {
			select {
			case l, ok := <-ch:
				if !ok {
					return logBatchMsg{lines: batch, closed: true}
				}
				batch = append(batch, l)
				if len(batch) >= logBatchMax {
					return logBatchMsg{lines: batch}
				}
			default:
				return logBatchMsg{lines: batch}
			}
		}
	}
}

func statusTick() tea.Cmd {
	return tea.Tick(time.Second, func(time.Time) tea.Msg { return statusTickMsg{} })
}

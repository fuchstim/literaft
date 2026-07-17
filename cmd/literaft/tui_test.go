package main

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func startSoloNode() (*testutils.TCPCluster, *testutils.Node) {
	GinkgoHelper()
	c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 1)
	n := c.ReadyLeader()
	return c, n
}

func newTestModel() *tuiModel {
	GinkgoHelper()
	m := newTUIModel("n1", nil, nil, newLogSink())
	DeferCleanup(m.zone.Close)
	m.width, m.height = 120, 40
	m.ready = true
	m.recalcLayout()
	return &m
}

var _ = Describe("tuiModel command history", func() {
	It("walks backward and forward through history, preserving the draft", func() {
		m := newTestModel()
		m.history = []string{"first;", "second;", "third;"}
		m.historyIdx = len(m.history)
		m.input.SetValue("half-typed")

		m.historyPrev()
		Expect(m.input.Value()).To(Equal("third;"))
		m.historyPrev()
		Expect(m.input.Value()).To(Equal("second;"))
		m.historyPrev()
		Expect(m.input.Value()).To(Equal("first;"))

		// Already at the oldest: another up is a no-op.
		m.historyPrev()
		Expect(m.input.Value()).To(Equal("first;"))

		m.historyNext()
		Expect(m.input.Value()).To(Equal("second;"))
		m.historyNext()
		Expect(m.input.Value()).To(Equal("third;"))

		// Returning past the newest restores the saved draft.
		m.historyNext()
		Expect(m.input.Value()).To(Equal("half-typed"))

		// Already at the draft: another down is a no-op.
		m.historyNext()
		Expect(m.input.Value()).To(Equal("half-typed"))
	})

	It("records submitted lines and resets the navigation cursor", func() {
		m := newTestModel()
		m.input.SetValue(".help")
		m.submit()

		Expect(m.history).To(Equal([]string{".help"}))
		Expect(m.historyIdx).To(Equal(len(m.history)))
		Expect(m.input.Value()).To(BeEmpty())
	})

	It("does not record blank lines and collapses consecutive duplicates", func() {
		m := newTestModel()
		m.input.SetValue("   ")
		m.submit()
		m.input.SetValue(".help")
		m.submit()
		m.input.SetValue(".help")
		m.submit()

		Expect(m.history).To(Equal([]string{".help"}))
	})
})

var _ = Describe("tuiModel line processing", func() {
	It("accumulates a statement across continuation lines before running it", func() {
		m := newTestModel()

		cmd := m.processLine("CREATE TABLE t (")
		Expect(cmd).To(BeNil())
		Expect(m.running).To(BeFalse())
		Expect(m.prompt()).To(Equal(promptCont))

		cmd = m.processLine("  id INTEGER);")
		Expect(cmd).NotTo(BeNil()) // runs the completed statement
		Expect(m.running).To(BeTrue())
		Expect(m.stmtBuf).To(BeEmpty())
		Expect(m.prompt()).To(Equal(promptMain))

		joined := renderedTranscript(m)
		Expect(joined).To(ContainSubstring("CREATE TABLE t ("))
		Expect(joined).To(ContainSubstring("id INTEGER);"))
	})

	It("quits on .exit, marking the model quitting once the command runs", func() {
		m := newTestModel()
		cmd := m.processLine(".exit")
		Expect(cmd).NotTo(BeNil())
		Expect(m.running).To(BeTrue())

		// The handler resolves .exit off-goroutine; running it returns a quit
		// message and marks the model quitting.
		Expect(drainBatch(cmd)).To(BeAssignableToTypeOf(tea.QuitMsg{}))
		Expect(m.quitting).To(BeTrue())
	})

	It("treats a meta-command as text mid-statement, not as a command", func() {
		m := newTestModel()
		m.processLine("SELECT")       // opens a statement (no trailing ;)
		cmd := m.processLine(".exit") // now part of the statement, not a quit
		Expect(m.quitting).To(BeFalse())
		Expect(cmd).To(BeNil())
		Expect(m.stmtBuf).To(ContainSubstring(".exit"))
	})

	It("runs .addvoter off-goroutine and shows the running indicator", func() {
		m := newTestModel()
		cmd := m.processLine(".addvoter n2 127.0.0.1:9001")
		Expect(cmd).NotTo(BeNil())
		Expect(m.running).To(BeTrue())
	})
})

var _ = Describe("tuiModel panes", func() {
	It("splits the width across two panes and leaves room for chrome", func() {
		m := newTestModel() // 120x40

		Expect(m.repl.Width).To(BeNumerically(">", 0))
		Expect(m.logs.Width).To(BeNumerically(">", 0))
		// Two bordered boxes (2 cols of border each) fill the full width.
		Expect(m.repl.Width + m.logs.Width + 2*borderSize).To(Equal(m.width))
		// Header + input + help rows are reserved above/below the panes.
		Expect(m.repl.Height).To(Equal(m.height - 3 - borderSize - 1))
	})

	It("bounds the log ring buffer to the newest lines", func() {
		m := newTestModel()
		batch := make([]string, logLimit+50)
		for i := range batch {
			batch[i] = "line"
		}
		m.appendLog(batch)
		Expect(m.logLines).To(HaveLen(logLimit))
	})
})

var _ = Describe("tuiModel against a live node", func() {
	var c interface{ Shutdown() }

	newLiveModel := func() *tuiModel {
		GinkgoHelper()
		cluster, n := startSoloNode()
		c = cluster
		m := newTUIModel("n1", n.Raft, n.DB, newLogSink())
		DeferCleanup(m.zone.Close)
		m.width, m.height = 120, 40
		m.ready = true
		m.recalcLayout()
		return &m
	}

	AfterEach(func() {
		if c != nil {
			c.Shutdown()
		}
	})

	It("renders the header, both panes, and the log stream", func() {
		m := newLiveModel()
		m.appendLog([]string{"2026-07-14T00:00:00 [INFO]  literaft: hello world"})
		out := m.View()
		Expect(out).To(ContainSubstring("literaft"))
		Expect(out).To(ContainSubstring("REPL"))
		Expect(out).To(ContainSubstring("Logs"))
		Expect(out).To(ContainSubstring("hello world"))
	})

	It("runs a submitted statement and shows the result in the transcript", func() {
		m := newLiveModel()
		m.input.SetValue("SELECT 1 AS n;")

		cmd := m.submit()
		Expect(m.running).To(BeTrue())
		Expect(cmd).NotTo(BeNil())

		// The batch fans out to the statement runner and the spinner tick;
		// drive it the way Bubble Tea would and feed the result back in.
		var result tea.Msg
		Eventually(func() tea.Msg {
			result = drainBatch(cmd)
			return result
		}).ShouldNot(BeNil())

		updated, _ := m.Update(result)
		final := updated.(tuiModel)
		Expect(final.running).To(BeFalse())
		joined := renderedTranscript(&final)
		Expect(joined).To(ContainSubstring("SELECT 1 AS n;"))
		Expect(joined).To(ContainSubstring("1"))
	})
})

// drainBatch runs cmd (a tea.Batch of the statement/command runner and a
// spinner tick) and returns the first stmtResultMsg or tea.QuitMsg it
// produces, mirroring how Bubble Tea executes batched commands. Spinner ticks
// and nil commands are skipped.
func drainBatch(cmd tea.Cmd) tea.Msg {
	msg := cmd()
	batch, ok := msg.(tea.BatchMsg)
	if !ok {
		switch msg.(type) {
		case stmtResultMsg, tea.QuitMsg:
			return msg
		}
		return nil
	}
	for _, c := range batch {
		if c == nil {
			continue
		}
		switch m := c().(type) {
		case stmtResultMsg:
			return m
		case tea.QuitMsg:
			return m
		}
	}
	return nil
}

// renderedTranscript joins the transcript lines with newlines.
func renderedTranscript(m *tuiModel) string {
	out := ""
	for i, ln := range m.transcript {
		if i > 0 {
			out += "\n"
		}
		out += ln
	}
	return out
}

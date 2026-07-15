package main

import (
	"bytes"
	"sync"
)

// logSinkBuffer bounds how many complete log lines the sink holds before the
// TUI has drained them. hclog writes must never block -- a stalled logger
// would stall the node -- so once the buffer is full the sink drops its
// oldest line to make room for the newest.
const logSinkBuffer = 8192

// logSink is the io.Writer the process logger targets in TUI mode. hclog
// writes one formatted record per Write; the sink splits those into whole
// lines and hands each to the TUI over a buffered channel, so the log stream
// pane can render them without the logger and the UI sharing any lock. Lines
// logged before the pane starts draining queue up, so the operator still sees
// the startup backlog once it appears.
type logSink struct {
	mu      sync.Mutex
	partial []byte
	ch      chan string
}

func newLogSink() *logSink {
	return &logSink{ch: make(chan string, logSinkBuffer)}
}

// Write buffers p, emitting one string per complete newline-terminated line.
// A trailing partial line is held until its newline arrives on a later Write.
func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.partial = append(s.partial, p...)
	for {
		i := bytes.IndexByte(s.partial, '\n')
		if i < 0 {
			break
		}
		line := string(bytes.TrimRight(s.partial[:i], "\r"))
		s.partial = s.partial[i+1:]
		s.push(line)
	}
	return len(p), nil
}

// push enqueues line, dropping the oldest buffered line if the channel is
// full so the newest is always retained.
func (s *logSink) push(line string) {
	select {
	case s.ch <- line:
		return
	default:
	}
	// Full: discard the oldest, then retry once. A racing reader may have
	// already made room, so both operations stay non-blocking.
	select {
	case <-s.ch:
	default:
	}
	select {
	case s.ch <- line:
	default:
	}
}

// lines is the channel the TUI reads log lines from.
func (s *logSink) lines() <-chan string { return s.ch }

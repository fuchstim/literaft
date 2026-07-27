package raftproto

import "github.com/fuchstim/literaft/internal/wal"

//go:generate buf generate

func NewLogEntryTransaction(frames []*wal.Frame) *LogEntry_Transaction {
	tx := &LogEntry_Transaction{
		Pages: make([]*LogEntry_Transaction_Page, 0, len(frames)),
	}
	for _, frame := range frames {
		tx.Pages = append(tx.Pages, &LogEntry_Transaction_Page{
			PgNo: frame.Header.PgNo(),
			Data: frame.Data,
		})

		if frame.Header.NTruncate() > 0 {
			tx.NTruncate = frame.Header.NTruncate()
		}
	}

	return tx
}

func (t *LogEntry_Transaction) Frames() []*wal.Frame {
	frames := make([]*wal.Frame, 0, len(t.Pages))
	for i, page := range t.Pages {
		header := wal.FrameHeader{}
		header.SetPgNo(page.PgNo)
		if i == len(t.Pages)-1 {
			header.SetNTruncate(t.NTruncate)
		}

		frames = append(frames, &wal.Frame{Header: &header, Data: page.Data})
	}

	return frames
}

package vfs

// Frame is one captured (page number, page image) pair from a write
// transaction's WAL frames, in the order SQLite wrote them
type Frame struct {
	Pgno uint32
	Page []byte
}

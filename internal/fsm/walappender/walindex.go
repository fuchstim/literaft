package walappender

import (
	"encoding/binary"
	"sync/atomic"
)

// Wal-index layout constants as per SQLite's wal.c. All wal-index
// fields are native byte order, not the WAL file's big-endian -- and for
// this engine "native" is always little-endian, since SQLite runs
// compiled to wasm and wasm's linear memory is little-endian by spec
// regardless of host CPU.
const (
	hdrCopySize  = 48                           // sizeof(WalIndexHdr)
	ckptInfoSize = 40                           // sizeof(WalCkptInfo)
	indexHdrSize = 2*hdrCopySize + ckptInfoSize // WALINDEX_HDR_SIZE, 136
	saltSize     = 8

	hashtableNPage    = 4096                            // HASHTABLE_NPAGE
	hashtableHash1    = 383                             // HASHTABLE_HASH_1
	hashtableNSlot    = hashtableNPage * 2              // HASHTABLE_NSLOT, 8192
	hashtableNPageOne = hashtableNPage - indexHdrSize/4 // HASHTABLE_NPAGE_ONE, 4062
)

// walIndexHeader is the in-memory form of one WalIndexHdr copy. salt is carried as
// an opaque 8-byte blob, never interpreted as a number: SQLite itself only
// ever memcpy's it between the WAL walIndexHeader and frame headers, so its byte
// order doesn't matter as long as we copy the same raw bytes everywhere.
type walIndexHeader struct {
	version     uint32
	change      uint32
	init        bool
	bigEndCksum bool
	pageSize    uint16
	maxFrame    uint32
	nPage       uint32
	frameCksum  [2]uint32
	salt        [saltSize]byte
}

func decodeWALIndexHeader(b []byte) walIndexHeader {
	_ = b[:hdrCopySize] // bounds check hint
	return walIndexHeader{
		version:     binary.LittleEndian.Uint32(b[0:4]),
		change:      binary.LittleEndian.Uint32(b[8:12]),
		init:        b[12] != 0,
		bigEndCksum: b[13] != 0,
		pageSize:    binary.LittleEndian.Uint16(b[14:16]),
		maxFrame:    binary.LittleEndian.Uint32(b[16:20]),
		nPage:       binary.LittleEndian.Uint32(b[20:24]),
		frameCksum:  [2]uint32{binary.LittleEndian.Uint32(b[24:28]), binary.LittleEndian.Uint32(b[28:32])},
		salt:        [saltSize]byte(b[32:40]),
	}
}

// encode serializes h and appends its own self-checksum (aCksum, over
// bytes 0-39, always native order regardless of bigEndCksum -- wal.c
// hardcodes nativeCksum=1 for this specific checksum).
func (h walIndexHeader) encode() [hdrCopySize]byte {
	var b [hdrCopySize]byte
	binary.LittleEndian.PutUint32(b[0:4], h.version)
	// bytes 4:8 are unused padding; left zero.
	binary.LittleEndian.PutUint32(b[8:12], h.change)
	if h.init {
		b[12] = 1
	}
	if h.bigEndCksum {
		b[13] = 1
	}
	binary.LittleEndian.PutUint16(b[14:16], h.pageSize)
	binary.LittleEndian.PutUint32(b[16:20], h.maxFrame)
	binary.LittleEndian.PutUint32(b[20:24], h.nPage)
	binary.LittleEndian.PutUint32(b[24:28], h.frameCksum[0])
	binary.LittleEndian.PutUint32(b[28:32], h.frameCksum[1])
	copy(b[32:40], h.salt[:])
	s0, s1 := checksum(0, 0, b[:40])
	binary.LittleEndian.PutUint32(b[40:44], s0)
	binary.LittleEndian.PutUint32(b[44:48], s1)
	return b
}

// readWALIndexHeader reads the wal-index header using the same tear-safe protocol
// real readers use: try copy 0, and if its self-checksum doesn't verify,
// fall back to copy 1 (wal.c:walIndexTryHdr). The second return value is
// false if neither copy is valid or initialized. A freshly zeroed region
// (a brand new -shm file) trivially satisfies the checksum -- checksum(0,
// 0, 40 zero bytes) is (0, 0), matching the stored zero "checksum" -- so
// isInit must also be checked to avoid mistaking that for a real header.
func readWALIndexHeader(region0 []byte) (walIndexHeader, bool) {
	for _, b := range [][]byte{
		region0[0:hdrCopySize],
		region0[hdrCopySize : 2*hdrCopySize],
	} {
		s0, s1 := checksum(0, 0, b[:40])
		if s0 == binary.LittleEndian.Uint32(b[40:44]) && s1 == binary.LittleEndian.Uint32(b[44:48]) {
			if h := decodeWALIndexHeader(b); h.init {
				return h, true
			}
		}
	}
	return walIndexHeader{}, false
}

// writeWALIndexHeader publishes h into region0 using SQLite's tear-safe two-copy
// protocol: write the second copy, barrier, then the first copy
// (wal.c:walIndexWriteHdr writes copy 1 before copy 0 -- readers trust
// copy 0 first and fall back to copy 1, so copy 1 must be durable before
// copy 0 is replaced).
func writeWALIndexHeader(region0 []byte, h walIndexHeader) {
	b := h.encode()
	copy(region0[hdrCopySize:2*hdrCopySize], b[:])
	barrier()
	copy(region0[0:hdrCopySize], b[:])
}

// barrier is a full memory barrier, so the copy-1 write above is visible
// to other mappers of this shared memory before the copy-0 write
// proceeds.
func barrier() {
	var b atomic.Bool
	b.Swap(true)
}

// framePage returns which 32KB wal-index page holds the hash-table entry
// for WAL frame number frame (1-based). Wal-index pages are numbered from
// 0 (wal.c:walFramePage).
func framePage(frame uint32) int {
	return int((int64(frame) + hashtableNPage - hashtableNPageOne - 1) / hashtableNPage)
}

// frameZero returns one less than the frame number of the first frame
// indexed by wal-index page `page` (wal.c:walHashGet's iZero).
func frameZero(page int) uint32 {
	if page == 0 {
		return 0
	}
	return uint32(hashtableNPageOne + (page-1)*hashtableNPage)
}

func walHash(pgno uint32) int { return int((pgno * hashtableHash1) & (hashtableNSlot - 1)) }
func walNextHash(k int) int   { return (k + 1) & (hashtableNSlot - 1) }

// hashTableOffsets returns the byte offsets of the aPgno and aHash arrays
// within wal-index page `page` (wal.c:walHashGet). Page 0's aPgno array is
// shifted past the 136-byte header that shares its region; every later
// page uses the full region for aPgno.
func hashTableOffsets(page int) (pgnoOff, hashOff int) {
	hashOff = hashtableNPage * 4 // 16384: aHash always starts here within its region
	if page == 0 {
		return indexHdrSize, hashOff
	}
	return 0, hashOff
}

// addFrameToWALIndex records, within the wal-index page for frame, that WAL
// frame `frame` holds database page number `pgno` (wal.c:walIndexAppend).
// region must be the page shm.Region(framePage(frame)) returned.
func addFrameToWALIndex(region []byte, frame, pgno uint32) {
	page := framePage(frame)
	pgnoOff, hashOff := hashTableOffsets(page)
	idx := int(frame - frameZero(page)) // 1-based position within this segment

	if idx == 1 {
		// First entry in this segment: it may hold stale data from a
		// prior WAL epoch that reused this mapped region -- wipe the
		// whole segment before adding the new entry.
		clear(region[pgnoOff:])
	}

	binary.LittleEndian.PutUint32(region[pgnoOff+(idx-1)*4:], pgno)
	for k := walHash(pgno); ; k = walNextHash(k) {
		off := hashOff + k*2
		if binary.LittleEndian.Uint16(region[off:]) == 0 {
			binary.LittleEndian.PutUint16(region[off:], uint16(idx))
			return
		}
	}
}

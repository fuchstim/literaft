package shm

import (
	"encoding/binary"
	"slices"
	"sync/atomic"

	"github.com/fuchstim/literaft/internal/wal"
)

// Wal-index layout constants, as defined by SQLite.
const (
	headerCopySize         = 48                                        // sizeof(WalIndexHdr)
	checkpointInfoSize     = 40                                        // sizeof(WalCkptInfo)
	checkpointInfoNReaders = 5                                         // WALINDEX_NREADER
	checkpointInfoOffset   = 2 * headerCopySize                        // WALINDEX_CKPTINFO_OFFSET, 96
	headerSize             = checkpointInfoOffset + checkpointInfoSize // WALINDEX_HDR_SIZE, 136

	hashtablePageCount        = 4096                              // HASHTABLE_NPAGE
	hashtableHash1            = 383                               // HASHTABLE_HASH_1
	hashtableSlotCount        = hashtablePageCount * 2            // HASHTABLE_NSLOT, 8192
	hashtableRegion0PageCount = hashtablePageCount - headerSize/4 // HASHTABLE_NPAGE_ONE, 4062

	readMarkNotUsed = 0xffffffff // READMARK_NOT_USED
)

// Try copy 0 first and verify its checksum. If its invalid, fallback to copy 1.
// If both are invalid, an uninitialized header is returned (IsInit() == false)
func readHeader(region0 []byte) Header {
	copy0 := slices.Clone(region0[0:headerCopySize])
	copy1 := slices.Clone(region0[headerCopySize : 2*headerCopySize])

	for _, c := range [][]byte{copy0, copy1} {
		checksum1, checksum2 := wal.ComputeChecksums(binary.LittleEndian, c[:40], 0, 0)
		hdr := Header(c)
		if checksum1 == hdr.Checksum1() && checksum2 == hdr.Checksum2() {
			return hdr
		}
	}

	return Header{}
}

// Write copy 1 first, then barrier, then write copy 0.
func writeHeader(hdr Header, region0 []byte) {
	hdr.UpdateChecksums()

	copy(region0[headerCopySize:2*headerCopySize], hdr[:])
	barrier()
	copy(region0[0:headerCopySize], hdr[:])
}

// literaft uses ncruces/go-sqlite3 which is built on WASM, so 'native' byte order is always little-endian.
type Header [headerCopySize]byte

func InitHeader(pageSize, lastFrameChecksum1, lastFrameChecksum2, salt1, salt2 uint32) Header {
	var h Header
	h.SetVersion(wal.WALHeaderVersion)
	h.SetChangeCounter(0)
	h.SetInit(true)
	h.SetBigEndianChecksum(false)
	h.SetPageSize(pageSize)
	h.SetMaxFrame(0)
	h.SetPageCount(0)
	h.SetLastFrameChecksum1(lastFrameChecksum1)
	h.SetLastFrameChecksum2(lastFrameChecksum2)
	h.SetSalt1(salt1)
	h.SetSalt2(salt2)

	return h
}

func (h Header) Version() uint32      { return binary.LittleEndian.Uint32(h[0:4]) }
func (h *Header) SetVersion(v uint32) { binary.LittleEndian.PutUint32(h[0:4], v) }

func (h Header) ChangeCounter() uint32      { return binary.LittleEndian.Uint32(h[8:12]) }
func (h *Header) SetChangeCounter(c uint32) { binary.LittleEndian.PutUint32(h[8:12], c) }

func (h Header) IsInit() bool { return h[12] != 0 }
func (h *Header) SetInit(init bool) {
	h[12] = 0
	if init {
		h[12] = 1
	}
}

// Only used for WAL checksums, WAL index always uses native order
func (h Header) BigEndianChecksum() bool { return h[13] != 0 }
func (h *Header) SetBigEndianChecksum(big bool) {
	h[13] = 0
	if big {
		h[13] = 1
	}
}

func (h Header) PageSize() uint32 {
	s := binary.LittleEndian.Uint16(h[14:16])
	if s == 1 {
		return 65536
	}
	return uint32(s)
}
func (h *Header) SetPageSize(size uint32) {
	if size == 65536 {
		size = 1
	}
	binary.LittleEndian.PutUint16(h[14:16], uint16(size))
}

func (h Header) MaxFrame() uint32        { return binary.LittleEndian.Uint32(h[16:20]) }
func (h *Header) SetMaxFrame(max uint32) { binary.LittleEndian.PutUint32(h[16:20], max) }

func (h Header) PageCount() uint32      { return binary.LittleEndian.Uint32(h[20:24]) }
func (h *Header) SetPageCount(n uint32) { binary.LittleEndian.PutUint32(h[20:24], n) }

func (h Header) LastFrameChecksum1() uint32      { return binary.LittleEndian.Uint32(h[24:28]) }
func (h *Header) SetLastFrameChecksum1(c uint32) { binary.LittleEndian.PutUint32(h[24:28], c) }

func (h Header) LastFrameChecksum2() uint32      { return binary.LittleEndian.Uint32(h[28:32]) }
func (h *Header) SetLastFrameChecksum2(c uint32) { binary.LittleEndian.PutUint32(h[28:32], c) }

func (h Header) Salt1() uint32     { return binary.LittleEndian.Uint32(h[32:36]) }
func (h Header) SetSalt1(s uint32) { binary.LittleEndian.PutUint32(h[32:36], s) }

func (h Header) Salt2() uint32      { return binary.LittleEndian.Uint32(h[36:40]) }
func (h *Header) SetSalt2(s uint32) { binary.LittleEndian.PutUint32(h[36:40], s) }

// WAL index checksums always use native order, regardless of the bigEndCksum flag.
func (h Header) Checksum1() uint32     { return binary.LittleEndian.Uint32(h[40:44]) }
func (h Header) SetChecksum1(c uint32) { binary.LittleEndian.PutUint32(h[40:44], c) }

func (h Header) Checksum2() uint32      { return binary.LittleEndian.Uint32(h[44:48]) }
func (h *Header) SetChecksum2(c uint32) { binary.LittleEndian.PutUint32(h[44:48], c) }

func (h *Header) UpdateChecksums() {
	checksum1, checksum2 := wal.ComputeChecksums(binary.LittleEndian, h[:40], 0, 0)

	h.SetChecksum1(checksum1)
	h.SetChecksum2(checksum2)
}

func readCheckpointInfo(region0 []byte) CheckpointInfo {
	return CheckpointInfo(slices.Clone(region0[checkpointInfoOffset : checkpointInfoOffset+checkpointInfoSize]))
}

func writeCheckpointInfo(info CheckpointInfo, region0 []byte) {
	copy(region0[checkpointInfoOffset:checkpointInfoOffset+checkpointInfoSize], info[:])
}

type CheckpointInfo [checkpointInfoSize]byte

func (c CheckpointInfo) ResetForRewind() {
	c.SetNBackfill(0)
	c.SetReadMark(1, 0)
	for i := 2; i < checkpointInfoNReaders; i++ {
		c.SetReadMark(uint8(i), readMarkNotUsed)
	}
	c.SetNBackfillAttempted(0)
}

func (c CheckpointInfo) NBackfill() uint32      { return binary.LittleEndian.Uint32(c[0:4]) }
func (c *CheckpointInfo) SetNBackfill(n uint32) { binary.LittleEndian.PutUint32(c[0:4], n) }

func (c CheckpointInfo) ReadMark(i uint8) uint32 {
	if i >= checkpointInfoNReaders {
		panic("reader index out of range")
	}

	return binary.LittleEndian.Uint32(c[4+i*4 : 8+i*4])
}
func (c *CheckpointInfo) SetReadMark(i uint8, mark uint32) {
	if i >= checkpointInfoNReaders {
		panic("reader index out of range")
	}

	binary.LittleEndian.PutUint32(c[4+i*4:8+i*4], mark)
}

func (c CheckpointInfo) NBackfillAttempted() uint32 {
	return binary.LittleEndian.Uint32(c[32:36])
}
func (c *CheckpointInfo) SetNBackfillAttempted(n uint32) {
	binary.LittleEndian.PutUint32(c[32:36], n)
}

func barrier() {
	var b atomic.Bool
	b.Swap(true)
}

// regionForFrame returns which wal-index region holds the hash-table entry
// for WAL frame number frameIdx (1-based). Wal-index regions are 0-indexed.
func regionForFrame(frameIdx uint32) int {
	return int((int64(frameIdx) + hashtablePageCount - hashtableRegion0PageCount - 1) / hashtablePageCount)
}

// frameZeroForRegion returns one less than the frame number of the first frame
// indexed by wal-index region `regionID`.
func frameZeroForRegion(regionID int) uint32 {
	if regionID == 0 {
		return 0
	}
	return uint32(hashtableRegion0PageCount + (regionID-1)*hashtablePageCount)
}

// hashSlotForPage returns the slot number in the wal-index hash table for database page number pgNo
func hashSlotForPage(pgNo uint32) int {
	return int((pgNo * hashtableHash1) & (hashtableSlotCount - 1))
}

// nextHashSlot returns the next slot number in the wal-index hash table after prevHash, wrapping around to 0 if necessary.
func nextHashSlot(curSlot int) int {
	return (curSlot + 1) & (hashtableSlotCount - 1)
}

// hashTableOffsets returns the byte offsets of the aPgno and aHash arrays
// within wal-index region `regionID`. Region 0's aPgno array is shifted past the
// 136-byte header that shares its region; every later page uses the full
// region for aPgno.
func hashTableOffsets(regionID int) (pgNoOff, hashOff int) {
	hashOff = hashtablePageCount * 4 // 16384: aHash always starts here within its region
	if regionID == 0 {
		return headerSize, hashOff
	}
	return 0, hashOff
}

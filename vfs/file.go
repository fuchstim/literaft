package vfs

import (
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

// File wraps a base sqlite3vfs.File, tagging it with its FileType.
//
// Required File methods (Close/ReadAt/WriteAt/Truncate/Sync/Size/Lock/
// Unlock/CheckReservedLock/SectorSize/DeviceCharacteristics) are promoted
// unchanged from the embedded base. The optional capability interfaces
// don't promote through an embedded interface, so each is forwarded
// explicitly below via vfsutil, mirroring the wrapping pattern used by
// ncruces' own vfs/adiantum and vfs/xts.
type File struct {
	sqlite3vfs.File
	kind FileType
}

func wrapFile(base sqlite3vfs.File, kind FileType) *File {
	return &File{File: base, kind: kind}
}

// Kind reports which SQLite file this wraps.
func (f *File) Kind() FileType { return f.kind }

var (
	_ sqlite3vfs.File                   = (*File)(nil)
	_ sqlite3vfs.FileUnwrap             = (*File)(nil)
	_ sqlite3vfs.FileLockState          = (*File)(nil)
	_ sqlite3vfs.FilePersistWAL         = (*File)(nil)
	_ sqlite3vfs.FilePowersafeOverwrite = (*File)(nil)
	_ sqlite3vfs.FileChunkSize          = (*File)(nil)
	_ sqlite3vfs.FileSizeHint           = (*File)(nil)
	_ sqlite3vfs.FileHasMoved           = (*File)(nil)
	_ sqlite3vfs.FileOverwrite          = (*File)(nil)
	_ sqlite3vfs.FileSync               = (*File)(nil)
	_ sqlite3vfs.FileCommitPhaseTwo     = (*File)(nil)
	_ sqlite3vfs.FileBatchAtomicWrite   = (*File)(nil)
	_ sqlite3vfs.FileCheckpoint         = (*File)(nil)
	_ sqlite3vfs.FilePragma             = (*File)(nil)
	_ sqlite3vfs.FileBusyHandler        = (*File)(nil)
	_ sqlite3vfs.FileSharedMemory       = (*File)(nil)
)

func (f *File) Unwrap() sqlite3vfs.File { return f.File }

func (f *File) LockState() sqlite3vfs.LockLevel {
	return vfsutil.WrapLockState(f.File)
}

func (f *File) PersistWAL() bool { return vfsutil.WrapPersistWAL(f.File) }
func (f *File) SetPersistWAL(keepWAL bool) {
	vfsutil.WrapSetPersistWAL(f.File, keepWAL)
}

func (f *File) PowersafeOverwrite() bool { return vfsutil.WrapPowersafeOverwrite(f.File) }
func (f *File) SetPowersafeOverwrite(psow bool) {
	vfsutil.WrapSetPowersafeOverwrite(f.File, psow)
}

func (f *File) ChunkSize(size int) { vfsutil.WrapChunkSize(f.File, size) }

func (f *File) SizeHint(size int64) error { return vfsutil.WrapSizeHint(f.File, size) }

func (f *File) HasMoved() (bool, error) { return vfsutil.WrapHasMoved(f.File) }

func (f *File) Overwrite() error { return vfsutil.WrapOverwrite(f.File) }

func (f *File) SyncSuper(super string) error { return vfsutil.WrapSyncSuper(f.File, super) }

func (f *File) CommitPhaseTwo() error { return vfsutil.WrapCommitPhaseTwo(f.File) }

func (f *File) BeginAtomicWrite() error    { return vfsutil.WrapBeginAtomicWrite(f.File) }
func (f *File) CommitAtomicWrite() error   { return vfsutil.WrapCommitAtomicWrite(f.File) }
func (f *File) RollbackAtomicWrite() error { return vfsutil.WrapRollbackAtomicWrite(f.File) }

func (f *File) CheckpointStart() { vfsutil.WrapCheckpointStart(f.File) }
func (f *File) CheckpointDone()  { vfsutil.WrapCheckpointDone(f.File) }

func (f *File) Pragma(name, value string) (string, error) {
	return vfsutil.WrapPragma(f.File, name, value)
}

func (f *File) BusyHandler(handler func() bool) { vfsutil.WrapBusyHandler(f.File, handler) }

func (f *File) SharedMemory() sqlite3vfs.SharedMemory {
	return vfsutil.WrapSharedMemory(f.File)
}

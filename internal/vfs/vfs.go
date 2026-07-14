// Package vfs implements the RAFT-backed SQLite VFS.
//
// It wraps ncruces' default VFS with a pass-through layer that changes
// nothing observable: every VFS and File method delegates to the wrapped
// implementation, including the optional capability interfaces
// (FileSharedMemory, FileLockState, FileCheckpoint, FileUnwrap, ...) so WAL
// mode, shared memory, and external-reader compatibility keep working
// unmodified. Open tags each file by type (database, WAL, journal) so
// commit-frame interception can be added on exactly the WAL write path.
package vfs

import (
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

// Register wraps base with the provided gate and registers it
// under name, so it can be selected with "?vfs=<name>" in a SQLite URI DSN.
//
// pageSize must be the real, cluster-wide fixed SQLite page size -- not
// just an optional enforcement knob a caller can skip by passing 0. It's
// also used unconditionally to compute frame-header offsets, so a wrong or
// zero value silently corrupts frame parsing rather than merely disabling
// a check. Register panics if pageSize is 0.
//
// logger must be non-nil; pass hclog.NewNullLogger() to disable output.
func Register(name string, base sqlite3vfs.VFS, gate Gate, pageSize uint32, logger hclog.Logger) {
	if pageSize == 0 {
		panic("vfs: pageSize must be the real, non-zero cluster page size")
	}
	sqlite3vfs.Register(name, &VFS{base, gate, pageSize, logger})
}

// VFS wraps a base sqlite3vfs.VFS. See package doc.
type VFS struct {
	base     sqlite3vfs.VFS
	gate     Gate
	pageSize uint32
	logger   hclog.Logger
}

var (
	_ sqlite3vfs.VFS         = (*VFS)(nil)
	_ sqlite3vfs.VFSFilename = (*VFS)(nil)
)

// Open implements sqlite3vfs.VFS.
func (v *VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, flags, err := vfsutil.WrapOpen(v.base, name, flags)
	if err != nil {
		return nil, flags, err
	}
	return wrapFile(file, fileType(flags), v.gate, v.pageSize, v.logger), flags, nil
}

// OpenFilename implements sqlite3vfs.VFSFilename.
func (v *VFS) OpenFilename(name *sqlite3vfs.Filename, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, flags, err := vfsutil.WrapOpenFilename(v.base, name, flags)
	if err != nil {
		return nil, flags, err
	}
	return wrapFile(file, fileType(flags), v.gate, v.pageSize, v.logger), flags, nil
}

// Delete implements sqlite3vfs.VFS.
func (v *VFS) Delete(name string, syncDir bool) error {
	return v.base.Delete(name, syncDir)
}

// Access implements sqlite3vfs.VFS.
func (v *VFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	return v.base.Access(name, flags)
}

// FullPathname implements sqlite3vfs.VFS.
func (v *VFS) FullPathname(name string) (string, error) {
	return v.base.FullPathname(name)
}

// FileType identifies which SQLite file a File wraps.
type FileType uint8

const (
	// FileTypeOther covers files not individually distinguished below:
	// temp files, temp journals, super journals, etc.
	FileTypeOther FileType = iota
	FileTypeDatabase
	FileTypeJournal
	FileTypeWAL
)

func (t FileType) String() string {
	switch t {
	case FileTypeDatabase:
		return "database"
	case FileTypeJournal:
		return "journal"
	case FileTypeWAL:
		return "wal"
	default:
		return "other"
	}
}

func fileType(flags sqlite3vfs.OpenFlag) FileType {
	switch {
	case flags&sqlite3vfs.OPEN_WAL != 0:
		return FileTypeWAL
	case flags&sqlite3vfs.OPEN_MAIN_DB != 0:
		return FileTypeDatabase
	case flags&(sqlite3vfs.OPEN_MAIN_JOURNAL|sqlite3vfs.OPEN_SUPER_JOURNAL) != 0:
		return FileTypeJournal
	default:
		return FileTypeOther
	}
}

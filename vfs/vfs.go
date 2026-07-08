// Package vfs implements the RAFT-backed SQLite VFS.
//
// It wraps ncruces' default (pure-Go) VFS with a pass-through layer that
// changes nothing observable: every VFS and File method delegates to the
// wrapped implementation, including the optional capability interfaces
// (FileSharedMemory, FileLockState, FileCheckpoint, FileUnwrap, ...) so WAL
// mode, shared memory, and external-reader compatibility keep working
// unmodified. Open tags each file by type (database, WAL, journal) so later
// milestones can add commit-frame interception on exactly the WAL write path
// (see docs/DESIGN.md, docs/ROADMAP.md M0-M2).
package vfs

import (
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

// Name is the name this package registers its default-wrapping VFS under.
const Name = "literaft"

func init() {
	Register(Name, sqlite3vfs.Find(""))
}

// Register wraps base with the M2 stub gate (AlwaysCommit) and registers it
// under name, so it can be selected with "?vfs=<name>" in a SQLite URI DSN.
func Register(name string, base sqlite3vfs.VFS) {
	RegisterGate(name, base, AlwaysCommit)
}

// RegisterGate is like Register but lets the caller supply the gate that
// decides whether each write transaction's commit frame may be published.
// RAFT integration (M4) will supply the real one; tests use it to exercise
// the abort branch.
func RegisterGate(name string, base sqlite3vfs.VFS, gate Gate) {
	sqlite3vfs.Register(name, WrapGate(base, gate))
}

// Wrap wraps base with the M2 stub gate (AlwaysCommit). Every other
// operation is delegated to base unchanged.
func Wrap(base sqlite3vfs.VFS) sqlite3vfs.VFS {
	return WrapGate(base, AlwaysCommit)
}

// WrapGate wraps base to tag opened files by FileType and gate WAL commit
// frames through gate. Every other operation is delegated to base
// unchanged.
func WrapGate(base sqlite3vfs.VFS, gate Gate) sqlite3vfs.VFS {
	return &VFS{base: base, gate: gate}
}

// VFS wraps a base sqlite3vfs.VFS. See package doc.
type VFS struct {
	base sqlite3vfs.VFS
	gate Gate
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
	return wrapFile(file, fileType(flags), v.gate), flags, nil
}

// OpenFilename implements sqlite3vfs.VFSFilename.
func (v *VFS) OpenFilename(name *sqlite3vfs.Filename, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, flags, err := vfsutil.WrapOpenFilename(v.base, name, flags)
	if err != nil {
		return nil, flags, err
	}
	return wrapFile(file, fileType(flags), v.gate), flags, nil
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

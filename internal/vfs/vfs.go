package vfs

import (
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3/util/vfsutil"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"
)

func Register(name string, base sqlite3vfs.VFS, gate Gate, logger hclog.Logger) {
	sqlite3vfs.Register(name, &VFS{base, gate, logger})
}

type VFS struct {
	sqlite3vfs.VFS

	gate   Gate
	logger hclog.Logger
}

var _ sqlite3vfs.VFS = (*VFS)(nil)
var _ sqlite3vfs.VFSFilename = (*VFS)(nil)

func (v *VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, flags, err := vfsutil.WrapOpen(v.VFS, name, flags)
	if err != nil {
		return nil, flags, err
	}

	// Only wrap WAL files
	if !isWALFile(flags) {
		return file, flags, nil
	}

	wf, err := newGatedWALFile(file, v.gate, v.logger)
	if err != nil {
		return nil, flags, err
	}

	return wf, flags, nil
}

func (v *VFS) OpenFilename(name *sqlite3vfs.Filename, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, flags, err := vfsutil.WrapOpenFilename(v.VFS, name, flags)
	if err != nil {
		return nil, flags, err
	}

	// Only wrap WAL file
	if !isWALFile(flags) {
		return file, flags, nil
	}

	wf, err := newGatedWALFile(file, v.gate, v.logger)
	if err != nil {
		return nil, flags, err
	}

	return wf, flags, nil
}

func isWALFile(flags sqlite3vfs.OpenFlag) bool {
	return flags&sqlite3vfs.OPEN_WAL != 0
}

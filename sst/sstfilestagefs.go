// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"io"
	"strings"

	fs "github.com/relab/wrfs"
)

type stageSstFS interface {
	fs.FS
	StageFileSystem()
}

type stageDirFS struct{ fs.FS }

// Static interface check
// To check if stageDirFS is implemented fs.FS, fs.OpenFileFS and StageSstFS interface.
var (
	_ fs.FS         = (*stageDirFS)(nil)
	_ fs.OpenFileFS = (*stageDirFS)(nil)
	_ stageSstFS    = (*stageDirFS)(nil)
)

func (f stageDirFS) Open(name string) (fs.File, error) {
	if name == "." {
		d, err := f.FS.Open(name)
		if err != nil {
			return nil, err
		}
		return stageDirFile{d}, nil
	}
	return f.FS.Open(name + ".sst")
}

func (f stageDirFS) Stat(name string) (fs.FileInfo, error) {
	if name != "." {
		name += ".sst"
	}
	return fs.Stat(f.FS, name)
}

func (f stageDirFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return fs.OpenFile(f.FS, name+".sst", flag, perm)
}

func (f stageDirFS) StageFileSystem() {}

type stageDirFile struct{ fs.File }

var (
	_ fs.File        = (*stageDirFile)(nil)
	_ io.Reader      = (*stageDirFile)(nil)
	_ fs.ReadDirFile = (*stageDirFile)(nil)
)

func (f stageDirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	dirEntries, err := f.File.(fs.ReadDirFile).ReadDir(n)
	if err != nil {
		return nil, err
	}
	if dirEntries != nil {
		stageDirEntries := make([]fs.DirEntry, 0, len(dirEntries))
		for _, e := range dirEntries {
			stageDirEntries = append(stageDirEntries, stageDirEntry{e})
		}
		return stageDirEntries, nil
	}
	return dirEntries, nil
}

type stageDirEntry struct{ fs.DirEntry }

var _ fs.DirEntry = (*stageDirEntry)(nil)

func (d stageDirEntry) Name() string {
	return strings.TrimSuffix(d.DirEntry.Name(), ".sst")
}

func (d stageDirEntry) Info() (fs.FileInfo, error) {
	i, err := d.DirEntry.Info()
	if err != nil {
		return nil, err
	}
	return stageFileInfo{i}, nil
}

type stageFileInfo struct{ fs.FileInfo }

var _ fs.FileInfo = (*stageFileInfo)(nil)

func (d stageFileInfo) Name() string {
	return strings.TrimSuffix(d.FileInfo.Name(), ".sst")
}

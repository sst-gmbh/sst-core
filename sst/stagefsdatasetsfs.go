// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"go.etcd.io/bbolt"
)

var (
	errRepositoryDirectoryNotFound = errors.New("repository directory not found")
	errDatasetNotFoundInRepository = errors.New("dataset not found in repository")
	errCannotReadRepositoryFile    = errors.New("can not read repository file")
	ErrIsDir                       = errors.New("is a directory")
)

type datasetsFS struct {
	datasets *bbolt.Bucket
}

var (
	_ fs.FS        = (*datasetsFS)(nil)
	_ fs.ReadDirFS = (*datasetsFS)(nil)
	_ stageSstFS   = (*datasetsFS)(nil)
)

func datasetsFSOf(datasets *bbolt.Bucket) datasetsFS {
	return datasetsFS{datasets: datasets}
}

func (f datasetsFS) Open(name string) (fs.File, error) {
	if name == "." {
		return namedDirFile{name: name}, nil
	}
	if f.datasets == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errDatasetNotFoundInRepository}
	}
	nameKey, err := uuid.Parse(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	actualKey, _ := f.datasets.Cursor().Seek(nameKey[:])
	if !bytes.Equal(actualKey, nameKey[:]) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errDatasetNotFoundInRepository}
	}
	return repositoryFile{keyedFile: keyedFile{nameKey}}, nil
}

func (f datasetsFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "read dir", Path: name, Err: errRepositoryDirectoryNotFound}
	}
	var dirEntries []fs.DirEntry
	c := f.datasets.Cursor()
	for k, h := c.Seek(uuid.Nil[:]); k != nil && len(k) == len(uuid.UUID{}); k, h = c.Next() {
		nameKey, err := uuid.FromBytes(k)
		if err != nil {
			return nil, &fs.PathError{Op: "read dir", Path: name, Err: err}
		}
		dirEntries = append(dirEntries, dirEntry{nameKey: nameKey, size: len(h)})
	}
	return dirEntries, nil
}

func (f datasetsFS) StageFileSystem() {}

type repositoryFile struct {
	keyedFile
}

var (
	_ fs.File       = (*repositoryFile)(nil)
	_ io.ReadCloser = (*repositoryFile)(nil)
	_ fs.FileInfo   = (*repositoryFile)(nil)
)

func (f repositoryFile) Stat() (fs.FileInfo, error) { return f, nil }
func (f repositoryFile) Size() int64                { return int64(0) }
func (f repositoryFile) Read([]byte) (int, error)   { return 0, errCannotReadRepositoryFile }

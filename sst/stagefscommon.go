// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// Package stagefs allows to use file system abstraction for Stage writing/reading.
package sst

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
)

var (
	errDatasetDirectoryNotFound           = errors.New("dataset directory not found")
	errNamedGraphNotFoundInDataset        = errors.New("named graph not found in dataset")
	errNamedGraphRevisionNotFound         = errors.New("NamedGraphRevision not found")
	errOpenFileMissedTruncFlagUnsupported = errors.New("open file missed os.O_TRUNC flag is unsupported")
	ErrDatasetHasBeenDeleted              = errors.New("Dataset has been deleted")
	errIsDir                              = errors.New("is a directory")
)

type dirEntry struct {
	nameKey uuid.UUID
	size    int
}

var (
	_ fs.DirEntry = (*dirEntry)(nil)
	_ fs.FileInfo = (*dirEntry)(nil)
)

func (e dirEntry) Name() string               { return e.nameKey.String() }
func (e dirEntry) IsDir() bool                { return false }
func (e dirEntry) Type() fs.FileMode          { return 0o444 }
func (e dirEntry) Info() (fs.FileInfo, error) { return e, nil }
func (e dirEntry) Size() int64                { return int64(e.size) }
func (e dirEntry) Mode() fs.FileMode          { return 0o444 }
func (e dirEntry) ModTime() time.Time         { return time.Time{} }
func (e dirEntry) Sys() interface{}           { return nil }

type (
	byteReader = bytes.Reader
	byteBuffer = bytes.Buffer
)

type keyedFile struct {
	nameKey uuid.UUID
}

func (f keyedFile) Close() error       { return nil }
func (f keyedFile) Name() string       { return f.nameKey.String() }
func (f keyedFile) Mode() fs.FileMode  { return 0o444 }
func (f keyedFile) ModTime() time.Time { return time.Time{} }
func (f keyedFile) IsDir() bool        { return false }
func (f keyedFile) Sys() interface{}   { return nil }

type byteFileReader struct {
	keyedFile
	*byteReader
}

var (
	_ fs.File       = (*byteFileReader)(nil)
	_ io.ReadCloser = (*byteFileReader)(nil)
	_ io.Seeker     = (*byteFileReader)(nil)
	_ fs.FileInfo   = (*byteFileReader)(nil)
)

func (f byteFileReader) Stat() (fs.FileInfo, error) { return f, nil }
func (f byteFileReader) Size() int64                { return f.byteReader.Size() }

type namedDirFile struct {
	name string
}

var (
	_ fs.File     = (*namedDirFile)(nil)
	_ fs.FileInfo = (*namedDirFile)(nil)
)

func (f namedDirFile) Close() error               { return nil }
func (f namedDirFile) Read([]byte) (int, error)   { return 0, errIsDir }
func (f namedDirFile) Stat() (fs.FileInfo, error) { return f, nil }
func (f namedDirFile) Name() string               { return f.name }
func (f namedDirFile) Mode() fs.FileMode          { return 0o777 }
func (f namedDirFile) ModTime() time.Time         { return time.Time{} }
func (f namedDirFile) IsDir() bool                { return true }
func (f namedDirFile) Sys() interface{}           { return nil }
func (f namedDirFile) Size() int64                { return int64(0) }

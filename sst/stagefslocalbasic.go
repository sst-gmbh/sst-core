// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"go.etcd.io/bbolt"
)

var (
	errPathNotFound         = errors.New("path not found")
	errFileNotFoundInBucket = errors.New("file not found in bucket")
)

type localBasicFS struct {
	bucket *bbolt.Bucket
}

var (
	_ fs.FS         = (*localBasicFS)(nil)
	_ fs.ReadDirFS  = (*localBasicFS)(nil)
	_ fs.OpenFileFS = (*localBasicFS)(nil)
	_ stageSstFS    = (*localBasicFS)(nil)
)

func localBasicFsOf(bucket *bbolt.Bucket) localBasicFS {
	return localBasicFS{bucket: bucket}
}

func (f localBasicFS) Open(name string) (fs.File, error) {
	if name == "." {
		return namedDirFile{name: name}, nil
	}
	nameKey, err := uuid.Parse(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	actualKey, value := f.bucket.Cursor().Seek(nameKey[:])
	if !bytes.Equal(actualKey, nameKey[:]) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errFileNotFoundInBucket}
	}
	return repositoryBucketFileReader{keyedFile: keyedFile{nameKey}, byteReader: bytes.NewReader(value)}, nil
}

func (f localBasicFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: errPathNotFound}
	}
	var dirEntries []fs.DirEntry
	err := f.bucket.ForEach(func(k, v []byte) error {
		nameKey, err := uuid.FromBytes(k)
		if err != nil {
			return err
		}
		dirEntries = append(dirEntries, dirEntry{nameKey: nameKey, size: len(v)})
		return nil
	})
	if err != nil {
		return nil, &fs.PathError{Op: "read dir", Path: name, Err: err}
	}
	return dirEntries, nil
}

func (f localBasicFS) OpenFile(name string, flag int, _ fs.FileMode) (fs.File, error) {
	nameKey, err := uuid.Parse(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open file", Path: name, Err: err}
	}
	var value []byte
	if flag&os.O_TRUNC == 0 {
		value = f.bucket.Get(nameKey[:])
	}
	buffer := bytes.NewBuffer(value)
	if flag&os.O_APPEND != 0 {
		_ = buffer.Next(len(value))
	}
	return repositoryBucketFileReadWriter{
		keyedFile:  keyedFile{nameKey},
		byteBuffer: buffer,
		bucket:     f.bucket,
	}, nil
}

func (f localBasicFS) StageFileSystem() {}

type repositoryBucketFileReader struct {
	keyedFile
	*byteReader
}

var (
	_ fs.File       = (*repositoryBucketFileReader)(nil)
	_ io.ReadCloser = (*repositoryBucketFileReader)(nil)
	_ io.Seeker     = (*repositoryBucketFileReader)(nil)
	_ fs.FileInfo   = (*repositoryBucketFileReader)(nil)
)

func (f repositoryBucketFileReader) Stat() (fs.FileInfo, error) { return f, nil }
func (f repositoryBucketFileReader) Size() int64                { return int64(f.Len()) }

type repositoryBucketFileReadWriter struct {
	keyedFile
	*byteBuffer
	bucket *bbolt.Bucket
}

var (
	_ fs.File        = (*repositoryBucketFileReadWriter)(nil)
	_ io.ReadCloser  = (*repositoryBucketFileReadWriter)(nil)
	_ io.WriteCloser = (*repositoryBucketFileReadWriter)(nil)
	_ fs.FileInfo    = (*repositoryBucketFileReadWriter)(nil)
)

func (f repositoryBucketFileReadWriter) Stat() (fs.FileInfo, error) { return f, nil }
func (f repositoryBucketFileReadWriter) Size() int64                { return int64(f.Len()) }

func (f repositoryBucketFileReadWriter) Close() error {
	return f.bucket.Put(f.nameKey[:], f.Bytes())
}

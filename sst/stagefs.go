// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"go.etcd.io/bbolt"
)

var (
	errStageDirectoryNotFound    = errors.New("stage directory not found")
	errNamedGraphNotFoundInStage = errors.New("named graph not found in stage")
	// errNamedGraphNotFoundByHash  = errors.New("named graph not found by hash")
)

// stageFS conforms (WIP - Phase1) to the specification bellow.
//
//	### Phase1
//	- bucket "NamedGraphs"->*ng* contains versioned NamedGraphs
//	  * key is the SHA key of the corresponding standalone binary NG-SST file
//	  * value is binary NG-SST file
//
//	    _Note:_ The "binary NG-SST file" should be consistent in itself, e.g. in another RCS or standalone
//
//	- bucket "Stages"->*s* contains a pool of exploded stages. This is a temporary solution.
//	  * key is a composite value _NS-uuid_:_base_commit-SHA_:_-dirty-sequence-number_
//	  * value is sub-bucket with the following structure:
//	    - key is _NG-uuid_
//	    - value is versioned _NG-SHA_
type stageFS struct {
	stage, namedGraphs *bbolt.Bucket
}

var (
	_ fs.FS         = (*stageFS)(nil)
	_ fs.ReadDirFS  = (*stageFS)(nil)
	_ fs.OpenFileFS = (*stageFS)(nil)
	_ stageSstFS    = (*stageFS)(nil)
)

func (f stageFS) Open(name string) (fs.File, error) {
	if name == "." {
		return namedDirFile{name: name}, nil
	}
	nameKey, err := uuid.Parse(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	actualKey, hash := f.stage.Cursor().Seek(nameKey[:])
	if !bytes.Equal(actualKey, nameKey[:]) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errNamedGraphNotFoundInStage}
	}
	content, err := f.namedGraphByHash(hash)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return stageFileReader{keyedFile: keyedFile{nameKey}, byteReader: bytes.NewReader(content)}, nil
}

func (f stageFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "read dir", Path: name, Err: errStageDirectoryNotFound}
	}
	var dirEntries []fs.DirEntry
	err := f.stage.ForEach(func(k, h []byte) error {
		nameKey, err := uuid.FromBytes(k)
		if err != nil {
			return err
		}
		content, err := f.namedGraphByHash(h)
		if err != nil {
			return &fs.PathError{Op: "read dir", Path: nameKey.String(), Err: err}
		}
		dirEntries = append(dirEntries, dirEntry{nameKey: nameKey, size: len(content)})
		return nil
	})
	if err != nil {
		return nil, &fs.PathError{Op: "read dir", Path: name, Err: err}
	}
	return dirEntries, nil
}

func (f stageFS) namedGraphByHash(hash []byte) ([]byte, error) {
	actualHash, content := f.namedGraphs.Cursor().Seek(hash)
	if !bytes.Equal(actualHash, hash) {
		return nil, errNamedGraphRevisionNotFound
	}
	return content, nil
}

func (f stageFS) OpenFile(name string, flag int, _ fs.FileMode) (fs.File, error) {
	nameKey, err := uuid.Parse(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open file", Path: name, Err: err}
	}
	var content []byte
	if flag&os.O_TRUNC == 0 {
		hash := f.stage.Get(nameKey[:])
		content, err = f.namedGraphByHash(hash)
		if err != nil {
			return nil, &fs.PathError{Op: "open file", Path: name, Err: err}
		}
	}
	buffer := bytes.NewBuffer(content)
	if flag&os.O_APPEND != 0 {
		_ = buffer.Next(len(content))
	}
	return stageFileReadWriter{
		keyedFile:   keyedFile{nameKey},
		byteBuffer:  buffer,
		stage:       f.stage,
		namedGraphs: f.namedGraphs,
	}, nil
}

func (f stageFS) StageFileSystem() {}

type stageFileReader struct {
	keyedFile
	*byteReader
}

var (
	_ fs.File       = (*stageFileReader)(nil)
	_ io.ReadCloser = (*stageFileReader)(nil)
	_ io.Seeker     = (*stageFileReader)(nil)
	_ fs.FileInfo   = (*stageFileReader)(nil)
)

func (f stageFileReader) Stat() (fs.FileInfo, error) { return f, nil }
func (f stageFileReader) Size() int64                { return int64(f.byteReader.Len()) }

type stageFileReadWriter struct {
	keyedFile
	*byteBuffer
	stage, namedGraphs *bbolt.Bucket
}

var (
	_ fs.File        = (*stageFileReadWriter)(nil)
	_ io.ReadCloser  = (*stageFileReadWriter)(nil)
	_ io.WriteCloser = (*stageFileReadWriter)(nil)
	_ fs.FileInfo    = (*stageFileReadWriter)(nil)
)

func (f stageFileReadWriter) Stat() (fs.FileInfo, error) { return f, nil }
func (f stageFileReadWriter) Size() int64                { return int64(f.byteBuffer.Len()) }

func (f stageFileReadWriter) Close() error {
	content := f.byteBuffer.Bytes()
	hash := sha256.Sum256(content)
	err := f.stage.Put(f.nameKey[:], hash[:])
	if err != nil {
		return err
	}
	actualHash, _ := f.namedGraphs.Cursor().Seek(hash[:])
	if !bytes.Equal(actualHash, hash[:]) {
		return f.namedGraphs.Put(hash[:], content)
	}
	return nil
}

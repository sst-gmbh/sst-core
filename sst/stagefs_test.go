// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var (
	_keyNamedGraphs = []byte("ng")
	_keyStages      = []byte("s")
	_emptyRevision  = [sha256.Size]byte{}
)

func Test_stageFS_Open(t *testing.T) {
	testDB := func(t testing.TB) *bbolt.DB {
		db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
		assert.NoError(t, err)
		return db
	}
	type args struct {
		name string
	}
	tests := []struct {
		name               string
		dbCreator          func(t testing.TB) *bbolt.DB
		transactionCreator func(db *bbolt.DB, fn func(*bbolt.Tx) error) error
		repositoryCreator  func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket)
		args               args
		wantAssertion      func(t testing.TB, got fs.File)
		assertion          assert.ErrorAssertionFunc
	}{
		{
			name:               "just_created_bucket_file",
			dbCreator:          testDB,
			transactionCreator: (*bbolt.DB).Update,
			repositoryCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				stageBucket := createTestStage(t, tx, nameKey)
				namedGraphBucket, err := tx.CreateBucket(_keyNamedGraphs)
				assert.NoError(t, err)
				writeContentFunction(t, stageBucket, namedGraphBucket, nameKey, []byte{0, 1, 2})
				return stageBucket, namedGraphBucket
			},
			args: args{
				name: "e5d83a0b-a7a8-4518-9795-1c28c71b8d16",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				val, err := io.ReadAll(got.(io.Reader))
				assert.NoError(t, err)
				assert.Equal(t, []byte{0, 1, 2}, val)
			},
			assertion: assert.NoError,
		},
		{
			name: "existing_bucket_file",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db := testDB(t)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
					stageBucket := createTestStage(t, tx, nameKey)
					namedGraphBucket, err := tx.CreateBucket(_keyNamedGraphs)
					assert.NoError(t, err)
					writeContentFunction(t, stageBucket, namedGraphBucket, nameKey, []byte("existing"))
					return nil
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			repositoryCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				stageBucket := testStage(t, tx, nameKey)
				namedGraphBucket := tx.Bucket(_keyNamedGraphs)
				return stageBucket, namedGraphBucket
			},
			args: args{
				name: "e5d83a0b-a7a8-4518-9795-1c28c71b8d16",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				val, err := io.ReadAll(got.(io.Reader))
				assert.NoError(t, err)
				assert.Equal(t, []byte("existing"), val)
			},
			assertion: assert.NoError,
		},
		{
			name:               "missing_bucket_file",
			dbCreator:          testDB,
			transactionCreator: (*bbolt.DB).Update,
			repositoryCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				stageBucket := createTestStage(t, tx, nameKey)
				namedGraphBucket, err := tx.CreateBucket(_keyNamedGraphs)
				assert.NoError(t, err)
				writeContentFunction(t, stageBucket, namedGraphBucket, nameKey, []byte{1})
				return stageBucket, namedGraphBucket
			},
			args: args{
				name: "3206ea3f-13ed-419a-b25c-4bc13e0a3968",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				assert.Nil(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errNamedGraphNotFoundInStage)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, tt.transactionCreator(db, func(tx *bbolt.Tx) error {
				stageBucket, namedGraphBucket := tt.repositoryCreator(t, tx)
				f := stageFS{
					stage:       stageBucket,
					namedGraphs: namedGraphBucket,
				}
				got, err := f.Open(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func Test_stageFS_ReadDir(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name               string
		dbCreator          func(t testing.TB) *bbolt.DB
		transactionCreator func(db *bbolt.DB, fn func(*bbolt.Tx) error) error
		bucketCreator      func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket)
		args               args
		wantAssertion      func(t testing.TB, got []fs.DirEntry)
		assertion          assert.ErrorAssertionFunc
	}{
		{
			name: "two_files",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
					stageBucket := createTestStage(t, tx, nameKey)
					namedGraphBucket, err := tx.CreateBucket(_keyNamedGraphs)
					assert.NoError(t, err)
					writeContentFunction(t, stageBucket, namedGraphBucket, nameKey, []byte("existing"))
					nameKey = uuid.MustParse("3206ea3f-13ed-419a-b25c-4bc13e0a3968")
					writeContentFunction(t, stageBucket, namedGraphBucket, nameKey, []byte("existing2"))
					return nil
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				return testStage(t, tx, uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")), tx.Bucket(_keyNamedGraphs)
			},
			args: args{
				name: ".",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				if assert.Len(t, got, 2) {
					assert.Equal(t, "3206ea3f-13ed-419a-b25c-4bc13e0a3968", got[0].Name())
					info, err := got[0].Info()
					assert.NoError(t, err)
					assert.Equal(t, int64(9), info.Size())
					assert.Equal(t, "e5d83a0b-a7a8-4518-9795-1c28c71b8d16", got[1].Name())
					info, err = got[1].Info()
					assert.NoError(t, err)
					assert.Equal(t, int64(8), info.Size())
				}
			},
			assertion: assert.NoError,
		},
		{
			name: "no_files",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					createTestStage(t, tx, uuid.MustParse("75098723-05bc-4748-a5e8-9f4a1d8628ea"))
					_, err := tx.CreateBucket(_keyNamedGraphs)
					return err
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				return testStage(t, tx, uuid.MustParse("75098723-05bc-4748-a5e8-9f4a1d8628ea")), tx.Bucket(_keyNamedGraphs)
			},
			args: args{
				name: ".",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				assert.Empty(t, got)
			},
			assertion: assert.NoError,
		},
		{
			name: "invalid_dir",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					createTestStage(t, tx, uuid.MustParse("3d95e428-3d2f-41f0-8340-e703a1be4cbe"))
					_, err := tx.CreateBucket(_keyNamedGraphs)
					return err
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				return testStage(t, tx, uuid.MustParse("75098723-05bc-4748-a5e8-9f4a1d8628ea")), tx.Bucket(_keyNamedGraphs)
			},
			args: args{
				name: "subdir",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				assert.Empty(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errStageDirectoryNotFound)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, tt.transactionCreator(db, func(tx *bbolt.Tx) error {
				stageBucket, namedGraphBucket := tt.bucketCreator(t, tx)
				f := stageFS{
					stage:       stageBucket,
					namedGraphs: namedGraphBucket,
				}
				got, err := f.ReadDir(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func Test_stageFS_OpenFile(t *testing.T) {
	type args struct {
		name string
		flag int
	}
	tests := []struct {
		name               string
		dbCreator          func(t testing.TB) *bbolt.DB
		transactionCreator func(db *bbolt.DB, fn func(*bbolt.Tx) error) error
		bucketCreator      func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket)
		args               args
		wantAssertion      func(t testing.TB, got fs.File)
		assertion          assert.ErrorAssertionFunc
		postAssertion      func(t testing.TB, db *bbolt.DB)
	}{
		{
			name: "create_file",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					createTestStage(t, tx, uuid.MustParse("45ef8714-66f7-4927-b94f-5abb87bc9274"))
					_, err := tx.CreateBucket(_keyNamedGraphs)
					return err
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).Update,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) (stage, namedGraphs *bbolt.Bucket) {
				return testStage(t, tx, uuid.MustParse("45ef8714-66f7-4927-b94f-5abb87bc9274")), tx.Bucket(_keyNamedGraphs)
			},
			args: args{
				name: "45ef8714-66f7-4927-b94f-5abb87bc9274",
				flag: os.O_CREATE | os.O_TRUNC,
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				_, err := got.(io.Writer).Write([]byte("line1\nline2\n"))
				assert.NoError(t, err)
				got.Close()
			},
			assertion: assert.NoError,
			postAssertion: func(t testing.TB, db *bbolt.DB) {
				assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
					nameKey := uuid.MustParse("45ef8714-66f7-4927-b94f-5abb87bc9274")
					stageBucket := testStage(t, tx, nameKey)
					assert.NotNil(t, stageBucket)
					namedGraphBucket := tx.Bucket(_keyNamedGraphs)
					assert.NotNil(t, namedGraphBucket)
					hash := stageBucket.Get(nameKey[:])
					assert.Equal(t, []byte{
						0x27, 0x51, 0xa3, 0xa2, 0xf3, 0x03, 0xad, 0x21, 0x75, 0x20, 0x38, 0x08, 0x5e, 0x2b, 0x8c, 0x5f,
						0x98, 0xec, 0xff, 0x61, 0xa2, 0xe4, 0xeb, 0xbd, 0x43, 0x50, 0x6a, 0x94, 0x17, 0x25, 0xbe, 0x80,
					}, hash)
					return nil
				}))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, tt.transactionCreator(db, func(tx *bbolt.Tx) error {
				stageBucket, namedGraphBucket := tt.bucketCreator(t, tx)
				f := stageFS{
					stage:       stageBucket,
					namedGraphs: namedGraphBucket,
				}
				got, err := f.OpenFile(tt.args.name, tt.args.flag, 0o444)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
			tt.postAssertion(t, db)
		})
	}
}

func createTestStage(t testing.TB, tx *bbolt.Tx, _ uuid.UUID) *bbolt.Bucket {
	allStageBucket, err := tx.CreateBucketIfNotExists(_keyStages)
	assert.NoError(t, err)
	stageBucket, err := allStageBucket.CreateBucket(stageKey(tx, uuid.Nil, _emptyRevision))
	assert.NoError(t, err)
	return stageBucket
}

func testStage(t testing.TB, tx *bbolt.Tx, _ uuid.UUID) *bbolt.Bucket {
	allStageBucket := tx.Bucket(_keyStages)
	assert.NotNil(t, allStageBucket)
	stageBucket := allStageBucket.Bucket(stageKey(tx, uuid.Nil, _emptyRevision))
	assert.NotNil(t, stageBucket)
	return stageBucket
}

func writeContentFunction(t testing.TB, stageBucket, namedGraphBucket *bbolt.Bucket, nameKey uuid.UUID, content []byte) {
	hash := sha256.Sum256(content)
	assert.NoError(t, namedGraphBucket.Put(hash[:], content))
	assert.NoError(t, stageBucket.Put(nameKey[:], hash[:]))
}

func stageKey(tx *bbolt.Tx, dsID uuid.UUID, revision [sha256.Size]byte) []byte {
	var key bytes.Buffer
	key.Write(dsID[:])
	key.Write(revision[:])
	stages := tx.Bucket(_keyStages)
	stageSeq := stages.Sequence()
	stageSeqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stageSeqBytes, stageSeq)
	key.Write(stageSeqBytes)
	return key.Bytes()
}

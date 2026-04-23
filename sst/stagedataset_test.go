// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestDatasetReadWriteFS_Open(t *testing.T) {
	type fields struct {
		dsSortedGraphIDs    []uuid.UUID
		dsSortedGraphHashes []Hash
	}
	type args struct {
		name string
	}
	nilHashBucketProvider := func(t testing.TB, tx *bbolt.Tx) (namedGraphRevisions *bbolt.Bucket) {
		namedGraphRevisions, _ = revisionBuckets(t, tx, HashNil())
		return
	}
	tests := []struct {
		name           string
		dbCreator      func(t testing.TB) (*bbolt.DB, fields)
		bucketProvider func(t testing.TB, tx *bbolt.Tx) (namedGraphRevisions *bbolt.Bucket)
		args           args
		wantAssertion  func(t testing.TB, got fs.File)
		assertion      assert.ErrorAssertionFunc
	}{
		{
			name: "just_created_rw_fs",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				return datasetTestDB(t, HashNil(), nil), fields{
					dsSortedGraphIDs:    nil,
					dsSortedGraphHashes: nil,
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: "3206ea3f-13ed-419a-b25c-4bc13e0a3968",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				assert.Nil(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errNamedGraphNotFoundInDataset)
			},
		},
		{
			name: "dataset_dir_file",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				return datasetTestDB(t, HashNil(), nil), fields{
					dsSortedGraphIDs:    nil,
					dsSortedGraphHashes: nil,
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: ".",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				fi, err := got.Stat()
				assert.NoError(t, err)
				assert.Equal(t, int64(0), fi.Size())
			},
			assertion: assert.NoError,
		},
		{
			name: "existing_dataset_file",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				ngID := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				var ngHash Hash
				db := datasetTestDB(t, HashNil(), func(t testing.TB, namedGraphRevisions, datasetRevision *bbolt.Bucket) {
					ngHash = testNG(t, namedGraphRevisions, datasetRevision, ngID, []byte{0, 1, 2})
				})
				return db, fields{
					dsSortedGraphIDs:    []uuid.UUID{ngID},
					dsSortedGraphHashes: []Hash{ngHash},
				}
			},
			bucketProvider: nilHashBucketProvider,
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, fields := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
				namedGraphRevisions := tt.bucketProvider(t, tx)
				f := localFullFs{
					bucketNgr:           namedGraphRevisions,
					dsSortedGraphIDs:    fields.dsSortedGraphIDs,
					dsSortedGraphHashes: fields.dsSortedGraphHashes,
				}
				got, err := f.Open(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func TestDatasetReadWriteFS_ReadDir(t *testing.T) {
	type fields struct {
		dsSortedGraphIDs    []uuid.UUID
		dsSortedGraphHashes []Hash
	}
	type args struct {
		name string
	}
	nilHashBucketProvider := func(t testing.TB, tx *bbolt.Tx) (namedGraphRevisions *bbolt.Bucket) {
		namedGraphRevisions, _ = revisionBuckets(t, tx, HashNil())
		return
	}
	tests := []struct {
		name           string
		dbCreator      func(t testing.TB) (*bbolt.DB, fields)
		bucketProvider func(t testing.TB, tx *bbolt.Tx) (namedGraphRevisions *bbolt.Bucket)
		args           args
		wantAssertion  func(t testing.TB, got []fs.DirEntry)
		assertion      assert.ErrorAssertionFunc
	}{
		{
			name: "no_files",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				return datasetTestDB(t, HashNil(), nil), fields{
					dsSortedGraphIDs:    nil,
					dsSortedGraphHashes: nil,
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: ".",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				assert.Empty(t, got)
			},
			assertion: assert.NoError,
		},
		{
			name: "two_files",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				ngID1 := uuid.MustParse("3206ea3f-13ed-419a-b25c-4bc13e0a3968")
				ngID2 := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				var ngHash1, ngHash2 Hash
				db := datasetTestDB(t, HashNil(), func(t testing.TB, namedGraphRevisions, datasetRevision *bbolt.Bucket) {
					ngHash1 = testNG(t, namedGraphRevisions, datasetRevision, ngID1, []byte("existing"))
					ngHash2 = testNG(t, namedGraphRevisions, datasetRevision, ngID2, []byte("existing2"))
				})
				return db, fields{
					dsSortedGraphIDs:    []uuid.UUID{ngID1, ngID2},
					dsSortedGraphHashes: []Hash{ngHash1, ngHash2},
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: ".",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				if assert.Len(t, got, 2) {
					assert.Equal(t, "3206ea3f-13ed-419a-b25c-4bc13e0a3968", got[0].Name())
					info, err := got[0].Info()
					assert.NoError(t, err)
					assert.Equal(t, int64(8), info.Size())
					assert.Equal(t, "e5d83a0b-a7a8-4518-9795-1c28c71b8d16", got[1].Name())
					info, err = got[1].Info()
					assert.NoError(t, err)
					assert.Equal(t, int64(9), info.Size())
				}
			},
			assertion: assert.NoError,
		},
		{
			name: "invalid_dir",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				return datasetTestDB(t, HashNil(), nil), fields{
					dsSortedGraphIDs:    nil,
					dsSortedGraphHashes: nil,
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: "subdir",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				assert.Empty(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errDatasetDirectoryNotFound)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, fields := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
				namedGraphRevisions := tt.bucketProvider(t, tx)
				f := localFullFs{
					bucketNgr:           namedGraphRevisions,
					dsSortedGraphIDs:    fields.dsSortedGraphIDs,
					dsSortedGraphHashes: fields.dsSortedGraphHashes,
				}
				got, err := f.ReadDir(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func TestDatasetReadWriteFS_OpenFile(t *testing.T) {
	type fields struct {
		dsSortedGraphIDs    []uuid.UUID
		dsSortedGraphHashes []Hash
	}
	type args struct {
		name string
		flag int
		perm fs.FileMode
	}
	nilHashBucketProvider := func(t testing.TB, tx *bbolt.Tx) (namedGraphRevisions *bbolt.Bucket) {
		namedGraphRevisions, _ = revisionBuckets(t, tx, HashNil())
		return
	}
	tests := []struct {
		name           string
		dbCreator      func(t testing.TB) (*bbolt.DB, fields)
		bucketProvider func(t testing.TB, tx *bbolt.Tx) (namedGraphRevisions *bbolt.Bucket)
		args           args
		wantAssertion  func(t testing.TB, got fs.File)
		assertion      assert.ErrorAssertionFunc
		postAssertion  func(t testing.TB, db *bbolt.DB, fields fields)
	}{
		{
			name: "recreate_file",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				db := datasetTestDB(t, HashNil(), nil)
				return db, fields{
					dsSortedGraphIDs:    []uuid.UUID{uuid.MustParse("45ef8714-66f7-4927-b94f-5abb87bc9274")},
					dsSortedGraphHashes: make([]Hash, 1),
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: "45ef8714-66f7-4927-b94f-5abb87bc9274",
				flag: os.O_CREATE | os.O_TRUNC,
				perm: 0o444,
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				_, err := got.(io.Writer).Write([]byte("line1\nline2\n"))
				assert.NoError(t, err)
				got.Close()
			},
			assertion: assert.NoError,
			postAssertion: func(t testing.TB, db *bbolt.DB, fields fields) {
				assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
					namedGraphRevisions := tx.Bucket(keyNamedGraphRevisions)
					assert.NotNil(t, namedGraphRevisions)
					ngHash := Hash{
						0x27, 0x51, 0xa3, 0xa2, 0xf3, 0x03, 0xad, 0x21, 0x75, 0x20, 0x38, 0x08, 0x5e, 0x2b, 0x8c, 0x5f,
						0x98, 0xec, 0xff, 0x61, 0xa2, 0xe4, 0xeb, 0xbd, 0x43, 0x50, 0x6a, 0x94, 0x17, 0x25, 0xbe, 0x80,
					}
					content := namedGraphRevisions.Get(ngHash[:])
					assert.NotNil(t, content)
					assert.Equal(t, ngHash, fields.dsSortedGraphHashes[0])
					return nil
				}))
			},
		},
		{
			name: "append_file_fails",
			dbCreator: func(t testing.TB) (*bbolt.DB, fields) {
				db := datasetTestDB(t, HashNil(), nil)
				return db, fields{
					dsSortedGraphIDs:    []uuid.UUID{uuid.MustParse("45ef8714-66f7-4927-b94f-5abb87bc9274")},
					dsSortedGraphHashes: make([]Hash, 1),
				}
			},
			bucketProvider: nilHashBucketProvider,
			args: args{
				name: "45ef8714-66f7-4927-b94f-5abb87bc9274",
				flag: os.O_RDWR | os.O_APPEND,
				perm: 0o444,
			},
			wantAssertion: func(t testing.TB, got fs.File) {},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errOpenFileMissedTruncFlagUnsupported)
			},
			postAssertion: func(t testing.TB, db *bbolt.DB, fields fields) {},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, fields := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
				namedGraphRevisions := tt.bucketProvider(t, tx)
				f := localFullFs{
					bucketNgr:           namedGraphRevisions,
					dsSortedGraphIDs:    fields.dsSortedGraphIDs,
					dsSortedGraphHashes: fields.dsSortedGraphHashes,
				}
				got, err := f.OpenFile(tt.args.name, tt.args.flag, tt.args.perm)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
			tt.postAssertion(t, db, fields)
		})
	}
}

func datasetTestDB(
	t testing.TB,
	dsHash Hash,
	augmenter func(t testing.TB, namedGraphRevisions, datasetRevision *bbolt.Bucket),
) *bbolt.DB {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
	assert.NoError(t, err)
	assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		namedGraphRevisions, err := tx.CreateBucket(keyNamedGraphRevisions)
		assert.NoError(t, err)
		datasetRevisions, err := tx.CreateBucket(keyDatasetRevisions)
		assert.NoError(t, err)
		datasetRevision, err := datasetRevisions.CreateBucket(dsHash[:])
		assert.NoError(t, err)
		if augmenter != nil {
			augmenter(t, namedGraphRevisions, datasetRevision)
		}
		return nil
	}))
	return db
}

func revisionBuckets(t testing.TB, tx *bbolt.Tx, dsHash Hash) (namedGraphRevisions, datasetRevision *bbolt.Bucket) {
	namedGraphRevisions = tx.Bucket(keyNamedGraphRevisions)
	assert.NotNil(t, namedGraphRevisions)
	datasetRevisions := tx.Bucket(keyDatasetRevisions)
	assert.NotNil(t, datasetRevisions)
	datasetRevision = datasetRevisions.Bucket(dsHash[:])
	assert.NotNil(t, datasetRevision)
	return
}

func testNG(t testing.TB, namedGraphRevisions, datasetRevision *bbolt.Bucket, nameKey uuid.UUID, content []byte) Hash {
	hash := sha256.Sum256(content)
	assert.NoError(t, namedGraphRevisions.Put(hash[:], content))
	assert.NoError(t, datasetRevision.Put(iDToPrefixedKey(nameKey, ngPrefix), hash[:]))
	return hash
}

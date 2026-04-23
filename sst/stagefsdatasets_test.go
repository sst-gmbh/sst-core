// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestRepositoryFS_Open(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name          string
		dbCreator     func(t testing.TB) *bbolt.DB
		args          args
		wantAssertion func(t testing.TB, got fs.File)
		assertion     assert.ErrorAssertionFunc
	}{
		{
			name: "existing_dataset",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					datasets, err := tx.CreateBucket(keyDatasets)
					assert.NoError(t, err)
					dsID := uuid.MustParse("57e4fbbc-1315-4084-afd6-6191a055a583")
					dsHash := HashNil()
					return datasets.Put(dsID[:], dsHash[:])
				}))
				return db
			},
			args: args{
				name: "57e4fbbc-1315-4084-afd6-6191a055a583",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				fi, err := got.Stat()
				assert.NoError(t, err)
				assert.Equal(t, "57e4fbbc-1315-4084-afd6-6191a055a583", fi.Name())
			},
			assertion: assert.NoError,
		},
		{
			name: "empty_repository_fails",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				return db
			},
			args: args{
				name: "57e4fbbc-1315-4084-afd6-6191a055a583",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				assert.Nil(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errDatasetNotFoundInRepository)
			},
		},
		{
			name: "empty_datasets_bucket_fails",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					_, err := tx.CreateBucket(keyDatasets)
					return err
				}))
				return db
			},
			args: args{
				name: "57e4fbbc-1315-4084-afd6-6191a055a583",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				assert.Nil(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errDatasetNotFoundInRepository)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
				f := datasetsFS{
					datasets: tx.Bucket(keyDatasets),
				}
				got, err := f.Open(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func TestRepositoryFS_ReadDir(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name          string
		dbCreator     func(t testing.TB) *bbolt.DB
		args          args
		wantAssertion func(t testing.TB, got []fs.DirEntry)
		assertion     assert.ErrorAssertionFunc
	}{
		{
			name: "two_datasets",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					datasets, err := tx.CreateBucket(keyDatasets)
					assert.NoError(t, err)
					dsID1 := uuid.MustParse("57e4fbbc-1315-4084-afd6-6191a055a583")
					dsHash := HashNil()
					assert.NoError(t, datasets.Put(dsID1[:], dsHash[:]))
					dsID2 := uuid.MustParse("23454c4f-5ed8-484c-bddf-6e70a8abb879")
					return datasets.Put(dsID2[:], dsHash[:])
				}))
				return db
			},
			args: args{
				name: ".",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				if assert.Len(t, got, 2) {
					assert.Equal(t, "23454c4f-5ed8-484c-bddf-6e70a8abb879", got[0].Name())
					assert.Equal(t, "57e4fbbc-1315-4084-afd6-6191a055a583", got[1].Name())
				}
			},
			assertion: assert.NoError,
		},
		{
			name: "invalid_dir",
			dbCreator: func(t testing.TB) *bbolt.DB {
				db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o444, nil)
				assert.NoError(t, err)
				assert.NoError(t, db.Update(func(tx *bbolt.Tx) error {
					datasets, err := tx.CreateBucket(keyDatasets)
					assert.NoError(t, err)
					dsID := uuid.MustParse("57e4fbbc-1315-4084-afd6-6191a055a583")
					dsHash := HashNil()
					return datasets.Put(dsID[:], dsHash[:])
				}))
				return db
			},
			args: args{
				name: "subdir",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				assert.Len(t, got, 0)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errRepositoryDirectoryNotFound)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
				f := datasetsFS{
					datasets: tx.Bucket(keyDatasets),
				}
				got, err := f.ReadDir(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func Test_repositoryFileReader_Read(t *testing.T) {
	t.Run("repository_file_read_returns_error", func(t *testing.T) {
		f := repositoryFile{
			keyedFile: keyedFile{
				nameKey: uuid.Nil,
			},
		}
		got, err := f.Read(make([]byte, 1024))
		assert.ErrorIs(t, err, errCannotReadRepositoryFile)
		assert.Equal(t, 0, got)
	})
}

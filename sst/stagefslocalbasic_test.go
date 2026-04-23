// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestRepositoryBucketFS_Open(t *testing.T) {
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
		bucketCreator      func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket
		args               args
		wantAssertion      func(t testing.TB, got fs.File)
		assertion          assert.ErrorAssertionFunc
	}{
		{
			name:               "just_created_bucket_file",
			dbCreator:          testDB,
			transactionCreator: (*bbolt.DB).Update,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket, err := tx.CreateBucket([]byte("t"))
				assert.NoError(t, err)
				nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				err = bucket.Put(nameKey[:], []byte{0, 1, 2})
				assert.NoError(t, err)
				return bucket
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
					bucket, err := tx.CreateBucket([]byte("t"))
					assert.NoError(t, err)
					nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
					return bucket.Put(nameKey[:], []byte("existing"))
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket := tx.Bucket([]byte("t"))
				assert.NotNil(t, bucket)
				return bucket
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
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket, err := tx.CreateBucket([]byte("t"))
				assert.NoError(t, err)
				nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				err = bucket.Put(nameKey[:], []byte{1})
				assert.NoError(t, err)
				return bucket
			},
			args: args{
				name: "3206ea3f-13ed-419a-b25c-4bc13e0a3968",
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				assert.Nil(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errFileNotFoundInBucket)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, tt.transactionCreator(db, func(tx *bbolt.Tx) error {
				bucket := tt.bucketCreator(t, tx)
				f := localBasicFS{
					bucket: bucket,
				}
				got, err := f.Open(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func TestRepositoryBucketFS_ReadDir(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name               string
		dbCreator          func(t testing.TB) *bbolt.DB
		transactionCreator func(db *bbolt.DB, fn func(*bbolt.Tx) error) error
		bucketCreator      func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket
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
					bucket, err := tx.CreateBucket([]byte("t"))
					assert.NoError(t, err)
					nameKey := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
					err = bucket.Put(nameKey[:], []byte("existing"))
					if err != nil {
						return err
					}
					nameKey = uuid.MustParse("3206ea3f-13ed-419a-b25c-4bc13e0a3968")
					err = bucket.Put(nameKey[:], []byte("existing2"))
					if err != nil {
						return err
					}
					return nil
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket := tx.Bucket([]byte("t"))
				assert.NotNil(t, bucket)
				return bucket
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
					_, err := tx.CreateBucket([]byte("t"))
					return err
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket := tx.Bucket([]byte("t"))
				assert.NotNil(t, bucket)
				return bucket
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
					_, err := tx.CreateBucket([]byte("t"))
					return err
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).View,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket := tx.Bucket([]byte("t"))
				assert.NotNil(t, bucket)
				return bucket
			},
			args: args{
				name: "subdir",
			},
			wantAssertion: func(t testing.TB, got []fs.DirEntry) {
				assert.Empty(t, got)
			},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, errPathNotFound)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.dbCreator(t)
			defer db.Close()
			assert.NoError(t, tt.transactionCreator(db, func(tx *bbolt.Tx) error {
				bucket := tt.bucketCreator(t, tx)
				f := localBasicFS{
					bucket: bucket,
				}
				got, err := f.ReadDir(tt.args.name)
				tt.assertion(t, err)
				tt.wantAssertion(t, got)
				return nil
			}))
		})
	}
}

func TestRepositoryBucketFS_OpenFile(t *testing.T) {
	type args struct {
		name string
		flag int
	}
	tests := []struct {
		name               string
		dbCreator          func(t testing.TB) *bbolt.DB
		transactionCreator func(db *bbolt.DB, fn func(*bbolt.Tx) error) error
		bucketCreator      func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket
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
					_, err := tx.CreateBucket([]byte("t"))
					return err
				}))
				return db
			},
			transactionCreator: (*bbolt.DB).Update,
			bucketCreator: func(t testing.TB, tx *bbolt.Tx) *bbolt.Bucket {
				bucket := tx.Bucket([]byte("t"))
				assert.NotNil(t, bucket)
				return bucket
			},
			args: args{
				name: "45ef8714-66f7-4927-b94f-5abb87bc9274",
				flag: os.O_CREATE | os.O_TRUNC,
			},
			wantAssertion: func(t testing.TB, got fs.File) {
				_, err := got.(io.Writer).Write([]byte("line1\nline2"))
				assert.NoError(t, err)
				got.Close()
			},
			assertion: assert.NoError,
			postAssertion: func(t testing.TB, db *bbolt.DB) {
				assert.NoError(t, db.View(func(tx *bbolt.Tx) error {
					bucket := tx.Bucket([]byte("t"))
					assert.NotNil(t, bucket)
					nameKey := uuid.MustParse("45ef8714-66f7-4927-b94f-5abb87bc9274")
					value := bucket.Get(nameKey[:])
					assert.Equal(t, []byte("line1\nline2"), value)
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
				bucket := tt.bucketCreator(t, tx)
				f := localBasicFS{
					bucket: bucket,
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

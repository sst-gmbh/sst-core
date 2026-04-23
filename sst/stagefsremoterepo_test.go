// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"crypto/sha256"
	"io"
	"testing"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"github.com/stretchr/testify/assert"
)

func TestMemRevFS_Open(t *testing.T) {
	type fields struct {
		ngRevisions map[Hash][]byte
		namedGraphs map[uuid.UUID]Hash
	}
	type args struct {
		name string
	}
	type test struct {
		fields        fields
		args          args
		wantAssertion func(t testing.TB, got fs.File)
		assertion     assert.ErrorAssertionFunc
	}
	tests := []struct {
		name        string
		testCreator func(t testing.TB) test
	}{
		{
			name: "existing_dataset_file",
			testCreator: func(t testing.TB) test {
				ngRevisions := map[Hash][]byte{}
				namedGraphs := map[uuid.UUID]Hash{}
				id := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				testMemNG(ngRevisions, namedGraphs, id, []byte{0, 1, 2})
				return test{
					fields: fields{
						ngRevisions: ngRevisions,
						namedGraphs: namedGraphs,
					},
					args: args{
						name: id.String(),
					},
					wantAssertion: func(t testing.TB, got fs.File) {
						val, err := io.ReadAll(got.(io.Reader))
						assert.NoError(t, err)
						assert.Equal(t, []byte{0, 1, 2}, val)
					},
					assertion: assert.NoError,
				}
			},
		},
		{
			name: "missing_dataset_file",
			testCreator: func(t testing.TB) test {
				return test{
					fields: fields{
						ngRevisions: map[Hash][]byte{},
						namedGraphs: map[uuid.UUID]Hash{},
					},
					args: args{
						name: "3206ea3f-13ed-419a-b25c-4bc13e0a3968",
					},
					wantAssertion: func(t testing.TB, got fs.File) {
						assert.Nil(t, got)
					},
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						return assert.ErrorIs(t, err, errNamedGraphNotFoundInDataset)
					},
				}
			},
		},
		{
			name: "dataset_dir_file",
			testCreator: func(t testing.TB) test {
				ngRevisions := map[Hash][]byte{}
				namedGraphs := map[uuid.UUID]Hash{}
				id := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				testMemNG(ngRevisions, namedGraphs, id, []byte{0, 1, 2})
				return test{
					fields: fields{
						ngRevisions: ngRevisions,
						namedGraphs: namedGraphs,
					},
					args: args{
						name: ".",
					},
					wantAssertion: func(t testing.TB, got fs.File) {
						fi, err := got.Stat()
						assert.NoError(t, err)
						assert.Equal(t, int64(0), fi.Size())
					},
					assertion: assert.NoError,
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttt := tt.testCreator(t)
			f := remoteRepoFS{
				ngRevisions: ttt.fields.ngRevisions,
				namedGraphs: ttt.fields.namedGraphs,
			}
			got, err := f.Open(ttt.args.name)
			ttt.assertion(t, err)
			ttt.wantAssertion(t, got)
		})
	}
}

func TestMemRevFS_ReadDir(t *testing.T) {
	type fields struct {
		ngRevisions map[Hash][]byte
		namedGraphs map[uuid.UUID]Hash
	}
	type args struct {
		name string
	}
	type test struct {
		fields        fields
		args          args
		wantAssertion func(t testing.TB, got []fs.DirEntry)
		assertion     assert.ErrorAssertionFunc
	}
	tests := []struct {
		name        string
		testCreator func(t testing.TB) test
	}{
		{
			name: "two_files",
			testCreator: func(t testing.TB) test {
				ngRevisions := map[Hash][]byte{}
				namedGraphs := map[uuid.UUID]Hash{}
				id1 := uuid.MustParse("e5d83a0b-a7a8-4518-9795-1c28c71b8d16")
				id2 := uuid.MustParse("3206ea3f-13ed-419a-b25c-4bc13e0a3968")
				testMemNG(ngRevisions, namedGraphs, id1, []byte("existing"))
				testMemNG(ngRevisions, namedGraphs, id2, []byte("existing2"))
				return test{
					fields: fields{
						ngRevisions: ngRevisions,
						namedGraphs: namedGraphs,
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
				}
			},
		},
		{
			name: "no_files",
			testCreator: func(t testing.TB) test {
				return test{
					fields: fields{
						ngRevisions: map[Hash][]byte{},
						namedGraphs: map[uuid.UUID]Hash{},
					},
					args: args{
						name: ".",
					},
					wantAssertion: func(t testing.TB, got []fs.DirEntry) {
						assert.Empty(t, got)
					},
					assertion: assert.NoError,
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttt := tt.testCreator(t)
			f := remoteRepoFS{
				ngRevisions: ttt.fields.ngRevisions,
				namedGraphs: ttt.fields.namedGraphs,
			}
			got, err := f.ReadDir(ttt.args.name)
			ttt.assertion(t, err)
			ttt.wantAssertion(t, got)
		})
	}
}

func testMemNG(ngRevisions map[Hash][]byte, namedGraphs map[uuid.UUID]Hash, id uuid.UUID, content []byte) {
	hash := sha256.Sum256(content)
	ngRevisions[hash] = content
	namedGraphs[id] = hash
}

// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_localFlatRepo_Commit(t *testing.T) {
	type args struct {
		message string
	}
	type test struct {
		d         string
		config    repoConfig
		stage     Stage
		args      args
		assertion assert.ErrorAssertionFunc
	}
	tests := []struct {
		name        string
		testCreator func(t *testing.T) test
	}{
		{
			name: "empty_message",
			testCreator: func(t *testing.T) test {
				d := createRepDir(t)
				id := uuid.MustParse("8799e0e1-2484-4097-9c10-1301461cf6d1")
				assertion := func(t assert.TestingT, err error, _ ...interface{}) bool {
					return assert.ErrorIs(t, err, ErrEmptyCommitMessage)
				}
				return test{
					stage: testLocalFlatStage(t, d, id, nil),
					args: args{
						message: "",
					},
					assertion: assertion,
					config: repoConfig{
						repositoryDir: d,
					},
				}
			},
		},

		{
			name: "simple",
			testCreator: func(t *testing.T) test {
				d := createRepDir(t)
				id := uuid.MustParse("2be0be85-0e7d-49b2-9b2c-6751ae6883e2")
				assertion := func(t assert.TestingT, err error, m ...interface{}) bool {
					if !assert.NoError(t, err) {
						return false
					}
					gotCommitRev := m[0].(Hash)
					assert.Equal(t, HashNil(), gotCommitRev)
					de, err := os.ReadDir(d)
					if !assert.NoError(t, err) {
						return false
					}
					return assert.Len(t, de, 1) &&
						assert.Equal(t, "dXJuOnV1aWQ6MmJlMGJlODUtMGU3ZC00OWIyLTliMmMtNjc1MWFlNjg4M2Uy.sst", de[0].Name()) &&
						assert.False(t, de[0].IsDir()) &&
						assert.Greater(t, noError{t}.fileInfo(de[0].Info()).Size(), int64(0))
				}
				return test{
					d:     d,
					stage: testLocalFlatStage(t, d, id, nil),
					args: args{
						message: "test",
					},
					assertion: assertion,
					config: repoConfig{
						repositoryDir: d,
					},
				}
			},
		},
		{
			name: "post_commit_notification_single_dataset",
			testCreator: func(t *testing.T) test {
				d := createRepDir(t)
				id := uuid.MustParse("971636e0-4aed-4572-b694-7107cdf99013")
				assertion := func(t assert.TestingT, err error, _ ...interface{}) bool {
					return true
				}
				stage := testLocalFlatStage(t, d, id, nil)
				return test{
					d: d,
					config: repoConfig{
						postCommitNotification: func(stage Stage, commitID Hash) {
							assert.Equal(t, Hash{}, commitID)
							gitHash := Hash{}
							copy(gitHash[:], commitID[:])
						},
						repositoryDir: d,
					},
					stage: stage,
					args: args{
						message: "post commit notification",
					},
					assertion: assertion,
				}
			},
		},
		{
			name: "post_commit_notification_second_dataset",
			testCreator: func(t *testing.T) test {
				d := createRepDir(t)
				prevStage := testLocalFlatStage(t, d, uuid.MustParse("49e3cfec-677b-4978-8cf1-ea730eb62464"), nil)
				prevStage.Commit(context.TODO(), "prev dataset", DefaultBranch)
				id := uuid.MustParse("0867b0e7-a917-4053-bd19-8ced4fddf141")
				assertion := func(t assert.TestingT, err error, _ ...interface{}) bool {
					return true
				}
				stage := testLocalFlatStage(t, d, id, nil)

				return test{
					d: d,
					config: repoConfig{
						postCommitNotification: func(stage Stage, commitID Hash) {
							assert.Equal(t, Hash{}, commitID)
							gitHash := Hash{}
							copy(gitHash[:], commitID[:])

						},
						repositoryDir: d,
					},
					stage: stage,
					args: args{
						message: "post commit notification",
					},
					assertion: assertion,
				}
			},
		},
		{
			name: "post_commit_notification_single_modified_dataset",
			testCreator: func(t *testing.T) test {
				d := createRepDir(t)
				id := uuid.MustParse("6505581b-97d9-41ad-853e-6a2f51da43c4")
				stage := testLocalFlatStage(t, d, id, nil)
				stage.Commit(context.TODO(), "created", DefaultBranch)

				g := stage.NamedGraphs()[0]
				assert.NotNil(t, g)

				ib := g.CreateIRINode("another")
				assert.NotNil(t, ib)

				assertion := assert.NoError

				return test{
					d: d,
					config: repoConfig{
						postCommitNotification: func(stage Stage, commitID Hash) {
							assert.Equal(t, Hash{}, commitID)
							var gitHash Hash
							copy(gitHash[:], commitID[:])
						},
						repositoryDir: d,
					},
					stage: stage,
					args: args{
						message: "post commit notification",
					},
					assertion: assertion,
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttt := tt.testCreator(t)
			r := localFlatRepository{
				config: ttt.config,
			}

			gotCommitRev, _, err := r.commitNewVersion(context.TODO(), ttt.stage.(*stage), ttt.args.message, DefaultBranch)
			ttt.assertion(t, err, gotCommitRev)
		})
	}
}

func Test_localFlatRepository_OpenDataset(t *testing.T) {
	type fields struct {
		repositoryDirectory func(t *testing.T) string
	}
	type args struct {
		iri IRI
	}
	type test struct {
		fields          fields
		args            args
		wantNIAssertion assert.ValueAssertionFunc
		assertion       assert.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "open_predefined_dataset",
			test: func() test {
				var tt test
				var d string
				tt = test{
					fields: fields{
						repositoryDirectory: func(t *testing.T) string {
							var err error
							d, err = filepath.Abs("testdata/localflatrepository")
							assert.NoError(t, err)
							return d
						},
					},
					args: args{
						iri: "http://www.w3.org/ns/shacl",
					},
					wantNIAssertion: func(t assert.TestingT, gotNI interface{}, misc ...interface{}) bool {
						r := misc[0].(*localFlatRepository)
						return assert.Equal(t, &localFlatDataset{r: r, iri: "http://www.w3.org/ns/shacl"}, gotNI)
					},
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if !assert.NoError(t, err) {
							return false
						}
						de, err := os.ReadDir(d)
						if !assert.NoError(t, err) {
							return false
						}
						return assert.Len(t, de, 1)
					},
				}
				return tt
			}(),
		},
		{
			name: "open_missing_dataset",
			test: func() test {
				var tt test
				var d string
				tt = test{
					fields: fields{
						repositoryDirectory: func(t *testing.T) string {
							d = createRepDir(t)
							c, err := os.Create(filepath.Join(d, "481bcb92-e4f0-47ec-8cec-1b3564415054.sst"))
							assert.NoError(t, err)
							assert.NoError(t, c.Close())
							return d
						},
					},
					args: args{
						iri: "http://www.w3.org/ns/shacl",
					},
					wantNIAssertion: assert.Nil,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						return assert.ErrorIs(t, err, ErrDatasetNotFound)
					},
				}
				return tt
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := localFlatRepository{
				config: repoConfig{
					repositoryDir: tt.fields.repositoryDirectory(t),
				},
			}
			gotNI, err := r.Dataset(context.TODO(), tt.args.iri)
			tt.assertion(t, err)
			if gotNI != nil {
				tt.wantNIAssertion(t, gotNI, &r)
			}
		})
	}
}

func createRepDir(t testing.TB) string {
	d := filepath.Join(t.TempDir(), "repdir")
	assert.NoError(t, os.MkdirAll(d, 0o777))
	return d
}

func Test_create_testdata_localFlatRepository(t *testing.T) {
	de, err := os.ReadDir("testdata/localflatrepository")
	require.NoError(t, err)
	if len(de) != 0 {
		t.Skip("testdata/localflatrepository already created")
	}
	repo, err := OpenLocalFlatRepository("testdata/localflatrepository")
	assert.NoError(t, err)

	st := repo.OpenStage(DefaultTriplexMode)
	ng := st.CreateNamedGraph(IRI(uuid.MustParse("66fb8abc-04ab-4c9c-a8f5-cd014fd433ed").URN()))
	m := ng.CreateIRINode("main")
	m.AddStatement(rdfType, ssoPart)
	m.AddStatement(rdfsComment, String("main part"))

	st.Commit(context.TODO(), "initial version", DefaultBranch)
}

func (t noError) fileInfo(info os.FileInfo, err error) os.FileInfo {
	assert.NoError(t, err)
	return info
}

func testLocalFlatStage(t testing.TB, dir string, id uuid.UUID, augmenter func(Stage)) Stage {
	repo, err := OpenLocalFlatRepository(dir)
	require.NoError(t, err)
	st := repo.OpenStage(DefaultTriplexMode)
	ng := st.CreateNamedGraph(IRI(id.URN()))
	main := ng.CreateIRINode("main")
	main.AddStatement(rdfType, lciOrganization)
	main.AddStatement(rdfsLabel, String("ABC Ltd."))
	if augmenter != nil {
		augmenter(st)
	}
	return st
}

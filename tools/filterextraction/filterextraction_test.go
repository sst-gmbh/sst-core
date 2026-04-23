// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package filterextraction_test

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"git.semanticstep.net/x/sst/sst"
	"git.semanticstep.net/x/sst/step/ap242xmlimport"
	"git.semanticstep.net/x/sst/sst_test/testutil"
	filterextraction "git.semanticstep.net/x/sst/tools/filterextraction"
	_ "git.semanticstep.net/x/sst/vocabularies/dict"
	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testWD                     = "../.."
	stepxmlLocalFlatRepository = "step/testdata/ewhfortest/stepxmlLocalFlatRepository"
	stepxmlLocalFullRepository = "step/testdata/ewhfortest/stepxmlLocalFullRepository"
	testDataBase               = "step/testdata/ewhfortest/"
)

func Test_stepxmlRepository(t *testing.T) {
	prevWD, err := os.Getwd()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, os.Chdir(prevWD))
	})
	require.NoError(t, os.Chdir(testWD))
	log.SetOutput(io.Discard)
	createInputSources(t)
	t.Run("local_flat_repository", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, os.RemoveAll(stepxmlLocalFlatRepository))

		r, err := sst.OpenLocalFlatRepository(stepxmlLocalFlatRepository)
		if err != nil {
			if err == sst.ErrRepositoryDoesNotExist {
				r, err = sst.CreateLocalFlatRepository(stepxmlLocalFlatRepository)
				if err != nil {
					return
				}
			} else {
				return
			}
		}

		defer r.Close()

		assert.NotPanics(t, func() {
			filterextraction.Run(r, testDataBase)
		})

		// check generated data
		testutil.DetailLogf(t, "==start loadStepxmlRepository for %s", t.Name())
		loadStepxmlRepository(t, r, fs.DirFS(stepxmlLocalFlatRepository))
		testutil.DetailLogf(t, "==end loadStepxmlRepository for %s", t.Name())
	})
	t.Run("storage_repository", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, os.RemoveAll(stepxmlLocalFullRepository))

		p, err := sst.OpenLocalRepository(stepxmlLocalFullRepository, "default@semanticstep.net", "default")
		if err != nil {
			if err == sst.ErrRepositoryDoesNotExist {
				p, err = sst.CreateLocalRepository(stepxmlLocalFullRepository, "default@semanticstep.net", "default", true)
				if err != nil {
					return
				}
			} else {
				return
			}
		}
		defer p.Close()

		tempPath := t.TempDir()
		tempP, err := sst.OpenLocalRepository(tempPath, "default@semanticstep.net", "default")
		if err != nil {
			if err == sst.ErrRepositoryDoesNotExist {
				tempP, err = sst.CreateLocalRepository(tempPath, "default@semanticstep.net", "default", true)
				if err != nil {
					return
				}
			} else {
				return
			}
		}
		defer tempP.Close()

		assert.NotPanics(t, func() {
			filterextraction.Run(p, testDataBase)
		})
		assert.NotPanics(t, func() {
			filterextraction.Run(tempP, testDataBase)
		})

		// check generated data
		loadStepxmlRepositoryDB := func(repoName string, r sst.Repository) {
			defer r.Close()
			testutil.DetailLogf(t, "==start loadStepxmlRepository for %s in %s", t.Name(), repoName)
			loadStepxmlRepository(t, r, fs.DirFS(stepxmlLocalFlatRepository))
			testutil.DetailLogf(t, "==end loadStepxmlRepository for %s in %s", t.Name(), repoName)
		}
		loadStepxmlRepositoryDB("LocalFull repository 1", p)
		loadStepxmlRepositoryDB("LocalFull repository 2", tempP)
	})
}

func createInputSources(t *testing.T) {
	if testutil.DetailLogEnabled() {
		var logOut strings.Builder
		ap242xmlimport.Logger().SetOutput(&logOut)
		defer t.Logf("\n%s", &logOut)
	} else {
		ap242xmlimport.Logger().SetOutput(io.Discard)
	}
	for _, testFile := range []string{
		//		"Assembly1_EMCoS",
		//		"BNC_MultiConnection",
		//		"Connectivity1_EMCoS",
		//		"Connectivity2_EMCoS",
		"HarnessExample_DesignSplitting",
		"HarnessExample_Flat",
		"HarnessExample_Hierarchical",
		"HarnessExample_HierarchicalReflect",
		//		"Topology1_EMCoS",
		//		"Topology2_EMCoS",
		//		"Topology3_EMCoS",
	} {
		assert.NotPanics(t, func() {
			t.Log("Start XML file=", testDataBase+testFile+".xml")
			graph, err := ap242xmlimport.AP242XmlImport(testDataBase + testFile + ".xml")
			t.Log("End XML file, error=", err)
			assert.NoError(t, err)
			t.Log("Start TTL file=", testDataBase+testFile+".ttl")
			f, err := os.Create(testDataBase + testFile + ".ttl")
			if err != nil {
				panic(err)
			}
			defer f.Close()
			err = graph.RdfWrite(f, sst.RdfFormatTurtle)
			t.Log("End TTL file, error=", err)
			assert.NoError(t, err)
		})
	}
}

func Benchmark_createRepository(b *testing.B) {
	log.SetOutput(io.Discard)
	prevWD, err := os.Getwd()
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, os.Chdir(prevWD))
	})
	require.NoError(b, os.Chdir(testWD))
	b.Run("local_flat_repository", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := b.TempDir()
			tempPath := b.TempDir()

			r, err := sst.OpenLocalFlatRepository(path)
			if err != nil {
				if err == sst.ErrRepositoryDoesNotExist {
					r, err = sst.CreateLocalFlatRepository(path)
					if err != nil {
						return
					}
				} else {
					return
				}
			}
			defer r.Close()

			tempR, err := sst.OpenLocalFlatRepository(tempPath)
			if err != nil {
				if err == sst.ErrRepositoryDoesNotExist {
					tempR, err = sst.CreateLocalFlatRepository(tempPath)
					if err != nil {
						return
					}
				} else {
					return
				}
			}
			defer tempR.Close()

			createRepository(b, r, tempR, testDataBase)
		}
	})
	b.Run("flat_storage_repository", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := b.TempDir()
			tempPath := b.TempDir()

			r, err := sst.OpenLocalFlatRepository(filepath.Join(path))
			if err != nil {
				if err == sst.ErrRepositoryDoesNotExist {
					r, err = sst.CreateLocalFlatRepository(filepath.Join(path))
					if err != nil {
						return
					}
				} else {
					return
				}
			}
			defer r.Close()

			tempR, err := sst.OpenLocalFlatRepository(tempPath)
			if err != nil {
				if err == sst.ErrRepositoryDoesNotExist {
					tempR, err = sst.CreateLocalFlatRepository(tempPath)
					if err != nil {
						return
					}
				} else {
					return
				}
			}
			defer tempR.Close()

			createRepository(b, r, tempR, testDataBase)
		}
	})
	b.Run("storage_repository", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := b.TempDir()
			tempPath := b.TempDir()
			r, err := sst.OpenLocalRepository(path, "default@semanticstep.net", "default")
			if err != nil {
				if err == sst.ErrRepositoryDoesNotExist {
					r, err = sst.CreateLocalRepository(path, "default@semanticstep.net", "default", true)
					if err != nil {
						return
					}
				} else {
					return
				}
			}
			defer r.Close()

			tempR, err := sst.OpenLocalRepository(tempPath, "default@semanticstep.net", "default")
			if err != nil {
				if err == sst.ErrRepositoryDoesNotExist {
					tempR, err = sst.CreateLocalRepository(tempPath, "default@semanticstep.net", "default", true)
					if err != nil {
						return
					}
				} else {
					return
				}
			}
			defer tempR.Close()

			createRepository(b, r, tempR, testDataBase)
		}
	})
}

func createRepository(t testing.TB, repo sst.Repository,
	tempRepo sst.Repository, tempPath string) {
	assert.NotPanics(t, func() {
		filterextraction.Run(repo, testDataBase)
	})
	assert.NotPanics(t, func() {
		filterextraction.Run(tempRepo, tempPath)
		filterextraction.Run(tempRepo, tempPath) // test merging of the repository contents
	})
}

func loadStepxmlRepository(t *testing.T, repository sst.Repository, repFS fs.FS) {
	_, err := fs.Stat(repFS, ".")
	if os.IsNotExist(err) {
		t.Log("stepxmlRepository does not exist")
		return
	}
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.NoError(t, fs.WalkDir(repFS, ".", func(path string, d fs.DirEntry, _ error) error {
		if path == "." {
			return nil
		}
		if d.IsDir() || strings.IndexByte(path, '.') >= 0 {
			return fs.SkipDir
		}
		testutil.DetailLogf(t, "Namespace %v\n", d.Name())
		namespace, err := repository.Dataset(context.TODO(), sst.IRI(uuid.MustParse(d.Name()).URN()))
		assert.NoError(t, err)
		stage, err := namespace.CheckoutBranch(context.TODO(), sst.DefaultBranch, sst.DefaultTriplexMode)
		assert.NoError(t, err)

		var gIRIs []sst.IRI
		for _, ng := range stage.NamedGraphs() {
			gIRIs = append(gIRIs, ng.IRI())
		}

		testutil.DetailLogf(t, "  Graph count: %d\n", len(gIRIs))

		graph := stage.NamedGraph(sst.IRI(uuid.MustParse(d.Name()).URN()))
		if assert.NoError(t, err) {
			var nodeCnt, typeOfCnt int
			ds := graph.GetSortedIBNodes()
			for _, d := range ds {
				if d.TypeOf() != nil {
					typeOfCnt++
				}
				nodeCnt++
				if d.TypeOf() != nil {
					if d.IsBlankNode() {
						testutil.DetailLogf(t, "  Node _:%s type: %s\n", d.ID(), d.TypeOf().PrefixedFragment())
					} else {
						testutil.DetailLogf(t, "  Node %s type: %s\n", d.Fragment(), d.TypeOf().PrefixedFragment())
					}
				} else {
					testutil.DetailLogf(t, "  Node %s type: %s\n", d.Fragment(), "(nil)")
				}
			}
			if nodeCnt > 1 {
				assert.Greater(t, typeOfCnt, 0)
			}
		}
		return nil
	}))
}

func TestMain(m *testing.M) {
	testutil.DetailLogMain(m)
}

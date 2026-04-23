// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"context"
	"path/filepath"
	"testing"

	"git.semanticstep.net/x/sst/sst"
	"git.semanticstep.net/x/sst/sstauth"
	"git.semanticstep.net/x/sst/sst_test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_remoteRepositoryInfo_SingleNamedGraph_SingleCommit(t *testing.T) {
	testName := t.Name() + "Repo"
	dir := filepath.Join("./testdata/" + testName)
	transportCreds, err := testutil.TestTransportCreds()
	require.NoError(t, err)

	defer removeFolder(dir)
	t.Run("write", func(t *testing.T) {
		removeFolder(dir)

		url := testutil.ServerServe(t, dir)
		constructCtx := sstauth.ContextWithAuthProvider(context.TODO(), testutil.TestProviderInstance)
		repo, err := sst.OpenRemoteRepository(constructCtx, url, transportCreds)

		if err != nil {
			panic(err)
		}
		defer repo.Close()

		stage := repo.OpenStage(sst.DefaultTriplexMode)
		ng := stage.CreateNamedGraph("")
		require.NotNil(t, ng)

		// Commit once
		_, modifiedDSIDs, err := stage.Commit(constructCtx, "Initial commit", sst.DefaultBranch)
		require.NoError(t, err)
		assert.Equal(t, 1, len(modifiedDSIDs)) // Ensure one DatasetID was modified

		// Validate statistics from RepositoryInfo
		stats, err := repo.Info(constructCtx, "")
		require.NoError(t, err)

		assert.Equal(t, 1, stats.NumberOfDatasets)
		assert.Equal(t, 1, stats.NumberOfDatasetRevisions)
		assert.Equal(t, 1, stats.NumberOfNamedGraphRevisions)
		assert.Equal(t, 1, stats.NumberOfCommits)
	})
}

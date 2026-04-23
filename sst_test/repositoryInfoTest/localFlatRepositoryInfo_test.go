// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"context"
	"testing"

	"git.semanticstep.net/x/sst/sst"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestLocalFlatRepositoryAddSingleDataset(t *testing.T) {
	// This test validates repository statistics after adding a single dataset.
	dir := "./testlocalflatRepo_SingleDataset"
	removeFolder(dir)
	defer removeFolder(dir)

	// Open the repo
	repo, err := sst.CreateLocalFlatRepository(dir)
	assert.NoError(t, err, "Failed to open repository")
	defer repo.Close()

	// Open a st for modifications
	st := repo.OpenStage(sst.DefaultTriplexMode)

	// Create a single named graph and ensure no errors
	_ = st.CreateNamedGraph("")
	assert.NoError(t, err, "Failed to create named graph")

	// Commit the changes
	_, modifiedDSIDs, err := st.Commit(context.TODO(), "First commit", sst.DefaultBranch)
	assert.NoError(t, err, "Failed to commit changes")
	assert.Equal(t, len(modifiedDSIDs), 1, "One dataset should be modified after commit")

	// Fetch updated repository statistics
	stats, err := repo.Info(context.TODO(), "")
	assert.NoError(t, err, "Failed to fetch repository statistics")

	// Validate statistics after adding one dataset
	assert.Equal(t, stats.NumberOfDatasets, 1, "NumberOfDatasets should be 1 after adding a dataset")
	assert.Equal(t, stats.NumberOfDatasetRevisions, 1, "NumberOfDatasetRevisions should equal the number of datasets")
	assert.Equal(t, stats.NumberOfNamedGraphRevisions, 1, "NumberOfNamedGraphRevisions should equal the number of datasets")
}

func TestLocalFlatRepositoryAddMultipleDatasets(t *testing.T) {
	// This test validates repository statistics after adding multiple datasets.
	dir := "./testlocalflatRepo_MultipleDatasets"
	removeFolder(dir)
	defer removeFolder(dir)

	// Open the repo
	repo, err := sst.CreateLocalFlatRepository(dir)
	assert.NoError(t, err, "Failed to open repository")
	defer repo.Close()

	// Open a st for modifications
	st := repo.OpenStage(sst.DefaultTriplexMode)

	// Create two named graphs and ensure no errors
	datasetID1 := uuid.Must(uuid.NewRandom())
	_ = st.CreateNamedGraph(sst.IRI(datasetID1.URN()))
	assert.NoError(t, err, "Failed to create the first named graph")

	datasetID2 := uuid.Must(uuid.NewRandom())
	_ = st.CreateNamedGraph(sst.IRI(datasetID2.URN()))
	assert.NoError(t, err, "Failed to create the second named graph")

	// Commit the changes
	_, modifiedDSIDs, err := st.Commit(context.TODO(), "Add multiple datasets", sst.DefaultBranch)
	assert.NoError(t, err, "Failed to commit changes")
	assert.Equal(t, len(modifiedDSIDs), 2, "Two datasets should be modified after commit")

	// Fetch updated repository statistics
	stats, err := repo.Info(context.TODO(), "")
	assert.NoError(t, err, "Failed to fetch repository statistics")

	// Validate statistics after adding two datasets
	assert.Equal(t, stats.NumberOfDatasets, 2, "NumberOfDatasets should be 2 after adding two datasets")
	assert.Equal(t, stats.NumberOfDatasetRevisions, 2, "NumberOfDatasetRevisions should equal the number of datasets")
	assert.Equal(t, stats.NumberOfNamedGraphRevisions, 2, "NumberOfNamedGraphRevisions should equal the number of datasets")
}

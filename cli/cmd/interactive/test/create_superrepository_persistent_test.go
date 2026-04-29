// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/semanticstep/sst-core/defaultderive"
	"github.com/semanticstep/sst-core/sst"
	_ "github.com/semanticstep/sst-core/vocabularies/dict"
	"github.com/semanticstep/sst-core/vocabularies/lci"
	"github.com/semanticstep/sst-core/vocabularies/rdf"
	"github.com/semanticstep/sst-core/vocabularies/rdfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// removeFolder is a helper function to clean up test directories
func removeFolder(dir string) {
	// check and delete old dir
	if _, err := os.Stat(dir); err == nil {
		err := os.RemoveAll(dir)
		if err != nil {
			fmt.Printf("Failed to delete %s: %s\n", dir, err)
		} else {
			fmt.Printf("%s has been deleted successfully\n", dir)
		}
	} else if os.IsNotExist(err) {
		fmt.Println(dir + " - This file or directory does not exist.")
	} else {
		fmt.Printf("Error checking if file exists: %s\n", err)
	}
}

// Test_CreateLocalSuperRepository_Persistent creates a SuperRepository that persists after the test
// This test does NOT clean up the directory, so the SuperRepository remains for manual inspection
func Test_CreateLocalSuperRepository_Persistent(t *testing.T) {
	// Use a fixed directory name that won't be cleaned up - directly in the repository directory
	dir := "./MySuperRepository"

	// Clean up only if it exists (optional - comment out if you want to keep existing data)
	removeFolder(dir)

	fmt.Printf("\n=== Creating SuperRepository at: %s ===\n", dir)

	// Create SuperRepository
	superRepo, err := sst.NewLocalSuperRepository(dir)
	require.NoError(t, err, "Creating SuperRepository should succeed")
	assert.NotNil(t, superRepo, "SuperRepository should not be nil")

	// Register index handler
	err = superRepo.RegisterIndexHandler(defaultderive.DeriveInfo())
	require.NoError(t, err, "Registering index handler should succeed")

	// List repositories (should include auto-created "default")
	ctx := context.TODO()
	repos, err := superRepo.List(ctx)
	require.NoError(t, err, "Listing repositories should succeed")
	assert.Contains(t, repos, "default", "Should contain default repository")
	fmt.Printf("Initial repositories: %v\n", repos)

	// Create a new repository
	repo, err := superRepo.Create(ctx, "myrepo")
	require.NoError(t, err, "Creating repository should succeed")
	assert.NotNil(t, repo, "Repository should not be nil")
	defer repo.Close()

	// Use repository - create some data
	stage := repo.OpenStage(sst.DefaultTriplexMode)
	graph := stage.CreateNamedGraph("")
	mainNode := graph.CreateIRINode("main")
	mainNode.AddStatement(rdf.Type, lci.Organization)
	mainNode.AddStatement(rdfs.Label, sst.String("My Company"))

	// Commit
	commitHash, modifiedDSIDs, err := stage.Commit(ctx, "Initial commit", sst.DefaultBranch)
	require.NoError(t, err, "Commit should succeed")
	assert.NotNil(t, commitHash, "Commit hash should not be nil")
	assert.Len(t, modifiedDSIDs, 1, "Should have one modified dataset")
	fmt.Printf("Commit succeeded, hash: %s\n", commitHash.String())

	// List all repositories again
	repos, err = superRepo.List(ctx)
	require.NoError(t, err)
	fmt.Printf("All repositories: %v\n", repos)
	assert.Contains(t, repos, "default")
	assert.Contains(t, repos, "myrepo")

	// Create another repository
	repo2, err := superRepo.Create(ctx, "repo2")
	require.NoError(t, err)
	defer repo2.Close()
	fmt.Printf("Created repository: repo2\n")

	// Final list
	repos, err = superRepo.List(ctx)
	require.NoError(t, err)
	fmt.Printf("Final repository list: %v\n", repos)

	fmt.Printf("\n=== SuperRepository created successfully! ===\n")
	fmt.Printf("Location: %s\n", dir)
	fmt.Printf("Repositories: %v\n", repos)
	fmt.Printf("\nYou can now use this SuperRepository in the CLI:\n")
	fmt.Printf("  sst > openlocalsuperrepository %s\n", dir)
	fmt.Printf("\nNote: This test does NOT clean up the directory.\n")
	fmt.Printf("      The SuperRepository will persist for manual inspection.\n")
}

// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"git.semanticstep.net/x/sst/cli/cmd/utils"
	"git.semanticstep.net/x/sst/sst"
	"github.com/blevesearch/bleve/v2"
)

func handleCommits(datasetAlias, command string, args []string) {
	// check if dataset exists
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}

	authCtx := utils.GetAuthContext(dataset.Repository(), interactiveConfig.AuthContexts)

	switch command {
	case "listcommits":
		var showDetails bool
		for _, arg := range args {
			if arg == "--details" {
				showDetails = true
				break
			}
		}

		utils.MuteLog()
		commits, err := dataset.LeafCommits(authCtx)
		utils.RestoreLog()
		if err != nil {
			fmt.Printf("Error retrieving leaf commits: %v\n", err)
			return
		}

		if len(commits) == 0 {
			fmt.Println("No commits found in this dataset.")
			return
		}

		visited := make(map[string]bool)

		for _, commit := range commits {
			if showDetails {
				utils.ListCommitHistoryDetailed(dataset, commit, visited, authCtx)
			} else {
				utils.ListCommitHistoryHashOnly(dataset, commit, visited, authCtx)
			}
		}
		return

	case "commitsbyhash":
		if len(args) == 0 {
			fmt.Println("Error: Missing commit hash.")
			return
		}
		hashInput := args[0]

		hashBytes, err := sst.StringToHash(hashInput)
		if err != nil {
			fmt.Printf("Error: Invalid commit hash: %v\n", err)
			return
		}

		commitDetails, err := dataset.CommitDetailsByHash(authCtx, hashBytes)
		if err != nil {
			fmt.Printf("Error retrieving commit details: %v\n", err)
			return
		}

		utils.PrintCommitDetails(commitDetails)
		return

	case "commitsbybranch":
		if len(args) == 0 {
			fmt.Println("Error: Missing branch name.")
			return
		}
		branch := args[0]

		// Catch potential panic (e.g. gRPC returning panic instead of error)
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "Error retrieving branch '%s': %v\n", branch, r)
			}
		}()

		commitDetails, err := dataset.CommitDetailsByBranch(authCtx, branch)
		if err != nil {
			if strings.Contains(err.Error(), "branch not found") {
				fmt.Printf("Error: Branch '%s' does not exist.\n", branch)
			} else {
				fmt.Printf("Error retrieving commit details for branch '%s': %v\n", branch, err)
			}
			return
		}

		utils.PrintCommitDetails(commitDetails)
		return

	default:
		fmt.Println("Unknown commit command. Available commands: listcommits, commitsbyhash <hash>, commitsbybranch <branch>")
	}
}

func handleCheckoutCommit(datasetAlias string, args []string) {
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}

	if len(args) < 1 {
		fmt.Println("Error: Missing arguments. Use '<dataset-alias>.CheckoutCommit <commit-id>'.")
		return
	}

	commitID := args[0]
	aliasResult, err := utils.GetAlias(args, "stage")
	if err != nil {
		fmt.Println(err)
		return
	}
	stageAlias := aliasResult.Alias

	// Use defer to confirm alias generation only on success
	success := false
	defer func() {
		if success {
			aliasResult.Confirm()
		}
	}()

	hash, err := sst.StringToHash(commitID)
	if err != nil {
		fmt.Printf("Error: Invalid commit hash: %v\n", err)
		return
	}

	if _, exists := interactiveConfig.Stages[stageAlias]; exists {
		fmt.Printf("Error: Stage alias '%s' already exists.\n", stageAlias)
		return
	}

	authCtx := utils.GetAuthContext(dataset.Repository(), interactiveConfig.AuthContexts)

	utils.MuteLog()
	stage, err := dataset.CheckoutCommit(authCtx, hash, sst.DefaultTriplexMode)
	utils.RestoreLog()
	if err != nil {
		fmt.Printf("Error: Failed to checkout commit %s: %v\n", commitID, err)
		return
	}

	// Success! Set flag so defer will confirm
	success = true

	interactiveConfig.Stages[stageAlias] = stage
	interactiveConfig.StageAliases = append(interactiveConfig.StageAliases, stageAlias)

	interactiveConfig.StageCommits[stageAlias] = hash

	fmt.Printf("CheckoutCommit successful. Stage '%s' created for commit %s.\n", stageAlias, commitID)
}

func handleCheckoutBranch(datasetAlias string, args []string) {
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}

	if len(args) < 1 {
		fmt.Println("Error: Missing arguments. Use '<dataset-alias>.CheckoutBranch <branchName>'.")
		return
	}

	branchName := args[0]
	aliasResult, err := utils.GetAlias(args, "stage")
	if err != nil {
		fmt.Println(err)
		return
	}
	stageAlias := aliasResult.Alias

	// Use defer to confirm alias generation only on success
	success := false
	defer func() {
		if success {
			aliasResult.Confirm()
		}
	}()

	if _, exists := interactiveConfig.Stages[stageAlias]; exists {
		fmt.Printf("Error: Stage alias '%s' already exists.\n", stageAlias)
		return
	}

	authCtx := utils.GetAuthContext(dataset.Repository(), interactiveConfig.AuthContexts)

	utils.MuteLog()
	stage, err := dataset.CheckoutBranch(authCtx, branchName, sst.DefaultTriplexMode)
	utils.RestoreLog()

	if err != nil {
		fmt.Printf("Error: Failed to checkout branch '%s': %v\n", branchName, err)
		return
	}

	// Success! Set flag so defer will confirm
	success = true

	interactiveConfig.Stages[stageAlias] = stage
	interactiveConfig.StageAliases = append(interactiveConfig.StageAliases, stageAlias)

	interactiveConfig.StageBranches[stageAlias] = branchName

	fmt.Printf("CheckoutBranch successful. Stage '%s' created for branch '%s'.\n", stageAlias, branchName)
}

func handleListField(alias string) {
	repo, exists := interactiveConfig.Repositories[alias]
	if !exists {
		fmt.Printf("Error: Repository alias '%s' not found.\n", alias)
		return
	}

	authCtx := utils.GetAuthContext(repo, interactiveConfig.AuthContexts)

	bleveIndex := repo.Bleve()
	if bleveIndex == nil {
		fmt.Printf("Error: Repository '%s' does not have an index.\n", alias)
		return
	}

	req := bleve.NewSearchRequest(bleve.NewMatchAllQuery())
	req.Size = 1000
	req.Fields = []string{"*"}

	result, err := bleveIndex.SearchInContext(authCtx, req)
	if err != nil {
		fmt.Printf("Error listing fields: %v\n", err)
		return
	}

	fieldSet := map[string]struct{}{}
	for _, hit := range result.Hits {
		for field := range hit.Fields {
			fieldSet[field] = struct{}{}
		}
	}

	if len(fieldSet) == 0 {
		fmt.Println("No indexed fields found.")
		return
	}

	fmt.Println("Available searchable fields:")
	for field := range fieldSet {
		fmt.Printf(" - %s\n", field)
	}
}

func handleDiff(datasetAlias string, args []string) {
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}
	if len(args) != 2 {
		fmt.Println("Usage: <dataset-alias>.diff <NG-Revision-Hash1> <NG-Revision-Hash2>")
		return
	}

	hash1, err := sst.StringToHash(args[0])
	if err != nil {
		fmt.Printf("Invalid hash1: %v\n", err)
		return
	}
	hash2, err := sst.StringToHash(args[1])
	if err != nil {
		fmt.Printf("Invalid hash2: %v\n", err)
		return
	}

	repo := dataset.Repository()
	ctx := utils.GetAuthContext(repo, interactiveConfig.AuthContexts)

	tris, err := utils.SstDiffTriples(ctx, repo, hash1, hash2, true)
	if err != nil {
		fmt.Printf("Error while computing diff: %v\n", err)
		return
	}
	fmt.Println("DiffTriples:")
	utils.PrintDiffTriples(tris)
}

// handleHistory shows the commit history graph for the given dataset alias.
// Usage: <dataset-alias>.history
func handleHistory(datasetAlias string, args []string) {
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}
	if len(args) != 0 {
		fmt.Println("Usage: <dataset-alias>.history")
		return
	}

	repo := dataset.Repository()
	if repo == nil {
		fmt.Printf("Error: Dataset '%s' is not linked to a repository.\n", datasetAlias)
		return
	}

	ctx := utils.GetAuthContext(repo, interactiveConfig.AuthContexts)
	ngIRI := dataset.IRI()

	_ = queryHistoryBranches(ngIRI, repo, ctx)
}

// handleBranches shows all branches for the given dataset alias.
// Usage: <dataset-alias>.branches
func handleBranches(datasetAlias string, args []string) {
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}
	if len(args) != 0 {
		fmt.Println("Usage: <dataset-alias>.branches")
		return
	}

	authCtx := utils.GetAuthContext(dataset.Repository(), interactiveConfig.AuthContexts)

	utils.MuteLog()
	branches, err := dataset.Branches(authCtx)
	utils.RestoreLog()

	if err != nil {
		fmt.Printf("Error retrieving branches: %v\n", err)
		return
	}

	if len(branches) == 0 {
		fmt.Println("No branches found in this dataset.")
		return
	}

	// Sort branches by name for consistent output
	var branchNames []string
	for branchName := range branches {
		branchNames = append(branchNames, branchName)
	}
	sort.Strings(branchNames)

	fmt.Println("Branches:")
	for _, branchName := range branchNames {
		fmt.Printf("  %s: %s\n", branchName, branches[branchName])
	}
}

// handleLeafCommits shows all leaf commits for the given dataset alias.
// Usage: <dataset-alias>.leafcommits
func handleLeafCommits(datasetAlias string, args []string) {
	dataset, exists := interactiveConfig.Datasets[datasetAlias]
	if !exists {
		fmt.Printf("Error: Dataset alias '%s' not found.\n", datasetAlias)
		return
	}
	if len(args) != 0 {
		fmt.Println("Usage: <dataset-alias>.leafcommits")
		return
	}

	authCtx := utils.GetAuthContext(dataset.Repository(), interactiveConfig.AuthContexts)

	utils.MuteLog()
	leafCommits, err := dataset.LeafCommits(authCtx)
	utils.RestoreLog()
	if err != nil {
		fmt.Printf("Error retrieving leaf commits: %v\n", err)
		return
	}

	if len(leafCommits) == 0 {
		fmt.Println("No leaf commits found in this dataset.")
		return
	}

	fmt.Println("Leaf Commits:")
	for _, commit := range leafCommits {
		fmt.Printf("  %s\n", commit)
	}
}

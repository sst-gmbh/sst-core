// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/chzyer/readline"
	"github.com/spf13/cobra"
)

// interactiveCmd starts an interactive mode
var InteractiveCmd = &cobra.Command{
	Use:   "interactive",
	Short: "Start an interactive mode for repository operations",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Entering SST CLI tool in interactive mode. Type 'q' to quit, 'help' to see available commands.")
		StartInteractiveMode()
	},
}

// StartInteractiveMode starts the interactive command loop
func StartInteractiveMode() {
	// Get user home directory for history file
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
	historyFile := filepath.Join(homeDir, ".sst_cli_history")

	// Create auto-completer
	completer := NewCompleter()

	// Configure readline with history support and auto-completion
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "sst > ",
		HistoryFile:     historyFile,
		HistoryLimit:    1000, // Store up to 1000 commands
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Printf("Error initializing readline: %v\n", err)
		// Fallback to basic input if readline fails
		startBasicInteractiveMode()
		return
	}
	defer rl.Close()

	for {
		// Read user input with readline (supports history navigation)
		line, err := rl.Readline()
		if err != nil {
			// Handle EOF (Ctrl+D) and interrupt (Ctrl+C)
			if err == readline.ErrInterrupt {
				fmt.Println("\nUse 'q' to quit.")
				continue
			}
			if err == io.EOF {
				fmt.Println("\nExiting SST CLI tool.")
				break
			}
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		input := strings.TrimSpace(line)

		// Skip empty lines
		if input == "" {
			continue
		}

		// Exit interactive mode
		if input == "q" {
			fmt.Println("Exiting SST CLI tool.")
			break
		}

		// Save command to history
		rl.SaveHistory(input)

		// Safe command execution with panic recovery
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] Unexpected internal error: %v\n", r)
				}
			}()
			handleInteractiveCommand(input)
		}()
	}
}

// startBasicInteractiveMode is a fallback mode without readline features
func startBasicInteractiveMode() {
	fmt.Println("Warning: Command history is not available. Using basic input mode.")
	fmt.Println("Entering SST CLI tool in interactive mode. Type 'q' to quit, 'help' to see available commands.")

	reader := bufio.NewReader(os.Stdin)
	// Simple input loop without history
	for {
		fmt.Print("sst > ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}
		input = strings.TrimSpace(input)

		if input == "q" {
			fmt.Println("Exiting SST CLI tool.")
			break
		}

		if input == "" {
			continue
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] Unexpected internal error: %v\n", r)
				}
			}()
			handleInteractiveCommand(input)
		}()
	}
}

// handleInteractiveCommand handles input commands in interactive mode
func handleInteractiveCommand(input string) {
	args := strings.Fields(input)
	if len(args) == 0 {
		// fmt.Println("No command provided.")
		return
	}

	switch strings.ToLower(args[0]) {
	case "help", "-h", "-help", "--h", "--help":
		displayHelp(os.Stdout)
		return
	}

	if isAliasCommandFormat(input) {
		// split the input into repoAlias and command (e.g., r1.DatasetIDs -> r1, DatasetIDs)
		parts := strings.SplitN(input, ".", 2)
		if len(parts) == 2 {
			alias := parts[0]
			commandArgs := strings.Fields(parts[1]) // split the command part into args

			if len(commandArgs) == 0 {
				fmt.Println("Missing command after alias. Usage: <alias>.<command> [args...]")
				return
			}

			command := strings.ToLower(commandArgs[0]) // get the command from the first arg
			args := commandArgs[1:]

			switch command {
			// Repository.info & Stage.info & NamedGraph.info
			case "info":
				handleInfo(alias)

			case "close":
				handleClose(alias)
			case "superrepository":
				handleRepositorySuperRepository(alias)
			case "datasets":
				handleDatasets(alias)
			case "dataset":
				handleDataset(alias, args)
			case "query":
				handleQuery(alias, args)
			// sst > r1.query 0e0d7c95-fa76-425f-81c8-30932338f83d
			// 1 matches, showing 1 through 1, took 106.017µs
			//   1. 0e0d7c95-fa76-425f-81c8-30932338f83d (score: 1.000000)
			//     id: cs6
			case "queryuuid":
				handleQueryUUID(alias, args)
			case "listfield":
				handleListField(alias)
			case "log":
				handleLog(alias, args)
			case "commitinfo":
				handleCommitInfo(alias, args)
			case "dump":
				handleDump(alias, args)
			case "commitdiff":
				handleCommitDiff(alias, args)
			//repo.openstage: create an empty stage
			case "openstage":
				handleOpenStage(alias, args)

			// Document storage inside the file system of an SST repository
			case "documentset":
				handleDocumentSet(alias, args)
			case "documentget":
				handleDocument(alias, args)
			case "documentinfo":
				handleDocumentInfo(alias, args)
			case "documents":
				handleDocuments(alias)
			case "documentdelete":
				handleDocumentDelete(alias, args)

			case "extractsstfile":
				handleExtractSstFile(alias, args)

			// Example: r1.syncfrom r2                                      // copy whole repo, all branches
			// Example: r1.syncfrom r2 master                               // copy whole repo, master branch only
			// Example: r1.syncfrom r2 master dataset-uuid-1 dataset-uuid-2 // copy only specific datasets in the master branch
			// Example: r1.syncfrom r2 * dataset-uuid-1 dataset-uuid-2      // copy only specific datasets, all branches
			case "syncfrom":
				handleSyncFrom(alias, args)
			case "clone":
				handleClone(alias, args)

			// Dataset.listCommits, Dataset.CommitsByHash, Dataset.CommitsByBranch
			case "listcommits", "commitsbyhash", "commitsbybranch":
				handleCommits(alias, command, args)

			// Dataset.CheckoutCommit <hash> and Dataset.CheckoutBranch <hash> to get a Stage: s1, s2, s3...
			case "checkoutcommit":
				handleCheckoutCommit(alias, args)
			case "checkoutbranch":
				handleCheckoutBranch(alias, args)

			case "diff":
				handleDiff(alias, args)

			case "history":
				handleHistory(alias, args)

			// Dataset.Branches and Dataset.LeafCommits
			case "branches":
				handleBranches(alias, args)
			case "leafcommits":
				handleLeafCommits(alias, args)

			// List the content of a Stage(e.g. of s1): Stage.NamedGraphs(), Stage.ReferenceNamedGraphs()
			case "namedgraphs":
				handleListNamedGraphs(alias)
			case "referencednamedgraphs":
				handleListReferencedNamedGraphs(alias)
			// Stage.GetNamedGraphByIRI
			case "namedgraph":
				handleNamedGraph(alias, args)
			// Stage.moveandmerge
			case "moveandmerge":
				handleMoveAndMerge(alias, args)
			//Stage.commit message branchName
			case "commit":
				handleStageCommit(alias, args)
			// Stage.validate
			case "validate":
				handleStageValidate(alias, args)
			case "trig":
				handleStageTriG(alias, args)

			// List the content of a NamedGraph.ForIRINodes(), NamedGraph.ForAllIbNodes(), NamedGraph.ForBlankNode(): n1, n2, n3...
			case "foririnodes":
				handleListForIRINode(alias)
			case "forallibnodes":
				handleListForAllIBNodes(alias)
			case "forblanknodes":
				handleListForBlankNode(alias)
			// NamedGraph.rdfwrite/Stage.rdfwrite
			case "rdfwrite":
				if _, isStage := interactiveConfig.Stages[alias]; isStage {
					handleStageRdfWrite(alias, args)
					return
				}
				handleRdfWrite(alias, args)
			case "exportap242xml":
				handleNamedGraphExportAP242XML(alias, args)
			case "ttl":
				handleTtl(alias)
			// NamedGraph.GetIRINodeByFragment, NamedGraph.GetBlankNodeByFragment
			case "getirinodebyfragment":
				handleGetIRINodeByFragment(alias, args)
			case "getblanknodebyfragment":
				handleGetBlankNodeByFragment(alias, args)

			// List triples stored in an ibnode: IBNode.ForAll()
			case "forall":
				handleForAllTriples(alias)

			// SuperRepository commands
			case "get":
				// Check if alias is a SuperRepository
				if _, exists := interactiveConfig.SuperRepositories[alias]; exists {
					handleSuperRepositoryGet(alias, args)
				} else {
					fmt.Printf("Error: Alias '%s' is not a SuperRepository.\n", alias)
				}
			case "create":
				// Check if alias is a SuperRepository
				if _, exists := interactiveConfig.SuperRepositories[alias]; exists {
					handleSuperRepositoryCreate(alias, args)
				} else {
					fmt.Printf("Error: Alias '%s' is not a SuperRepository.\n", alias)
				}
			case "delete":
				// Check if alias is a SuperRepository
				if _, exists := interactiveConfig.SuperRepositories[alias]; exists {
					handleSuperRepositoryDelete(alias, args)
				} else {
					fmt.Printf("Error: Alias '%s' is not a SuperRepository.\n", alias)
				}
			case "list":
				// Check if alias is a SuperRepository
				if _, exists := interactiveConfig.SuperRepositories[alias]; exists {
					handleSuperRepositoryList(alias, args)
				} else {
					fmt.Printf("Error: Alias '%s' is not a SuperRepository.\n", alias)
				}

			default:
				fmt.Printf("Unknown command '%s'. Type 'help' for available commands.\n", command)
			}
		} else {
			fmt.Println("Invalid command format. Use '<repo-alias>.<command>', '<dataset-alias>.<command>', etc. Type 'help' for available commands.")
		}
	} else {
		// if there is not a repoAlias.command format, handle other commands
		switch strings.ToLower(args[0]) {
		case "q":
			fmt.Println("Exiting...")
			return
		case "open":
			handleOpen(args[1:])
		case "openlocalrepository":
			handleOpen(append([]string{"local"}, args[1:]...))
		case "openlocalflatrepository":
			handleOpenLocalFlatRepository(args[1:])
		case "openremoterepository":
			handleOpen(append([]string{"remote"}, args[1:]...))
		case "openlocalsuperrepository":
			handleOpenLocalSuperRepository(args[1:])
		case "openremotesuperrepository":
			handleOpenRemoteSuperRepository(args[1:])
		case "status":
			handleStatus()
		case "rdfread":
			handleRDFRead(args[1:])

		// run command line interface for converting files
		// Example:
		// go run cmd/converter/main.go -format=xml-to-sst -input=step/testdata/ewh/HarnessExample_Flat.xml
		// go run cmd/converter/main.go -format=sst-to-ap242 -input=step/testdata/ewh/HarnessExample_Flat.ttl
		// go run cmd/converter/main.go -format=xml-to-ap242 -input=step/testdata/ewh/HarnessExample_Flat.xml
		// go run cmd/converter/main.go -format=step-to-ap242 -input=step/p21/testdata/as1-ec-214.stp
		// go run cmd/converter/main.go -format=step-to-sst -input=step/p21/testdata/ts1-oc-242.stp.Z
		// go run cmd/converter/main.go -format=json-to-sst -input=sstjson/testdata/test1.json
		// go run cmd/converter/main.go -format=sst-to-json -input=sstjson/testdata/test2.ttl

		// importap242xml cli/testfile/as1-ec-214-ap242.xml
		case "importap242xml":
			handleImportAP242XML(args[1:])
		// importp21 cli/testfile/as1-ec-214.stp
		case "importp21":
			handleImportP21(args[1:])
		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

func isAliasCommandFormat(input string) bool {
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9]+(\.[a-zA-Z0-9]+)+(\s+.+)?$`, input)
	return matched
}

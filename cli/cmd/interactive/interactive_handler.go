// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/semanticstep/sst-core/cli/cmd/utils"
	"github.com/semanticstep/sst-core/sst"
	"github.com/semanticstep/sst-core/sstauth"
	"github.com/semanticstep/sst-core/step/ap242xmlexport"
	"github.com/semanticstep/sst-core/step/ap242xmlimport"
	"github.com/semanticstep/sst-core/step/p21"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// displayOpenRepositories prints the list of currently opened repositories
func displayOpenRepositories() {
	if len(interactiveConfig.RepositoryAliases) == 0 {
		fmt.Println("No repositories are currently open.")
		return
	}
	fmt.Print("Currently opened repositories: ")
	first := true
	for alias := range interactiveConfig.Repositories {
		if !first {
			fmt.Print(", ")
		}
		fmt.Print(alias)
		first = false
	}
	fmt.Println()
}

// displayOpenDatasets prints the list of currently opened datasets
func displayOpenDatasets() {
	if len(interactiveConfig.DatasetAliases) == 0 {
		fmt.Println("No datasets are currently open.")
		return
	}
	fmt.Print("Currently opened datasets: ")
	first := true
	for alias := range interactiveConfig.Datasets {
		if !first {
			fmt.Print(", ")
		}
		fmt.Print(alias)
		first = false
	}
	fmt.Println()
}

func handleOpen(args []string) {
	reader := bufio.NewReader(os.Stdin)
	var repoType, repoPath, repoURL, alias string

	// Ask for local or remote if not provided
	if len(args) == 0 || (args[0] != "local" && args[0] != "remote") {
		fmt.Print("Do you want to open a 'local' or 'remote' repository? ")
		inputType, _ := reader.ReadString('\n')
		repoType = strings.TrimSpace(inputType)
	} else {
		repoType = args[0]
		args = args[1:] // Remove type from args
	}

	// Ask for path or URL
	switch repoType {
	case "local":
		if len(args) == 0 {
			fmt.Print("Enter local repository path: ")
			repoPath, _ = reader.ReadString('\n')
			repoPath = strings.TrimSpace(repoPath)
		} else {
			repoPath = args[0]
			args = args[1:] // Remove path from args
		}
	case "remote":
		if len(args) == 0 {
			fmt.Print("Enter remote repository URL: ")
			repoURL, _ = reader.ReadString('\n')
			repoURL = strings.TrimSpace(repoURL)
		} else {
			repoURL = args[0]
			args = args[1:] // Remove URL from args
		}
	default:
		fmt.Println("Invalid repository type. Use 'local' or 'remote'.")
		return
	}

	aliasResult, err := utils.GetAlias(args, "repository")
	if err != nil {
		fmt.Println(err)
		return
	}
	alias = aliasResult.Alias

	// Use defer to confirm alias generation only on success
	success := false
	defer func() {
		if success {
			aliasResult.Confirm()
		}
	}()

	// Check if alias already exists
	if _, exists := interactiveConfig.Repositories[alias]; exists {
		fmt.Printf("Error: Repository with alias '%s' already exists.\n", alias)
		return
	}

	// Open repository
	var repository sst.Repository

	switch repoType {
	case "local":
		if err := utils.ValidatePath(repoPath); err != nil {
			fmt.Printf("Invalid path: %v\n", err)
			return
		}
		repository, err = sst.OpenLocalRepository(repoPath, "default@semanticstep.net", "default")
		if err != nil {
			fmt.Printf("Error: Unable to open local repository: %s\n", err)
			return
		}

		interactiveConfig.RepositoryLocations[alias] = repoPath
		interactiveConfig.RepositoryTypes[alias] = "local"
	case "remote":
		// transportCreds, err := testutil.TestTransportCreds()
		// if err != nil {
		// 	fmt.Printf("Failed to load TLS credentials: %v\n", err)
		// 	return
		// }
		// constructCtx := auth.ContextWithAuthProvider(context.TODO(), utils.TestProvider)
		// repository, err = sst.OpenRemoteRepository(constructCtx, repoURL, transportCreds)
		// if err != nil {
		// 	fmt.Printf("Error: Unable to open %s repository: %s\n", repoType, err)
		// 	return
		// }
		creds := credentials.NewTLS(nil)
		realProvider := utils.GetRealProvider()
		constructCtx := sstauth.ContextWithAuthProvider(context.TODO(), realProvider)

		utils.MuteLog()
		var panicErr any
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicErr = r
				}
			}()

			repository, err = sst.OpenRemoteRepository(constructCtx, repoURL, grpc.WithTransportCredentials(creds))
		}()
		utils.RestoreLog()
		if panicErr != nil {
			fmt.Printf("Cannot connect to remote repository at '%s'.\n", repoURL)
			fmt.Println("Please check that the URL is correct and your network is available.")
			fmt.Printf("(Technical info: %v)\n", panicErr)
			return
		}
		if err != nil {
			msg, details := utils.ExplainRemoteRepositoryOpenError(repoURL, err)
			fmt.Println(msg)
			if details {
				fmt.Printf("Details: %v\n", err)
			}
			return
		}
		if repository == nil {
			fmt.Printf("Could not open remote repository '%s' (internal error: empty handle).\n", repoURL)
			return
		}

		interactiveConfig.AuthContexts[repository] = constructCtx
		interactiveConfig.RepositoryLocations[alias] = repoURL
		interactiveConfig.RepositoryTypes[alias] = "remote"
	default:
		fmt.Println("Invalid repository type. Use 'local' or 'remote'.")
		return
	}

	// Success! Set flag so defer will confirm
	success = true

	interactiveConfig.Repositories[alias] = repository
	interactiveConfig.RepositoryAliases = append(interactiveConfig.RepositoryAliases, alias)

	// Confirm success
	fmt.Printf("Repository '%s' (%s) opened successfully.\n", alias, repoType)
}

// handleOpenLocalFlatRepository opens a LocalFlat repository (directory of .sst files).
func handleOpenLocalFlatRepository(args []string) {
	var repoPath, alias string

	if len(args) == 0 {
		fmt.Println("Usage: openlocalflatrepository <path> [-a <alias>]")
		return
	}

	repoPath = args[0]

	aliasResult, err := utils.GetAlias(args[1:], "repository")
	if err != nil {
		fmt.Println(err)
		return
	}
	alias = aliasResult.Alias

	success := false
	defer func() {
		if success {
			aliasResult.Confirm()
		}
	}()

	if _, exists := interactiveConfig.Repositories[alias]; exists {
		fmt.Printf("Error: Repository with alias '%s' already exists.\n", alias)
		return
	}

	if err := utils.ValidatePath(repoPath); err != nil {
		fmt.Printf("Invalid path: %v\n", err)
		return
	}

	repository, err := sst.OpenLocalFlatRepository(repoPath)
	if err != nil {
		fmt.Printf("Error: Unable to open local flat repository: %s\n", err)
		return
	}

	success = true

	interactiveConfig.Repositories[alias] = repository
	interactiveConfig.RepositoryLocations[alias] = repoPath
	interactiveConfig.RepositoryTypes[alias] = "localflat"
	interactiveConfig.RepositoryAliases = append(interactiveConfig.RepositoryAliases, alias)

	fmt.Printf("Repository '%s' (local flat) opened successfully.\n", alias)
}

func handleStatus() {
	// Track which resources have been printed to avoid duplicates
	printed := struct {
		superRepositories map[string]bool
		repositories      map[string]bool
		datasets          map[string]bool
		stages            map[string]bool
		namedGraphs       map[string]bool
		ibNodes           map[string]bool
	}{
		superRepositories: make(map[string]bool),
		repositories:      make(map[string]bool),
		datasets:          make(map[string]bool),
		stages:            make(map[string]bool),
		namedGraphs:       make(map[string]bool),
		ibNodes:           make(map[string]bool),
	}

	// Phase 0: Display SuperRepositories with their associated repositories (hierarchical)
	for _, superRepoAlias := range interactiveConfig.SuperRepositoryAliases {
		superRepo, exists := interactiveConfig.SuperRepositories[superRepoAlias]
		if !exists || superRepo == nil {
			fmt.Printf("%s: (nil)\n", superRepoAlias)
			continue
		}

		// Display SuperRepository
		loc := "(unknown location)"
		if v, ok := interactiveConfig.SuperRepositoryLocations[superRepoAlias]; ok && v != "" {
			loc = v
		}
		fmt.Printf("%s: %s (SuperRepository)\n", superRepoAlias, loc)
		printed.superRepositories[superRepoAlias] = true

		// Display repositories belonging to this SuperRepository
		for _, repoAlias := range interactiveConfig.RepositoryAliases {
			repo, exists := interactiveConfig.Repositories[repoAlias]
			if !exists || repo == nil {
				continue
			}
			if repo.SuperRepository() == superRepo {
				printed.repositories[repoAlias] = true
				repoLoc := "(unknown location)"
				if v, ok := interactiveConfig.RepositoryLocations[repoAlias]; ok && v != "" {
					repoLoc = v
				}
				fmt.Printf("  %s: %s\n", repoAlias, repoLoc)
			}
		}
	}

	// Phase 1: Display repositories with their associated resources (hierarchical)
	for _, repoAlias := range interactiveConfig.RepositoryAliases {
		if printed.repositories[repoAlias] {
			continue // Already displayed under a SuperRepository
		}
		repo, exists := interactiveConfig.Repositories[repoAlias]
		if !exists || repo == nil {
			fmt.Printf("%s: (nil)\n", repoAlias)
			continue
		}

		// Display repository
		loc := "(unknown location)"
		if v, ok := interactiveConfig.RepositoryLocations[repoAlias]; ok && v != "" {
			loc = v
		}
		fmt.Printf("%s: %s\n", repoAlias, loc)

		// Display datasets belonging to this repository
		for _, dsAlias := range interactiveConfig.DatasetAliases {
			ds, exists := interactiveConfig.Datasets[dsAlias]
			if exists && ds != nil && ds.Repository() == repo {
				fmt.Printf("  %s: %s\n", dsAlias, ds.IRI())
				printed.datasets[dsAlias] = true
			}
		}

		// Display stages belonging to this repository
		for _, stAlias := range interactiveConfig.StageAliases {
			st, exists := interactiveConfig.Stages[stAlias]
			if !exists || st == nil {
				continue
			}
			// Only show stages that are linked to this specific repository
			if st.Repository() == repo {
				fmt.Printf("  %s: %s\n", stAlias, stageRevisionSuffix(stAlias))
				printed.stages[stAlias] = true

				// Display namedgraphs in this stage
				for _, ngAlias := range interactiveConfig.NamedGraphAliases {
					ng, exists := interactiveConfig.NamedGraphs[ngAlias]
					if !exists || ng == nil {
						continue
					}
					if st.NamedGraph(ng.IRI()) == nil {
						continue
					}
					printed.namedGraphs[ngAlias] = true
					fmt.Printf("    %s: %s\n", ngAlias, ngRevisionSuffix(ng))

					// Display ibnodes in this namedgraph
					for _, nodeAlias := range interactiveConfig.IBNodeAliases {
						n, exists := interactiveConfig.IBNodes[nodeAlias]
						if !exists || n == nil {
							continue
						}
						if nodeBelongsToNamedGraph(n, ng) {
							printed.ibNodes[nodeAlias] = true
							fmt.Printf("      %s: %s\n", nodeAlias, getNodeDisplayString(n))
						}
					}
				}
			}
		}
	}

	// Phase 2: Display orphaned stages (without repository or not linked to any opened repository)
	for _, stAlias := range interactiveConfig.StageAliases {
		if printed.stages[stAlias] {
			continue
		}
		st, exists := interactiveConfig.Stages[stAlias]
		if !exists || st == nil {
			// Stage alias exists but stage is nil
			fmt.Printf("%s: %s (nil stage)\n", stAlias, stageRevisionSuffix(stAlias))
			continue
		}

		// Check if stage is linked to any opened repository
		stageRepo := st.Repository()
		hasOpenedRepo := false
		if stageRepo != nil {
			for _, repo := range interactiveConfig.Repositories {
				if repo == stageRepo {
					hasOpenedRepo = true
					break
				}
			}
		}

		// Display stage if it's not linked to any opened repository
		if stageRepo == nil || !hasOpenedRepo {
			suffix := stageRevisionSuffix(stAlias)
			if stageRepo == nil {
				suffix += " (no repository)"
			} else {
				suffix += " (repository not opened)"
			}
			fmt.Printf("%s: %s\n", stAlias, suffix)

			// Display namedgraphs in this orphaned stage
			for _, ngAlias := range interactiveConfig.NamedGraphAliases {
				if printed.namedGraphs[ngAlias] {
					continue
				}
				ng, exists := interactiveConfig.NamedGraphs[ngAlias]
				if !exists || ng == nil {
					continue
				}
				if st.NamedGraph(ng.IRI()) == nil {
					continue
				}
				printed.namedGraphs[ngAlias] = true
				fmt.Printf("  %s: %s\n", ngAlias, ngRevisionSuffix(ng))

				// Display ibnodes in this namedgraph
				for _, nodeAlias := range interactiveConfig.IBNodeAliases {
					if printed.ibNodes[nodeAlias] {
						continue
					}
					n, exists := interactiveConfig.IBNodes[nodeAlias]
					if !exists || n == nil {
						continue
					}
					if nodeBelongsToNamedGraph(n, ng) {
						printed.ibNodes[nodeAlias] = true
						fmt.Printf("    %s: %s\n", nodeAlias, getNodeDisplayString(n))
					}
				}
			}
		}
	}

	// Phase 3: Display orphaned datasets (without repository)
	for _, dsAlias := range interactiveConfig.DatasetAliases {
		if printed.datasets[dsAlias] {
			continue
		}
		ds, exists := interactiveConfig.Datasets[dsAlias]
		if exists && ds != nil {
			fmt.Printf("%s: %s (no repository)\n", dsAlias, ds.IRI())
		}
	}

	// Phase 4: Display orphaned namedgraphs (without stage)
	for _, ngAlias := range interactiveConfig.NamedGraphAliases {
		if printed.namedGraphs[ngAlias] {
			continue
		}
		ng, exists := interactiveConfig.NamedGraphs[ngAlias]
		if exists && ng != nil {
			fmt.Printf("%s: %s (no stage)\n", ngAlias, ngRevisionSuffix(ng))

			// Display ibnodes in this orphaned namedgraph
			for _, nodeAlias := range interactiveConfig.IBNodeAliases {
				if printed.ibNodes[nodeAlias] {
					continue
				}
				n, exists := interactiveConfig.IBNodes[nodeAlias]
				if !exists || n == nil {
					continue
				}
				if nodeBelongsToNamedGraph(n, ng) {
					printed.ibNodes[nodeAlias] = true
					fmt.Printf("  %s: %s\n", nodeAlias, getNodeDisplayString(n))
				}
			}
		}
	}

	// Phase 5: Display orphaned ibnodes (without namedgraph)
	for _, nodeAlias := range interactiveConfig.IBNodeAliases {
		if printed.ibNodes[nodeAlias] {
			continue
		}
		n, exists := interactiveConfig.IBNodes[nodeAlias]
		if exists && n != nil {
			fmt.Printf("%s: %s (no namedgraph)\n", nodeAlias, getNodeDisplayString(n))
		}
	}
}

// nodeBelongsToNamedGraph checks if an IBNode belongs to a NamedGraph
func nodeBelongsToNamedGraph(n sst.IBNode, ng sst.NamedGraph) bool {
	if ng == nil {
		return false
	}
	var iriStr string
	func() {
		defer func() { recover() }()
		iriStr = n.IRI().String()
	}()
	if iriStr != "" {
		// IRI node - check by fragment
		return ng.GetIRINodeByFragment(n.Fragment()) != nil
	}
	// Blank node - check by ID
	return ng.GetBlankNodeByID(n.ID()) != nil
}

// getNodeDisplayString returns the display string for an IBNode
func getNodeDisplayString(n sst.IBNode) string {
	var iriStr string
	func() {
		defer func() { recover() }()
		iriStr = n.IRI().String()
	}()
	if iriStr != "" {
		return iriStr
	}
	return n.ID().String()
}

// stageRevisionSuffix returns a suffix string indicating the revision information for a stage.
// It returns "[branch: xxx]" if the stage has a branch, "[commit: abc12345]" if it has a commit hash,
// "[source: filepath]" if it was created from a source file, or "empty stage" otherwise.
func stageRevisionSuffix(stageAlias string) string {
	if b, ok := interactiveConfig.StageBranches[stageAlias]; ok && b != "" {
		return "[branch: " + b + "]"
	}
	if h, ok := interactiveConfig.StageCommits[stageAlias]; ok {
		hs := h.String()
		if len(hs) > 8 {
			hs = hs[:8]
		}
		return "[commit: " + hs + "]"
	}
	if src, ok := interactiveConfig.StageSources[stageAlias]; ok && src != "" {
		return "[source: " + src + "]"
	}
	return "empty stage"
}

// ngRevisionSuffix returns a suffix string for status display: IRI if set, else empty string.
func ngRevisionSuffix(ng sst.NamedGraph) string {
	if s := ng.IRI().String(); s != "" {
		return "[iri: " + s + "]"
	} else {
		return ""
	}
}

func handleRDFRead(args []string) {
	if len(args) < 1 {
		fmt.Println("Error: Missing arguments. Usage: rdfread <file-path> [--format ttl|trig] [-a <stage-alias>]")
		return
	}

	// Determine input file path and optional explicit format.
	// Supported:
	//   rdfread file.ttl
	//   rdfread file.trig
	//   rdfread file --format ttl|trig
	// Alias flags (-a) are handled by utils.GetAlias below.
	var (
		filePath string
		format   = sst.RdfFormatTurtle
	)

	// Find first non-flag token as the file path
	for i := 0; i < len(args); i++ {
		a := strings.TrimSpace(args[i])
		if a == "" {
			continue
		}
		// Skip alias flag and its value
		if a == "-a" && i+1 < len(args) {
			i++
			continue
		}
		// Skip format flag and its value
		if (a == "--format" || a == "-f") && i+1 < len(args) {
			i++
			continue
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		filePath = a
		break
	}

	if filePath == "" {
		fmt.Println("Error: Missing file path. Usage: rdfread <file-path> [--format ttl|trig] [-a <stage-alias>]")
		return
	}

	// Optional explicit format flag
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--format", "-f":
			if i+1 >= len(args) {
				fmt.Println("Error: Missing value for --format. Use: --format ttl|trig")
				return
			}
			switch strings.ToLower(strings.TrimSpace(args[i+1])) {
			case "ttl", "turtle":
				format = sst.RdfFormatTurtle
			case "trig":
				format = sst.RdfFormatTriG
			default:
				fmt.Printf("Error: Unsupported format %q. Supported: ttl, trig\n", args[i+1])
				return
			}
		}
	}

	// If not explicitly set, infer from file extension
	if format == sst.RdfFormatTurtle {
		lower := strings.ToLower(filePath)
		if strings.HasSuffix(lower, ".trig") {
			format = sst.RdfFormatTriG
		}
	}

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

	// Open RDF file (.ttl or .trig)
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error: Failed to open file '%s': %v\n", filePath, err)
		return
	}
	defer func() {
		if e := file.Close(); e != nil {
			log.Printf("Error closing file: %v", e)
		}
	}()

	// Convert RDF (Turtle/TriG) into a new Stage.
	stage, err := sst.RdfRead(bufio.NewReader(file), format, sst.StrictHandler, sst.DefaultTriplexMode)
	if err != nil {
		fmt.Printf("Error: Failed to read RDF file: %v\n", err)
		return
	}

	// Success! Set flag so defer will confirm
	success = true

	interactiveConfig.Stages[stageAlias] = stage
	interactiveConfig.StageAliases = append(interactiveConfig.StageAliases, stageAlias)
	interactiveConfig.StageSources[stageAlias] = filePath

	fmt.Printf("rdfRead successful. Stage '%s' created for file '%s'.\n", stageAlias, filePath)
}

func handleImportAP242XML(args []string) {
	if len(args) < 1 {
		fmt.Println("Error: Missing arguments. Usage: importap242xml <file-path>")
		return
	}
	filePath := args[0]

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

	// Open XML file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error: Failed to open file '%s': %v\n", filePath, err)
		return
	}
	defer func() {
		if e := file.Close(); e != nil {
			log.Printf("Error closing file: %v", e)
		}
	}()

	reader := bufio.NewReader(file)

	// Mute both standard log and ap242xmlimport logger
	// Save original outputs
	originalLogOutput := log.Default().Writer()
	originalAp242LoggerOutput := ap242xmlimport.Logger().Writer()

	// Redirect both to null device
	nullDevice, _ := os.Open(os.DevNull)
	log.SetOutput(nullDevice)
	ap242xmlimport.Logger().SetOutput(nullDevice)

	defer func() {
		// Restore original outputs
		log.SetOutput(originalLogOutput)
		ap242xmlimport.Logger().SetOutput(originalAp242LoggerOutput)
		nullDevice.Close()
	}()

	// Import AP242 XML to SST graph
	graph, err := ap242xmlimport.FromXMLReader(reader)
	if err != nil {
		fmt.Printf("Error: Failed to import AP242 XML: %v\n", err)
		return
	}

	// Get the stage from the graph
	stage := graph.Stage()

	// Success! Set flag so defer will confirm
	success = true

	interactiveConfig.Stages[stageAlias] = stage
	interactiveConfig.StageAliases = append(interactiveConfig.StageAliases, stageAlias)
	interactiveConfig.StageSources[stageAlias] = filePath

	// Create alias for the graph (remove stage alias flag from args first)
	remainingArgs := utils.RemoveAliasFlag(args)
	graphAliasResult, err := utils.GetAlias(remainingArgs, "namedgraph")
	var graphAlias string
	if err != nil {
		fmt.Printf("Warning: Failed to get graph alias: %v\n", err)
	} else {
		graphAlias = graphAliasResult.Alias
		graphSuccess := false
		defer func() {
			if graphSuccess {
				graphAliasResult.Confirm()
			}
		}()

		if _, exists := interactiveConfig.NamedGraphs[graphAlias]; exists {
			fmt.Printf("Warning: NamedGraph alias '%s' already exists, skipping graph alias creation.\n", graphAlias)
		} else {
			graphSuccess = true
			interactiveConfig.NamedGraphs[graphAlias] = graph
			interactiveConfig.NamedGraphAliases = append(interactiveConfig.NamedGraphAliases, graphAlias)
		}
	}

	fmt.Printf("Import AP242 XML successful. Stage '%s' created for file '%s'.\n", stageAlias, filePath)

	if graphAlias != "" {
		if _, exists := interactiveConfig.NamedGraphs[graphAlias]; exists {
			utils.PrintNamedGraphDetails(graphAlias, graph)
		}
	}
}

func handleNamedGraphExportAP242XML(graphAlias string, args []string) {
	graph, exists := interactiveConfig.NamedGraphs[graphAlias]
	if !exists || graph == nil {
		fmt.Printf("Error: NamedGraph alias '%s' not found.\n", graphAlias)
		return
	}
	if len(args) < 1 || strings.TrimSpace(args[0]) == "" {
		fmt.Println("Error: Missing output file. Usage: <namedgraph>.exportap242xml <output-file.xml>")
		return
	}

	outputFile := strings.TrimSpace(args[0])
	if !strings.HasSuffix(strings.ToLower(outputFile), ".xml") {
		outputFile += ".xml"
	}

	// Check if file exists
	if _, err := os.Stat(outputFile); err == nil {
		fmt.Printf("File '%s' already exists. Overwrite? (y/N): ", outputFile)
		var input string
		fmt.Scanln(&input)
		if strings.ToLower(strings.TrimSpace(input)) != "y" {
			fmt.Println("Aborted.")
			return
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error checking file '%s': %v\n", outputFile, err)
		return
	}

	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create file '%s': %v\n", outputFile, err)
		return
	}
	defer func() {
		if e := f.Close(); e != nil {
			log.Printf("Error closing file: %v", e)
		}
	}()

	// Mute log output during export to suppress log output from ap242xmlexport
	// ap242xmlexport uses logger which outputs to os.Stderr, and also uses standard log.Panic
	// Save original stderr and log output
	originalStderr := os.Stderr
	originalLogOutput := log.Default().Writer()

	// Redirect both stderr and log to null device
	nullDevice, _ := os.Open(os.DevNull)
	os.Stderr = nullDevice
	log.SetOutput(nullDevice)

	defer func() {
		// Restore original outputs
		os.Stderr = originalStderr
		log.SetOutput(originalLogOutput)
		nullDevice.Close()
	}()

	// Export NamedGraph to AP242 XML
	if err := ap242xmlexport.AP242XmlExport(graph, f); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to export AP242 XML: %v\n", err)
		return
	}

	fmt.Printf("AP242 XML exported successfully to '%s'\n", outputFile)
}

func handleImportP21(args []string) {
	if len(args) < 1 {
		fmt.Println("Error: Missing arguments. Usage: importp21 <file-path>")
		return
	}
	filePath := args[0]

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

	// Open P21/STEP file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error: Failed to open file '%s': %v\n", filePath, err)
		return
	}
	defer func() {
		if e := file.Close(); e != nil {
			log.Printf("Error closing file: %v", e)
		}
	}()

	reader := bufio.NewReader(file)

	var graph sst.NamedGraph
	graph, err = p21.Parse(reader, log.Default())
	if err != nil {
		fmt.Printf("Error: Failed to import P21/STEP: %v\n", err)
		return
	}

	if graph == nil {
		fmt.Printf("Error: Failed to import P21/STEP: graph is nil\n")
		return
	}

	stage := graph.Stage()
	if stage == nil {
		fmt.Printf("Error: Failed to import P21/STEP: stage is nil\n")
		return
	}

	// Success! Set flag so defer will confirm
	success = true

	interactiveConfig.Stages[stageAlias] = stage
	interactiveConfig.StageAliases = append(interactiveConfig.StageAliases, stageAlias)
	interactiveConfig.StageSources[stageAlias] = filePath

	// Create alias for the graph (remove stage alias flag from args first)
	remainingArgs := utils.RemoveAliasFlag(args)
	graphAliasResult, err := utils.GetAlias(remainingArgs, "namedgraph")
	var graphAlias string
	if err != nil {
		fmt.Printf("Warning: Failed to get graph alias: %v\n", err)
	} else {
		graphAlias = graphAliasResult.Alias
		graphSuccess := false
		defer func() {
			if graphSuccess {
				graphAliasResult.Confirm()
			}
		}()

		if _, exists := interactiveConfig.NamedGraphs[graphAlias]; exists {
			fmt.Printf("Warning: NamedGraph alias '%s' already exists, skipping graph alias creation.\n", graphAlias)
		} else {
			graphSuccess = true
			interactiveConfig.NamedGraphs[graphAlias] = graph
			interactiveConfig.NamedGraphAliases = append(interactiveConfig.NamedGraphAliases, graphAlias)
		}
	}

	fmt.Printf("Import P21/STEP successful. Stage '%s' created for file '%s'.\n", stageAlias, filePath)

	if graphAlias != "" {
		if _, exists := interactiveConfig.NamedGraphs[graphAlias]; exists {
			utils.PrintNamedGraphDetails(graphAlias, graph)
		}
	}
}

func handleClone(sourceAlias string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: <source-repo-alias>.clone <target-directory> [target-alias]")
		fmt.Println("Example: r1.clone ./my-repo")
		fmt.Println("Example: r1.clone ./my-repo r2")
		return
	}

	// Get source repository
	sourceRepo, ok := interactiveConfig.Repositories[sourceAlias]
	if !ok {
		fmt.Printf("Error: Source repository alias '%s' not found.\n", sourceAlias)
		return
	}

	targetDir := args[0]

	// Get target alias if provided, otherwise generate automatically
	var targetAlias string
	var aliasResult utils.AliasResult
	if len(args) >= 2 {
		targetAlias = args[1]
	} else {
		// Generate alias automatically
		var err error
		aliasResult, err = utils.GetAlias([]string{}, "repository")
		if err != nil {
			fmt.Println(err)
			return
		}
		targetAlias = aliasResult.Alias
	}

	// Check if target alias already exists
	if _, exists := interactiveConfig.Repositories[targetAlias]; exists {
		fmt.Printf("Error: Repository alias '%s' already exists.\n", targetAlias)
		return
	}

	// Convert to absolute path
	absTargetDir, err := filepath.Abs(targetDir)
	if err != nil {
		fmt.Printf("Error: Failed to resolve target directory: %v\n", err)
		return
	}

	// Check if target directory already exists
	if _, err := os.Stat(absTargetDir); err == nil {
		fmt.Printf("Error: Target directory '%s' already exists.\n", absTargetDir)
		return
	}

	// Get auth context for source repository
	sourceCtx := utils.GetAuthContext(sourceRepo, interactiveConfig.AuthContexts)

	// Create target local repository
	fmt.Printf("Creating local repository at %s...\n", absTargetDir)
	targetRepo, err := sst.CreateLocalRepository(absTargetDir, "default@semanticstep.net", "default", true)
	if err != nil {
		fmt.Printf("Error: Failed to create local repository: %v\n", err)
		return
	}

	// Sync from source to target
	fmt.Printf("Syncing data from repository '%s' to '%s'...\n", sourceAlias, absTargetDir)
	utils.MuteLog()
	err = targetRepo.SyncFrom(sourceCtx, sourceRepo)
	utils.RestoreLog()

	if err != nil {
		fmt.Printf("Error: Failed to sync repository: %v\n", err)
		return
	}

	fmt.Printf("Successfully cloned repository to '%s'.\n", absTargetDir)

	targetRepo.Close()

	// Open the cloned repository in interactive mode
	fmt.Printf("Opening cloned repository as '%s'...\n", targetAlias)
	openedRepo, err := sst.OpenLocalRepository(absTargetDir, "default@semanticstep.net", "default")
	if err != nil {
		fmt.Printf("Warning: Failed to open cloned repository: %v\n", err)
		return
	}

	// Register the repository in interactive config
	interactiveConfig.Repositories[targetAlias] = openedRepo
	interactiveConfig.RepositoryLocations[targetAlias] = absTargetDir
	interactiveConfig.RepositoryTypes[targetAlias] = "local"
	interactiveConfig.RepositoryAliases = append(interactiveConfig.RepositoryAliases, targetAlias)

	fmt.Printf("Repository '%s' opened successfully.\n", targetAlias)

	// Confirm alias generation if it was auto-generated
	if len(args) < 2 {
		aliasResult.Confirm()
	}
}

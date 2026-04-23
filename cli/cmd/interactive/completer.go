// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"strings"

	"github.com/chzyer/readline"
)

// sstCompleter implements readline.AutoCompleter for command completion
type sstCompleter struct{}

// Do implements the AutoCompleter interface
func (c *sstCompleter) Do(line []rune, pos int) ([][]rune, int) {
	input := string(line[:pos])
	// If input is empty, return all top-level commands
	if strings.TrimSpace(input) == "" {
		return c.completeTopLevelCommands("")
	}

	// Check if input contains a dot (alias.command format)
	if strings.Contains(input, ".") {
		parts := strings.SplitN(input, ".", 2)
		if len(parts) == 2 {
			alias := strings.TrimSpace(parts[0])
			commandPrefix := parts[1]
			return c.completeAliasCommand(alias, commandPrefix)
		}
		// If there's a dot but no command part, suggest aliases
		return c.completeAliases(strings.TrimSpace(parts[0]))
	}

	// Check if input looks like it might be an alias (no space, no dot yet)
	trimmedInput := strings.TrimSpace(input)
	if !strings.Contains(trimmedInput, " ") {
		// Try to complete as alias first
		aliasCompletions, prefixLen := c.completeAliases(trimmedInput)
		if len(aliasCompletions) > 0 {
			// Add dot after alias
			result := make([][]rune, len(aliasCompletions))
			for i, comp := range aliasCompletions {
				result[i] = append(comp, '.')
			}
			return result, prefixLen
		}
	}

	// Otherwise, complete as top-level command
	return c.completeTopLevelCommands(trimmedInput)
}

// completeTopLevelCommands returns completions for top-level commands
func (c *sstCompleter) completeTopLevelCommands(prefix string) ([][]rune, int) {
	commands := []string{
		"q", "help",
		"open", "openlocalrepository", "openlocalflatrepository", "openremoterepository",
		"status", "rdfread", "importap242xml", "importp21",
	}

	var matches [][]rune
	prefixLower := strings.ToLower(prefix)

	for _, cmd := range commands {
		if prefix == "" || strings.HasPrefix(strings.ToLower(cmd), prefixLower) {
			if prefix == "" {
				matches = append(matches, []rune(cmd))
			} else {
				matches = append(matches, []rune(cmd[len(prefix):]))
			}
		}
	}

	return matches, len(prefix)
}

// completeAliases returns completions for aliases (repositories, datasets, stages, etc.)
func (c *sstCompleter) completeAliases(prefix string) ([][]rune, int) {
	var matches [][]rune
	prefixLower := strings.ToLower(prefix)

	// Collect all aliases
	allAliases := []string{}
	allAliases = append(allAliases, interactiveConfig.RepositoryAliases...)
	allAliases = append(allAliases, interactiveConfig.DatasetAliases...)
	allAliases = append(allAliases, interactiveConfig.StageAliases...)
	allAliases = append(allAliases, interactiveConfig.NamedGraphAliases...)
	allAliases = append(allAliases, interactiveConfig.IBNodeAliases...)

	for _, alias := range allAliases {
		if prefix == "" || strings.HasPrefix(strings.ToLower(alias), prefixLower) {
			if prefix == "" {
				matches = append(matches, []rune(alias))
			} else {
				matches = append(matches, []rune(alias[len(prefix):]))
			}
		}
	}

	return matches, len(prefix)
}

// completeAliasCommand returns completions for commands after an alias (e.g., r1.info)
func (c *sstCompleter) completeAliasCommand(alias, commandPrefix string) ([][]rune, int) {
	aliasLower := strings.ToLower(alias)
	commandPrefixLower := strings.ToLower(commandPrefix)

	// Determine alias type and get appropriate commands
	var commands []string

	// Check if it's a repository alias
	if _, isRepo := interactiveConfig.Repositories[aliasLower]; isRepo {
		commands = []string{
			"info", "close", "datasets", "dataset",
			"query", "queryuuid", "listfield", "log", "commitInfo", "commitinfo", "dump",
			"commitdiff", "openstage", "documentset", "documentget",
			"documentinfo", "documents", "documentdelete", "extractsstfile",
		}
	} else if _, isDataset := interactiveConfig.Datasets[aliasLower]; isDataset {
		commands = []string{
			"info", "listcommits", "commitsbyhash", "commitsbybranch",
			"branches", "leafcommits", "checkoutcommit", "checkoutbranch",
			"diff", "history",
		}
	} else if _, isStage := interactiveConfig.Stages[aliasLower]; isStage {
		commands = []string{
			"info", "namedgraphs", "referencednamedgraphs",
			"namedgraph", "moveandmerge",
			"commit", "validate", "rdfwrite", "trig",
		}
	} else if _, isNamedGraph := interactiveConfig.NamedGraphs[aliasLower]; isNamedGraph {
		commands = []string{
			"info", "foririnodes", "forallibnodes", "forblanknodes",
			"getirinodebyfragment", "getblanknodebyfragment",
			"rdfwrite", "exportap242xml", "ttl",
		}
	} else if _, isIBNode := interactiveConfig.IBNodes[aliasLower]; isIBNode {
		commands = []string{
			"forall",
		}
	} else {
		// Unknown alias, return all possible commands
		commands = []string{
			"info", "close", "datasets", "dataset",
			"query", "queryuuid", "listfield", "log", "commitInfo", "commitinfo", "dump",
			"commitdiff", "openstage", "documentset", "documentget",
			"documentinfo", "documents", "documentdelete", "extractsstfile",
			"listcommits", "commitsbyhash",
			"commitsbybranch", "branches", "leafcommits", "checkoutcommit",
			"checkoutbranch", "diff", "history", "namedgraphs",
			"referencednamedgraphs", "namedgraph",
			"moveandmerge", "commit", "validate", "foririnodes",
			"forallibnodes", "forblanknodes", "getirinodebyfragment",
			"getblanknodebyfragment", "rdfwrite", "ttl", "forall",
		}
	}

	var matches [][]rune
	for _, cmd := range commands {
		if commandPrefix == "" || strings.HasPrefix(strings.ToLower(cmd), commandPrefixLower) {
			if commandPrefix == "" {
				matches = append(matches, []rune(cmd))
			} else {
				matches = append(matches, []rune(cmd[len(commandPrefix):]))
			}
		}
	}

	return matches, len(commandPrefix)
}

// NewCompleter creates and returns a new sstCompleter instance
func NewCompleter() readline.AutoCompleter {
	return &sstCompleter{}
}

# SST Core Command Line Interface (CLI) - Complete Guide

## Overview

The SST Core Command Line Interface (CLI) is a low-level interactive tool designed for debugging, testing, and managing SST Core components and SST Repositories. It provides a terminal-based interface for direct interaction with SST repositories, datasets, stages, and other core elements.

## Building the CLI

If the CLI tool is not yet built, you can create the executable by running:

```bash
go build -o cli/sst ./cli/main.go
```

This will generate the `sst` executable in the `cli/` directory.

## Starting the CLI

The CLI tool is started by invoking the sst executable:

```bash
$ ./cli/sst
```

Or explicitly:

```bash
$ ./cli/sst interactive
```

Upon startup, the CLI enters interactive mode:

```
Entering SST CLI tool in interactive mode. Type 'q' to quit, 'help' to see available commands.

sst >
```

To exit the interactive mode, simply type:

```
sst > q
Exiting SST CLI tool
```

## Basic Usage

### Getting Help

At any time, type `help` to see all available commands:

```
sst > help
```

This displays a comprehensive list of interactive commands organized by category.

### Status and Information

Check the current state of opened resources:

```
sst > status
```

This shows all currently opened repositories, datasets, stages, named graphs, and IBNodes with their aliases.

## Command Syntax

Commands in the CLI follow a pattern of `<alias>.<command>` for operations on specific resources. Resources are automatically assigned aliases when opened (e.g., `r1`, `r2` for repositories; `d1`, `d2` for datasets; `s1`, `s2` for stages).

- **Repository commands:** `<repo-alias>.<command>`
  - Example: `r1.info`, `r1.datasets`, `r1.close`
- **Dataset commands:** `<dataset-alias>.<command>`
  - Example: `d1.commitsbyhash <hash>`, `d1.history`, `d1.checkoutbranch <branch>`
- **Stage commands:** `<stage-alias>.<command>`
  - Example: `s1.validate`, `s1.commit "message"`, `s1.namedgraphs`
- **NamedGraph commands:** `<namedgraph-alias>.<command>`
  - Example: `g1.info`, `g1.foririnodes`, `g1.ttl`
- **IBNode commands:** `<ibnode-alias>.<command>`
  - Example: `n1.forall`
- **Standalone commands:** Direct commands without alias
  - Example: `help`, `q`, `status`, `rdfread`

## Repository Commands

### Opening Repositories

Open a local repository:

```
sst > openlocalrepository /path/to/repository
```

This opens a local SST repository and assigns it an alias (e.g., `r1`).

Open a remote repository:

```
sst > openremoterepository https://example.com/repo
```

This opens a remote SST repository via URL and assigns it an alias.

### Repository Information

Show repository information:

```
sst > <repo-alias>.info
```

Displays detailed information about the repository including:
- EndPoint
- MasterDBSize and DerivedDBSize
- Number of Datasets, Dataset Revisions, Named Graph Revisions, Commits
- RepositoryLogs count
- Whether it's remote
- Whether it supports revision history
- Bleve index information

Example:
```
sst > r1.info
```

### List Datasets

List all datasets in the repository:

```
sst > <repo-alias>.datasets
```

Example:
```
sst > r1.datasets
```

### Get Dataset by IRI

Get a dataset by IRI:

```
sst > <repo-alias>.dataset <iri>
```

Example:
```
sst > r1.dataset urn:uuid:fcfe1293-3045-4717-8065-7dc3659e1faf
```

The dataset is assigned an alias (e.g., `d1`) for subsequent operations.

### Query Repository Index

Run a Bleve text query in the repository index:

```
sst > <repo-alias>.query <bleve-query> [--limit <number>]
```

Example:
```
sst > r1.query *
```

Query document by UUID:

```
sst > <repo-alias>.queryuuid <uuid>
```

Example:
```
sst > r1.queryuuid fbe2b5ad-2cc1-4549-a4d4-eb16972ce619
```

> **Note:** Both query commands display all indexed fields for each result. The `--limit` parameter (default: 10) controls how many results are returned.

### List Indexed Fields

List all indexed fields in the repository:

```
sst > <repo-alias>.listfield
```

This shows all available searchable fields in the Bleve index.

### Commit History

List commit history:

```
sst > <repo-alias>.log [-v]
```

Use `-v` flag to show detailed commit information.

Example:
```
sst > r1.log
sst > r1.log -v
```

Show commit details by hash:

```
sst > <repo-alias>.commitInfo <commit-hash>
```

Example:
```
sst > r1.commitInfo abc123def456...
```

Show commit diff (all changes in a commit):

```
sst > <repo-alias>.commitdiff <commit-hash>
```

This shows all added/modified/deleted NamedGraphs in the given commit.

### SuperRepository (of a repository)

Show SuperRepository information for this repository:

```
sst > <repo-alias>.superrepository
```

### Stage Operations

Create an empty stage:

```
sst > <repo-alias>.openstage
```

This creates a new empty stage and assigns it an alias (e.g., `s1`).

### Document Operations

List all documents in the repository:

```
sst > <repo-alias>.documents
```

Show document metadata:

```
sst > <repo-alias>.documentinfo <hash>
```

Upload a document file:

```
sst > <repo-alias>.documentset <file>
```

Example:
```
sst > r1.documentset /path/to/document.pdf
```

Download a document by hash:

```
sst > <repo-alias>.documentget <hash> <output-file>
```

Example:
```
sst > r1.documentget abc123... output.pdf
```

Delete a document:

```
sst > <repo-alias>.documentdelete <hash>
```

### Internal Operations (Advanced)

Dump internal BoltDB data (use with caution):

```
sst > <repo-alias>.dump <bucket-key>[/<sub-key>]
```

Example:
```
sst > r1.dump ds
```

Clone a repository to a local directory:

```
sst > <repo-alias>.clone <target-directory>
```

Sync data from another repository:

```
sst > <repo-alias>.syncfrom <source-repo-alias> [branch] [dataset1] [dataset2] ...
```

Extract raw SST file:

```
sst > <repo-alias>.extractsstfile <hash>
```

Extract the raw SST file of a NamedGraphRevision by its hash.

### Close Repository

Close a repository:

```
sst > <repo-alias>.close
```

This closes the repository and removes it from the active session.

## Dataset Commands

### List Commits

List all commits in a dataset:

```
sst > <dataset-alias>.listcommits
```

This shows all leaf commits (commits not identified by a branch).

List all branches and their commit hashes:

```
sst > <dataset-alias>.branches
```

Example output:
```
Branches:
  master: abc123def456...
  branch1: def456ghi789...
```

List all leaf commits (commits not identified by a branch):

```
sst > <dataset-alias>.leafcommits
```

### Get Commit Details

Get commit details by hash:

```
sst > <dataset-alias>.commitsbyhash <hash>
```

Example:
```
sst > d1.commitsbyhash abc123def456...
```

Get commit details by branch:

```
sst > <dataset-alias>.commitsbybranch <branch-name>
```

Example:
```
sst > d1.commitsbybranch master
```

Both commands display:
- Commit Hash
- Author
- Date
- Message
- Dataset Revisions
- NamedGraph Revisions
- Parent Commits

### Checkout Operations

Checkout a specific commit:

```
sst > <dataset-alias>.checkoutcommit <hash>
```

This creates a new stage from the specified commit. If no stage alias is provided, one will be auto-generated.

Example:
```
sst > d1.checkoutcommit abc123... stage s2
```

Checkout a branch:

```
sst > <dataset-alias>.checkoutbranch <branch-name>
```

This creates a new stage from the specified branch.

Example:
```
sst > d1.checkoutbranch master
```

### History and Diff

Show commit history graph:

```
sst > <dataset-alias>.history
```

This displays an graph visualization of the commit history.

Compare two NamedGraphRevision hashes:

```
sst > <dataset-alias>.diff <NGR-hash1> <NGR-hash2>
```

This shows the differences between two NamedGraphRevision hashes, including added, modified, and deleted triples.

Example:
```
sst > d1.diff abc123... def456...
```

## Stage Commands

### Stage Information

Show stage information:

```
sst > <stage-alias>.info
```

Displays:
- Number of local graphs
- Number of referenced graphs
- Total number of triples
- IBNodes count for each graph

### Named Graph Operations

List named graphs in the stage:

```
sst > <stage-alias>.namedgraphs
```

List referenced named graphs:

```
sst > <stage-alias>.referencednamedgraphs
```

Get named graph by IRI:

```
sst > <stage-alias>.namedgraph <iri>
```

Examples:
```
sst > s1.namedgraph urn:uuid:fcfe1293-3045-4717-8065-7dc3659e1faf#
```

### Validation

Validate stage content:

```
sst > <stage-alias>.validate
```

This validates the stage content for RDF syntax and domain-range constraints. The output is formatted with clear error and warning sections organized by NamedGraph.

### Commit Changes

Commit current changes in the stage:

```
sst > <stage-alias>.commit <message> [branch]
```

Example:
```
sst > s1.commit "Initial commit"
sst > s1.commit "Updated configuration" feature-branch
```

### Reading RDF Files

Read an RDF file in Turtle or TriG format into a new stage:

```
sst > rdfread <file>
```

Example:
```
sst > rdfread /path/to/data.ttl
```

This creates a new stage from the RDF file.

### Import/Export (Converters)

Import AP242 XML into a new stage:

```
sst > importap242xml <file>
```

Export a NamedGraph to AP242 XML:

```
sst > <namedgraph-alias>.exportap242xml <output-file.xml>
```

Import a STEP P21 file into a new stage:

```
sst > importp21 <file>
```

## SuperRepository Commands

SuperRepositories are opened with `openlocalsuperrepository` / `openremotesuperrepository` and get an alias (e.g., `sr1`).

Commands:

```
sst > <superrepo-alias>.info
sst > <superrepo-alias>.close
sst > <superrepo-alias>.list
sst > <superrepo-alias>.get <repo-name>
sst > <superrepo-alias>.create <repo-name>
sst > <superrepo-alias>.delete <repo-name>
```

### RDF Output (Stage)

Write RDF of the Stage to a file (TriG format):

```
sst > <stage>.rdfwrite <file>
```

Print RDF of the Stage to the console (TriG format):

```
sst > <stage>.trig
```

## NamedGraph Commands

### NamedGraph Information

Show named graph information:

```
sst > <namedgraph-alias>.info
```

Displays detailed information including:
- IRI and ID
- Whether it's referenced or empty
- Whether it's modified
- Number of IRI Nodes, Blank Nodes, Term Collections
- Number of Direct/All Imported Graphs
- Number of Subject/Predicate/Object/TermCollection Triples
- Commit Hash, NamedGraph Revision Hash, Dataset Revision Hash

### List Nodes

List all IRI nodes:

```
sst > <namedgraph-alias>.foririnodes
```

List all IBNodes (IRI nodes and blank nodes):

```
sst > <namedgraph-alias>.forallibnodes
```

List all blank nodes:

```
sst > <namedgraph-alias>.forblanknodes
```

### Get Node by Fragment

Get IRINode by fragment:

```
sst > <namedgraph-alias>.getirinodebyfragment <fragment-id>
```

Get blank node by fragment:

```
sst > <namedgraph-alias>.getblanknodebyfragment <fragment-id>
```

### RDF Output

Write RDF to a file (Turtle format):

```
sst > <namedgraph-alias>.rdfwrite <file>
```

Example:
```
sst > ng1.rdfwrite output.ttl
```

Print RDF to console (Turtle format):

```
sst > <namedgraph-alias>.ttl
```

This outputs the entire NamedGraph in Turtle format to the console.

## IBNode Commands

### List Triples

List all triples in an IBNode:

```
sst > <ibnode-alias>.forall
```

This displays all triples where the IBNode appears as subject, predicate, or object.

## Workflow Examples

### Example 1: Basic Repository and Dataset Workflow

```
# Open a local repository
sst > openlocalrepository /path/to/repo

# Check status
sst > status

# List datasets
sst > r1.datasets

# Get a dataset
sst > r1.dataset urn:uuid:fcfe1293-3045-4717-8065-7dc3659e1faf

# View branches
sst > d1.branches

# Checkout a branch
sst > d1.checkoutbranch master

# View stage info
sst > s1.info

# List named graphs
sst > s1.namedgraphs

# Get a named graph
sst > s1.namedgraph urn:uuid:fcfe1293-3045-4717-8065-7dc3659e1faf#

# View named graph info
sst > g1.info
```

### Example 2: Reading and Validating RDF

```
# Read an RDF file
sst > rdfread data.ttl

# Validate the stage
sst > s1.validate

# If valid, commit it
sst > s1.commit "Initial data import"
```

### Example 3: Working with Commits

```
# View commit history
sst > r1.log -v

# Get commit details
sst > d1.commitsbyhash abc123...

# View history graph
sst > d1.history

# Compare revisions
sst > d1.diff hash1 hash2
```

### Example 4: Querying and Searching

```
# List indexed fields
sst > r1.listfield

# Run a text query
sst > r1.query "field:value"
sst > r1.query connector --limit 20

# Query by UUID
sst > r1.queryuuid fbe2b5ad-2cc1-4549-a4d4-eb16972ce619

# Parse a part number
sst > r1.parse D38999/26ZJ35JE
```

## Exiting

To exit the interactive mode, simply type:

```
sst > q
```

This will close all opened resources and exit the CLI.

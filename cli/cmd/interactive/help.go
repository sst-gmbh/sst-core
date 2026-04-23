// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"fmt"
	"io"
)

// displayHelp shows available commands
func displayHelp(w io.Writer) {
	fmt.Fprintln(w, "Available commands in interactive mode:")

	commands := []struct {
		command string
		desc    string
	}{
		{"q", "Exit interactive mode"},
		{"help", "Show this help message"},
		{"openlocalrepository <path>", "Open a local repository"},
		{"openlocalflatrepository <path>", "Open a local flat repository (directory of .sst files)"},
		{"openremoterepository <URL>", "Open a remote repository"},
		{"openlocalsuperrepository <path>", "Open a local SuperRepository"},
		{"openremotesuperrepository <URL>", "Open a remote SuperRepository"},
		{"status", "Show currently opened repo, dataset ..."},
		{"rdfread <file>", "Read an RDF file in Turtle or TriG format into a new stage"},
		{"importap242xml <file>", "Import AP242 XML file into a new stage"},
		{"importp21 <file>", "Import P21/STEP file into a new stage"},

		{"<repo>.info", "Show repository info"},
		{"<repo>.close", "Close a repository"},
		{"<repo>.superrepository", "Show SuperRepository information for this repository"},
		{"<repo>.datasets", "List datasets"},
		{"<repo>.dataset <iri>", "Get dataset by IRI"},
		{"<repo>.query <bleve-query>", "Run a Bleve text query in the repository index"},
		{"<repo>.queryuuid <uuid>", "Query document by UUID"},
		{"<repo>.listfield", "List indexed fields in the repository"},
		{"<repo>.log [-v|--verbose]", "List commit history; use -v to show detailed info"},
		{"<repo>.commitInfo <commit-hash>", "Show commit details by commit hash"},
		{"<repo>.commitdiff <commit-hash>", "Show all changes (diff) in the given commit (added/modified/deleted NamedGraphs)"},
		{"<repo>.extractsstfile <hash>", "Extract the raw SST file of a NamedGraphRevision by its hash"},
		{"<repo>.dump <bucket-key>[/<sub-key>]", "Dump internal BoltDB data. Use with caution. See below for key meanings."},
		{"<repo>.openstage", "Create an empty stage"},
		{"<repo>.syncfrom <source-repo-alias> [branch] [dataset1] [dataset2] ...", "Sync data from another repository to this repository."},
		{"<repo>.clone <target-directory>", "Clone this repository to a local directory"},

		{"<repo>.documentinfo <hash>", "Show document metadata in the repository"},
		{"<repo>.documents", "List all documents in the repository"},
		{"<repo>.documentdelete <hash>", "Delete a document by its hash"},
		{"<repo>.documentset <file>", "Upload a document file"},
		{"<repo>.documentget <hash> <output>", "Download a document by hash to a local file"},

		{"<superrepo>.info", "Show SuperRepository info"},
		{"<superrepo>.close", "Close a SuperRepository"},
		{"<superrepo>.list", "List all repositories in the SuperRepository"},
		{"<superrepo>.get <repo-name>", "Get a repository from the SuperRepository"},
		{"<superrepo>.create <repo-name>", "Create a new repository in the SuperRepository"},
		{"<superrepo>.delete <repo-name>", "Delete a repository from the SuperRepository"},

		{"<dataset>.listcommits", "List commits in a dataset"},
		{"<dataset>.commitsbyhash <hash>", "List commits by hash"},
		{"<dataset>.commitsbybranch <branch>", "List commits by branch"},
		{"<dataset>.branches", "List all branches and their commit hashes"},
		{"<dataset>.leafcommits", "List all leaf commits (commits not identified by a branch)"},
		{"<dataset>.checkoutcommit <hash>", "Checkout commit"},
		{"<dataset>.checkoutbranch <name>", "Checkout branch"},
		{"<dataset>.diff <NGR-hash1> <NGR-hash2>", "Compare two NamedGraphRevision hashes and show their differences"},
		{"<dataset>.history", "Show commit history graph for the dataset"},

		{"<stage>.info", "Show stage info"},
		{"<stage>.namedgraphs", "List named graphs in a stage"},
		{"<stage>.referencednamedgraphs", "List referenced named graphs in a stage"},
		{"<stage>.namedgraph <iri>", "Get named graph by IRI"},
		{"<stage-alias>.commit <message> [branch]", "Commit current changes in the stage, with a message and optional branch name"},
		{"<stage>.validate", "Validate stage content (rdf/domain-range)"},
		{"<stage>.rdfwrite <file>", "Write RDF of the Stage to a file (TriG format)"},
		{"<stage>.trig", "Print RDF of the Stage to the console (TriG format)"},

		{"<namedgraph>.info", "Show named graph info"},
		{"<namedgraph>.foririnodes", "List all IRI nodes in the named graph"},
		{"<namedgraph>.forallibnodes", "List all IBNodes (IRI nodes and blank nodes) in the named graph"},
		{"<namedgraph>.forblanknodes", "List all blank nodes in the named graph"},
		{"<namedgraph>.getirinodebyfragment", "Get IRINode by fragment (fragment ID)"},
		{"<namedgraph>.getblanknodebyfragment", "Get blank node by fragment (fragment ID)"},
		{"<namedgraph>.rdfwrite <file>", "Write RDF of the NamedGraph to a file (Turtle format)"},
		{"<namedgraph>.exportap242xml <output-file.xml>", "Export a NamedGraph to an AP242 XML .xml file"},
		{"<namedgraph>.ttl", "Print RDF of the NamedGraph to the console (Turtle format)"},

		{"<ibnode>.forall", "List triples in an IBNode"},
	}

	for _, cmd := range commands {
		fmt.Fprintf(w, "    %-40s %s\n", cmd.command, cmd.desc)
	}

	// Extra dump details
	fmt.Fprintln(w, "\n<repo>.dump key reference:")
	fmt.Fprintln(w, "    ngr   - NamedGraphRevisions   (revisions of named graphs)")
	fmt.Fprintln(w, "    dsr   - DatasetRevisions      (dataset version history)")
	fmt.Fprintln(w, "    c     - Commits               (commit metadata: author, message, timestamp)")
	fmt.Fprintln(w, "    ds    - Datasets              (dataset metadata: IRI, UUID, etc.)")
	fmt.Fprintln(w, "    dl    - DatasetLog            (chronological commit log entries)")

	fmt.Fprintln(w, "\nExamples:")
	fmt.Fprintln(w, "    r1.dump ds                     # Dump all datasets")
	fmt.Fprintln(w, "    r1.dump \"c/<commit-hash>\"      # Dump specific commit metadata entry")
	fmt.Fprintln(w, "    r1.dump c                      # View all commit metadata")
}

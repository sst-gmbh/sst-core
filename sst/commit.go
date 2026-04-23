// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrCommitNotFound = errors.New("commit not found")
)

// DefaultBranch specifies the "master" branch as a default branch in SST.
const DefaultBranch = "master"

// CommitDetails provide detailed information about a commit.
// This structure is returned by Repository CommitDetails method and Dataset methods CommitDetailsByHash and CommitDetailsByBranch.
// This structure is only used as returned value; therefore any changes on the values of this structure will not affect SST.
type CommitDetails struct {
	Commit              Hash
	Author              string
	AuthorDate          time.Time
	Message             string
	ParentCommits       map[IRI][]Hash
	DatasetRevisions    map[IRI]Hash
	NamedGraphRevisions map[IRI]Hash
}

// print CommitDetails content into standard out; e.g.
//
//	Output of CommitDetails.Dump()
//	commit Hash: 6r7cqYdrKKJmCNnfKoKCxfVbUjCd7uK8PwxZAQCGXSeN
//	Author: default@semanticstep.net
//	Author Date: 2025-01-23 02:00:25 +0000 UTC
//	Message: First commit of C
//	No Parents
//	Datasets:
//		Dataset ID:  c1efcf54-3e8e-4cc7-a7d1-82a9f613a363
func (r *CommitDetails) Dump() {
	fmt.Println("commit Hash:", r.Commit)
	fmt.Println("Author:", r.Author)
	fmt.Println("Author Date:", r.AuthorDate)
	fmt.Println("Message:", r.Message)

	if len(r.ParentCommits) == 0 {
		fmt.Println("No ParentsCommits")
	} else {
		fmt.Println("Parents:")
		for key, value := range r.ParentCommits {
			fmt.Println("	Dataset ID: ", key, "Commit Hash: ", value)
		}
	}
	for key, value := range r.DatasetRevisions {
		fmt.Println("DatasetID: ", key, " DatasetRevision: ", value)
	}

	for key, value := range r.NamedGraphRevisions {
		fmt.Println("NamedGraphID: ", key, " NamedGraphRevision: ", value)
	}
}

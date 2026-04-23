// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

type datasetCommitPair struct {
	DsIDs []uuid.UUID
	Pair  [2]Hash
}

type commitRelationship uint8

const (
	commitUnrelated = commitRelationship(iota)
	commitOlder
	commitSame
	commitNewer
)

type commitRelationshipRange struct {
	Relationship commitRelationship
	Count        uint8
}

const maxCommitRelationshipRangeCount = uint8(0xff)

// The purpose of this function sortedIDIdx is to find the index position of a particular UUID in a sorted UUID slice.
// It uses binary search to find it efficiently. If the UUID is found, its index and a boolean value are returned true,
// otherwise 0 and false are returned.
func sortedIDIdx(sortedIDs []uuid.UUID, id uuid.UUID) (int, bool) {
	idx := sort.Search(len(sortedIDs), func(i int) bool { return bytes.Compare(sortedIDs[i][:], id[:]) >= 0 })
	if idx < len(sortedIDs) && sortedIDs[idx] == id {
		return idx, true
	}
	return 0, false
}

func compareCommits(r Repository, dsCommitPairs []datasetCommitPair) (_ []commitRelationshipRange, err error) {
	relationships := make([]commitRelationshipRange, 0, len(dsCommitPairs))
	db := r.(*localFullRepository).db
	tx1, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() {
		e := tx1.Rollback()
		if e != nil && err == nil {
			err = e
		}
	}()
	commits1 := tx1.Bucket(keyCommits)
	if commits1 == nil {
		return relationships, nil
	}
	tx2, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() {
		e := tx2.Rollback()
		if e != nil && err == nil {
			err = e
		}
	}()
	commits2 := tx2.Bucket(keyCommits)
	if commits2 == nil {
		return relationships, nil
	}
	err = db.View(func(tx *bbolt.Tx) error {
		commits := tx.Bucket(keyCommits)
		if commits == nil {
			return nil
		}
		datasets := tx.Bucket(keyDatasets)
		var rr commitRelationshipRange
		for _, p := range dsCommitPairs {
			for _, dsID := range p.DsIDs {
				dsKey := iDToPrefixedKey(dsID, commitDsPrefix)
				cID1, found1 := comparisonCommit(dsKey, commits, p.Pair[0])
				cID2, found2 := comparisonCommit(dsKey, commits, p.Pair[1])
				oCommit := commits.Bucket(cID1[:])
				if oCommit == nil {
					rel := compareCommitsIfBucketIsNotAvailable(datasets, dsID, cID1, cID2, found1, found2)
					relationships, rr = addToCommitRelationshipRange(relationships, rr, rel)
					continue
				}
				nCommit := commits.Bucket(cID2[:])
				if nCommit == nil {
					rel := compareCommitsIfBucketIsNotAvailable(datasets, dsID, cID1, cID2, found1, found2)
					relationships, rr = addToCommitRelationshipRange(relationships, rr, rel)
					continue
				}
				if cID1 == cID2 {
					relationships, rr = addToCommitRelationshipRange(relationships, rr, commitSame)
					continue
				}

				var wg sync.WaitGroup
				wg.Add(2)
				var breakOlder, breakNewer int32
				var older bool
				go func() {
					older = isCommitAncestor(dsKey, commits1, cID1, cID2, &breakOlder)
					if older {
						atomic.StoreInt32(&breakNewer, 1)
					}
					wg.Done()
				}()
				var newer bool
				go func() {
					newer = isCommitAncestor(dsKey, commits2, cID2, cID1, &breakNewer)
					if newer {
						atomic.StoreInt32(&breakOlder, 1)
					}
					wg.Done()
				}()
				wg.Wait()
				var rel commitRelationship
				if older && !newer {
					rel = commitOlder
				} else if !older && newer {
					rel = commitNewer
				} else {
					rel = compareCommitsIfBucketIsNotAvailable(datasets, dsID, cID1, cID2, found1, found2)
				}
				relationships, rr = addToCommitRelationshipRange(relationships, rr, rel)
			}
		}
		if rr.Count != 0 {
			relationships = append(relationships, rr)
		}
		return nil
	})
	return relationships, err
}

func comparisonCommit(dsKey []byte, commits *bbolt.Bucket, commitID Hash) (_ Hash, found bool) {
	return comparisonCommitFrom(dsKey, commits, commitID, commitID, HashNil())
}

func comparisonCommitFrom(
	dsKey []byte, commits *bbolt.Bucket, cmpCommitIDIn, cIDIn Hash, cmpDSHashIn Hash,
) (_ Hash, found bool) {
	cmpCommitID, cID, cmpDSHash := cmpCommitIDIn, cIDIn, cmpDSHashIn
	for {
		cmt := commits.Bucket(cID[:])
		if cmt == nil {
			return cmpCommitID, false
		}
		actualDSKey, dsHashParentCommitHash := cmt.Cursor().Seek(dsKey)
		if !bytes.Equal(actualDSKey, dsKey) {
			return cmpCommitID, true
		}
		dsHash := Hash(([len(Hash{})]byte)(dsHashParentCommitHash[:len(Hash{})]))
		if cmpDSHash == HashNil() {
			cmpDSHash = dsHash
		}
		if cmpDSHash != dsHash {
			return cmpCommitID, true
		}
		cmpCommitID = cID
		for i := len(Hash{}) * 3; i <= len(dsHashParentCommitHash); i += len(Hash{}) {
			cID := Hash(([len(Hash{})]byte)(dsHashParentCommitHash[i-len(Hash{}) : i]))
			cID, found = comparisonCommitFrom(dsKey, commits, cmpCommitID, cID, cmpDSHash)
			if cID != HashNil() && cID != cmpCommitID {
				return cID, found
			}
		}
		if len(dsHashParentCommitHash) < len(Hash{})<<1 {
			return cmpCommitID, true
		}
		cID = Hash(([len(Hash{})]byte)(dsHashParentCommitHash[len(Hash{}) : len(Hash{})<<1]))
	}
}

func compareCommitsIfBucketIsNotAvailable(
	datasets *bbolt.Bucket, dsID uuid.UUID, cID1, cID2 Hash, found1, found2 bool,
) (rel commitRelationship) {
	if !found1 || !found2 {
		c1Latest := isLatestDatasetCommit(datasets, dsID, cID1)
		c2Latest := isLatestDatasetCommit(datasets, dsID, cID2)
		if c1Latest && !c2Latest {
			rel = commitOlder
		} else if !c1Latest && c2Latest {
			rel = commitNewer
		} else {
			rel = commitSame // hackish
		}
	}
	return
}

func isLatestDatasetCommit(datasets *bbolt.Bucket, dsID uuid.UUID, cID Hash) bool {
	if datasets != nil {
		dsRefs := datasets.Bucket(dsID[:])
		if dsRefs == nil {
			return false
		}
		dsRefCursor := dsRefs.Cursor()
		for ref, refCommitID := dsRefCursor.First(); ref != nil; ref, refCommitID = dsRefCursor.Next() {
			if ref[0] != '\x00' {
				break
			}
			if Hash(([len(Hash{})]byte)(refCommitID)) == cID {
				return true
			}
		}
	}
	return false
}

func addToCommitRelationshipRange(
	inRelationships []commitRelationshipRange, inRR commitRelationshipRange, rel commitRelationship,
) (relationships []commitRelationshipRange, rr commitRelationshipRange) {
	relationships, rr = inRelationships, inRR
	if rr.Count != 0 && (rr.Relationship != rel || rr.Count == maxCommitRelationshipRangeCount) {
		relationships = append(relationships, rr)
		rr = commitRelationshipRange{}
	}
	rr.Relationship = rel
	rr.Count++
	return
}

func isCommitAncestor(
	dsKey []byte, commits *bbolt.Bucket, childCommitID, ancestorCommitID Hash, earlyBreak *int32,
) bool {
	cID := childCommitID
	for {
		if cID == ancestorCommitID {
			return true
		}
		if earlyBreak != nil && atomic.LoadInt32(earlyBreak) != 0 {
			return false
		}
		cmt := commits.Bucket(cID[:])
		if cmt == nil {
			return false
		}
		actualDSKey, dsHashParentCommitHash := cmt.Cursor().Seek(dsKey)
		if !bytes.Equal(actualDSKey, dsKey) {
			return false
		}
		for i := len(Hash{}) * 3; i <= len(dsHashParentCommitHash); i += len(Hash{}) {
			cID := BytesToHash(dsHashParentCommitHash[i-len(Hash{}) : i])

			if isCommitAncestor(dsKey, commits, cID, ancestorCommitID, earlyBreak) {
				return true
			}
		}
		if len(dsHashParentCommitHash) >= len(Hash{})<<1 {
			cID = BytesToHash(dsHashParentCommitHash[len(Hash{}) : len(Hash{})<<1])
			continue
		}
		return false
	}
}

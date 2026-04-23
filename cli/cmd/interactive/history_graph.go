// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"git.semanticstep.net/x/sst/sst"
)

type bfsNode struct {
	Row           int
	Col           int
	Symbol        string
	Self          string
	CommitMessage string
	Author        string
	Date          time.Time
	Parents       []string
}

func queryHistoryBranches(ngIRI sst.IRI, repo sst.Repository, ctx context.Context) (graph [][]*bfsNode) {
	ds, err := repo.Dataset(ctx, ngIRI)
	if err != nil {
		panic(err)
	}

	leafCommits, err := ds.LeafCommits(ctx)
	if err != nil {
		panic(err)
	}

	if len(leafCommits) == 0 {
		if branches, berr := ds.Branches(ctx); berr == nil && len(branches) > 0 {
			for _, h := range branches {
				leafCommits = append(leafCommits, h)
			}
		}
		if len(leafCommits) == 0 {
			return graph
		}
	}

	currentLayer := map[sst.Hash]interface{}{}
	nextLayer := map[sst.Hash]interface{}{}
	allNodes := []*bfsNode{}
	allNodesMap := map[string]*bfsNode{}
	for _, v := range leafCommits {
		nextLayer[v] = nil
	}

	for len(nextLayer) > 0 {
		currentLayer = nextLayer
		nextLayer = map[sst.Hash]interface{}{}

		for k := range currentLayer {
			commitDetails, detErr := ds.CommitDetailsByHash(ctx, k)
			if detErr != nil || commitDetails == nil {
				continue
			}

			parentCommits := []string{}
			for _, v := range commitDetails.ParentCommits[ngIRI] {
				parentCommits = append(parentCommits, v.String())
			}

			if _, existed := allNodesMap[commitDetails.Commit.String()]; !existed {
				node := &bfsNode{
					Self:          commitDetails.Commit.String(),
					CommitMessage: commitDetails.Message,
					Author:        commitDetails.Author,
					Date:          commitDetails.AuthorDate,
					Parents:       parentCommits,
				}
				allNodes = append(allNodes, node)
				allNodesMap[commitDetails.Commit.String()] = node
			}

			for _, v := range commitDetails.ParentCommits[ngIRI] {
				nextLayer[v] = nil
			}
		}
	}

	sort.Slice(allNodes, func(i, j int) bool {
		return allNodes[i].Date.Before(allNodes[j].Date)
	})

	curRow := 0
	curCol := 0
	maxRowMap := map[int]int{}
	maxCol := 0
	var parentNode *bfsNode
	for _, v := range allNodes {
		if parentNode == nil {
			v.Row = curRow
			v.Col = curCol
			v.Symbol = "*"

			lineForNode := []*bfsNode{}
			lineForNode = append(lineForNode, v)

			graph = append(graph, lineForNode)
			maxRowMap[curCol] = curRow
			parentNode = v

			curRow++
		} else {
			if len(v.Parents) == 1 {
				if v.Parents[0] == parentNode.Self {
					v.Row = curRow
					v.Col = curCol
					v.Symbol = "*"

					lineForNode := []*bfsNode{}
					for i := 0; i < curCol; i++ {
						lineForNode = append(lineForNode, &bfsNode{Symbol: " "})
					}
					lineForNode = append(lineForNode, v)

					graph = append([][]*bfsNode{lineForNode}, graph...)
					maxRowMap[curCol] = curRow
					curRow++

					parentNode = v
				} else {
					currentParent := allNodesMap[v.Parents[0]]

					if maxRowMap[currentParent.Col] == currentParent.Row {
						for i := range graph {
							length := len(graph[i])

							if length <= currentParent.Col {
								for j := 0; j < currentParent.Col-length; j++ {
									graph[i] = append(graph[i], &bfsNode{Symbol: " "})
								}
								graph[i] = append(graph[i], &bfsNode{Symbol: "|"})
							} else {
								if graph[i][currentParent.Col] == currentParent ||
									graph[i][currentParent.Col].Symbol != " " {
									break
								} else {
									graph[i][currentParent.Col].Symbol = "|"
								}
							}
						}

						curCol = currentParent.Col
						v.Row = curRow
						v.Col = curCol
						v.Symbol = "*"

						lineForNode := []*bfsNode{}
						for i := 0; i < curCol; i++ {
							lineForNode = append(lineForNode, &bfsNode{Symbol: " "})
						}
						lineForNode = append(lineForNode, v)

						graph = append([][]*bfsNode{lineForNode}, graph...)
						maxRowMap[curCol] = curRow
						curRow++
					} else {
						maxCol++
						curCol = maxCol

						for i := range graph {
							length := len(graph[i])

							for j := 0; j < curCol-length; j++ {
								graph[i] = append(graph[i], &bfsNode{Symbol: " "})
							}

							if graph[i][currentParent.Col] == currentParent {
								graph[i] = append(graph[i], &bfsNode{Symbol: "/"})
								break
							} else {
								graph[i] = append(graph[i], &bfsNode{Symbol: "|"})
							}
						}

						v.Row = curRow
						v.Col = curCol
						v.Symbol = "*"

						lineForNode := []*bfsNode{}
						for i := 0; i < curCol; i++ {
							lineForNode = append(lineForNode, &bfsNode{Symbol: " "})
						}
						lineForNode = append(lineForNode, v)

						graph = append([][]*bfsNode{lineForNode}, graph...)
						maxRowMap[curCol] = curRow
						curRow++
					}

					parentNode = v
				}
			} else {
				if allTop(maxRowMap, v.Parents, allNodesMap) {
					mostLeftCol := allNodesMap[v.Parents[0]].Col
					mostRightCol := allNodesMap[v.Parents[0]].Col
					for _, parent := range v.Parents {
						if mostLeftCol > allNodesMap[parent].Col {
							mostLeftCol = allNodesMap[parent].Col
						}
						if mostRightCol < allNodesMap[parent].Col {
							mostRightCol = allNodesMap[parent].Col
						}
					}

					for _, parent := range v.Parents {
						for i := range graph {
							length := len(graph[i])

							if length <= allNodesMap[parent].Col {
								for j := 0; j < allNodesMap[parent].Col-length; j++ {
									graph[i] = append(graph[i], &bfsNode{Symbol: " "})
								}
								graph[i] = append(graph[i], &bfsNode{Symbol: "|"})
							} else {
								if graph[i][allNodesMap[parent].Col] == allNodesMap[parent] ||
									graph[i][allNodesMap[parent].Col].Symbol != " " {
									break
								} else {
									graph[i][allNodesMap[parent].Col].Symbol = "|"
								}
							}
						}
					}

					curCol = mostLeftCol
					v.Row = curRow
					v.Col = curCol
					v.Symbol = "*"

					lineForNode := []*bfsNode{}
					for i := 0; i < curCol; i++ {
						lineForNode = append(lineForNode, &bfsNode{Symbol: " "})
					}
					lineForNode = append(lineForNode, v)
					for i := 0; i < mostRightCol-curCol; i++ {
						lineForNode = append(lineForNode, &bfsNode{Symbol: " "})
					}
					for _, parent := range v.Parents {
						if allNodesMap[parent].Col != mostLeftCol {
							lineForNode[allNodesMap[parent].Col].Symbol = "\\"
						}
					}

					graph = append([][]*bfsNode{lineForNode}, graph...)
					maxRowMap[curCol] = curRow
					curRow++
				} else {
					mostEarlyRow := allNodesMap[v.Parents[0]].Row
					for _, vp := range v.Parents {
						if allNodesMap[vp].Row < mostEarlyRow {
							mostEarlyRow = allNodesMap[vp].Row
						}
					}

					maxCol++
					curCol = maxCol

					for i := 0; i < len(graph)-mostEarlyRow; i++ {
						length := len(graph[i])

						for j := 0; j < curCol-length; j++ {
							graph[i] = append(graph[i], &bfsNode{Symbol: " "})
						}
						graph[i] = append(graph[i], &bfsNode{Symbol: "|"})
					}

					for _, vp := range v.Parents {
						rowIdx := len(graph) - allNodesMap[vp].Row - 1
						colIdx := len(graph[rowIdx]) - 1
						graph[rowIdx][colIdx].Symbol = "/"
					}

					v.Row = curRow
					v.Col = curCol
					v.Symbol = "*"

					lineForNode := []*bfsNode{}
					for i := 0; i < curCol; i++ {
						lineForNode = append(lineForNode, &bfsNode{Symbol: " "})
					}
					lineForNode = append(lineForNode, v)

					graph = append([][]*bfsNode{lineForNode}, graph...)
					maxRowMap[curCol] = curRow
					curRow++
				}

				parentNode = v
			}
		}
	}

	if branches, err := ds.Branches(ctx); err == nil {
		for i := range graph {
			info := ""
			for _, subv := range graph[i] {
				if subv.Symbol == "*" {
					info += subv.CommitMessage + " " + subv.Author + " " + subv.Date.UTC().Format(time.RFC3339)

					branch := []string{}
					for k, v := range branches {
						if subv.Self == v.String() {
							branch = append(branch, k)
						}
					}
					if len(branch) > 0 {
						info += " <- " + strings.Join(branch, ",")
					}

					lineLen := len(graph[i])
					for j := 0; j < maxCol+1-lineLen; j++ {
						graph[i] = append(graph[i], &bfsNode{Symbol: " "})
					}
					graph[i] = append(graph[i], &bfsNode{Symbol: info})
					break
				}
			}
		}
	}

	for _, v := range graph {
		for _, subv := range v {
			fmt.Printf("%s ", subv.Symbol)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")

	return graph
}

func allTop(maxRowMap map[int]int, allParents []string, allNodesMap map[string]*bfsNode) bool {
	for _, v := range allParents {
		if maxRowMap[allNodesMap[v].Col] != allNodesMap[v].Row {
			return false
		}
	}

	return true
}

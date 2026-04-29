// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// Package filterextraction split a bigger NamedGraph to smaller ones
// according to the needs of SST Ontologies and to support SST Repositories.
package filterextraction

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/semanticstep/sst-core/sst"
	_ "github.com/semanticstep/sst-core/vocabularies/dict" // register vocabulary map
	"github.com/semanticstep/sst-core/vocabularies/lci"
	"github.com/semanticstep/sst-core/vocabularies/rdfs"
	"github.com/semanticstep/sst-core/vocabularies/rep"
	"github.com/semanticstep/sst-core/vocabularies/sso"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// returns a map
// key:    predicate
// value:  subject collection of this predicate
func CollectNodeAsPredicateUsages(graph sst.NamedGraph) (map[sst.IBNode]map[sst.IBNode]struct{}, error) {
	usages := map[sst.IBNode]map[sst.IBNode]struct{}{}
	err := graph.ForAllIBNodes(func(d sst.IBNode) error {
		return d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if s == d && p.OwningGraph() == graph {
				u, found := usages[p]
				if !found {
					u = map[sst.IBNode]struct{}{}
					usages[p] = u
				}
				u[d] = struct{}{}
			}
			return nil
		})
	})
	return usages, err
}

type nodeGroup struct {
	rootNodeType  sst.ElementInformer
	group         map[sst.IBNode]struct{}
	outsideNodes  map[sst.IBNode]struct{}
	ID            uuid.UUID
	importedGraph sst.NamedGraph
}

func listMap(group nodeGroup) {
	log.Printf("NodeList for graph %s\n", group.ID)
	for ib := range group.group {
		if ib.IsBlankNode() {
			log.Printf("Node =: %s\n", ib.ID())
		} else {
			log.Printf("Node =: %s\n", ib.Fragment())
		}
	}
	for ib := range group.outsideNodes {
		log.Printf("  Outside ref: %s\n", ib.Fragment())
	}
}

func findVocabularyTopPredicate(p sst.IBNode) (sst.ElementInformer, error) {
	var superProperty sst.IBNode
	err := p.ForAll(func(_ int, ts, tp sst.IBNode, to sst.Term) error {
		if p == ts && tp.Is(rdfs.SubPropertyOf) && sst.IsKindIBNode(to) {
			superProperty = to.(sst.IBNode)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if superProperty != nil {
		return findVocabularyTopPredicate(superProperty)
	}
	dt := p.InVocabulary()
	return findVocabularyTopProperty(dt), nil
}

func findVocabularyTopProperty(dt sst.ElementInformer) sst.ElementInformer {
	// log.Printf(" %s", dt.Element().GoSimpleName() )
	if dt.IsObjectProperty() {
		// log.Printf(".")
		dtSuper := dt.SubPropertyOf()
		if dtSuper != nil {
			return findVocabularyTopProperty(dtSuper)
		}
	}
	return dt
}

// process part-whole relationships, see Mereology
func loopNodes(graph sst.NamedGraph, ib sst.IBNode, group nodeGroup) error {
	// log.Printf("LoopNode =: %s\n", ib.Fragment())
	group.group[ib] = struct{}{}
	if ib.IsBlankNode() {
		log.Printf("Add node %s to group %s", ib.ID(), group.ID)
	} else {
		log.Printf("Add node %s to group %s", ib.Fragment(), group.ID)
	}
	err := ib.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		// log.Printf("FindTop:")
		dt, err := findVocabularyTopPredicate(p)
		if err != nil {
			return err
		}
		// log.Printf(" = %s\n", dt.VocabularyElement().GoSimpleName())
		switch dt.(type) {
		case lci.IsPartOf:
			if o == ib {
				if p.OwningGraph() == graph {
					group.group[p] = struct{}{}
					log.Printf("Add node %s to group %s", ib.Fragment(), group.ID)
				}
				err := loopNodes(graph, s, group)
				if err != nil {
					return err
				}
			}
		case lci.IsHasPart:
			if s == ib {
				if p.OwningGraph() == graph {
					group.group[p] = struct{}{}
					log.Printf("Add node %s to group %s", ib.Fragment(), group.ID)
				}
				err := loopNodes(graph, o.(sst.IBNode), group)
				if err != nil {
					return err
				}
			}
		default:
		}
		return nil
	})
	return err
}

func processRootNodes(graph sst.NamedGraph, rootIB sst.IBNode) (nodeGroup, error) {
	returnedGroup := nodeGroup{
		group: map[sst.IBNode]struct{}{},
	}
	log.Printf("RootNode =: %s\n", rootIB.Fragment())
	err := loopNodes(graph, rootIB, returnedGroup)
	if err != nil {
		return returnedGroup, err
	}
	groupFragments := make([]string, 0, len(returnedGroup.group))
	var groupFragmentsLen int
	for d := range returnedGroup.group {
		var df string
		if d.IsBlankNode() {
			df = ""
		} else {
			df = d.Fragment()
		}
		groupFragments = append(groupFragments, df)
		groupFragmentsLen += len(df)
	}
	sort.Slice(groupFragments, func(j, k int) bool {
		return groupFragments[j] < groupFragments[k]
	})
	groupFragmentBytes := make([]byte, 0, groupFragmentsLen)
	for _, f := range groupFragments {
		groupFragmentBytes = append(groupFragmentBytes, ([]byte)(f)...)
	}
	returnedGroup.ID = uuid.NewSHA1(uuid.NameSpaceURL, groupFragmentBytes)
	returnedGroup.outsideNodes = map[sst.IBNode]struct{}{}
	for ib := range returnedGroup.group {
		err := collectOutsideNodes(returnedGroup, ib)
		if err != nil {
			return returnedGroup, err
		}
	}
	listMap(returnedGroup)
	// next: separate group in its own NG
	return returnedGroup, nil
}

func SearchRootNodes(graph sst.NamedGraph) ([]nodeGroup, error) {
	grouppedNodes := map[sst.IBNode]uuid.UUID{}
	var groups []nodeGroup
	var err error
	err = graph.ForAllIBNodes(func(d sst.IBNode) error {
		tempType := d.TypeOf()
		if tempType != nil {
			switch rt := tempType.InVocabulary().(type) {
			// KindPart may change to KindGenericPart
			case sso.KindPart,
				lci.KindOrganization,
				sso.KindShapeFeatureDefinition:
				sst.GlobalLogger.Info("IBNode is KindPart/KindOrganization/KindShapeFeatureDefinition", zap.String("IRI", d.IRI().String()))
				groups, err = appendGroups(graph, d, rt, groups, grouppedNodes)
				return err
			case rep.KindRepresentationContext:
				sst.GlobalLogger.Info("IBNode is KindRepresentationContext", zap.String("IRI", d.IRI().String()))

				var repItemCnt int
				var placementsOnly bool
				var hasExtGeomModel bool
				var is3DGeometricContext bool
				err = d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					switch p.InVocabulary().(type) {
					case rep.IsContextOfItems:
						if o != d {
							return nil
						}
						if _, ok := s.TypeOf().InVocabulary().(sso.KindExternalGeometricModel); ok {
							hasExtGeomModel = true
						}
						if repItemCnt == 0 {
							placementsOnly, err = hasPlacementsOnly(s)
							if err != nil {
								return err
							}
						}
						repItemCnt++
					case rep.IsRepresentationsInContext:
						if s != d {
							return nil
						}
						if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
							o := o.(sst.IBNode)
							if _, ok := o.TypeOf().InVocabulary().(sso.KindExternalGeometricModel); ok {
								hasExtGeomModel = true
							}
							if repItemCnt == 0 {
								placementsOnly, err = hasPlacementsOnly(o)
								if err != nil {
									return err
								}
							}
						}
						repItemCnt++

					case rep.IsCoordinateSpaceDimension:
						if s != d {
							return nil
						}
						if val, ok := o.(sst.Integer); ok && val == 3 {
							is3DGeometricContext = true
						}

					}
					return nil
				})
				if err != nil {
					return err
				}
				if (repItemCnt != 1 || !placementsOnly) && !hasExtGeomModel && is3DGeometricContext {
					groups, err = appendGroups(graph, d, rt, groups, grouppedNodes)
					return err
				}
				// if is3DGeometricContext {
				// 	groups, err = appendGroups(graph, d, rt, groups, grouppedNodes)
				// 	return err
				// }
			}
		}
		return nil
	})
	sort.Slice(groups, func(i, j int) bool {
		g1, g2 := groups[i], groups[j]
		return rootNodeTypeToPriority(g1.rootNodeType) < rootNodeTypeToPriority(g2.rootNodeType)
	})
	return groups, err
}

func appendGroups(
	graph sst.NamedGraph,
	rootNode sst.IBNode,
	rt sst.ElementInformer,
	inGroups []nodeGroup,
	grouppedNodes map[sst.IBNode]uuid.UUID,
) ([]nodeGroup, error) {
	groups := inGroups
	nodegroup, err := processRootNodes(graph, rootNode)
	if err != nil {
		return groups, err
	}
	nodegroup.rootNodeType = rt
	groups = append(groups, nodegroup)
	// Check if nodes in a group were not already assigned to another group
	// if that is the case print a warning and move node from node list
	// and consider it as outside node
	for d := range nodegroup.group {
		if gID, found := grouppedNodes[d]; found {
			log.Printf("  WARNING: Node %s already assigned to group %s\n", d.Fragment(), gID)
			delete(nodegroup.group, d)
			nodegroup.outsideNodes[d] = struct{}{}
			log.Printf("  Add Node %s to group %s outsideNodes\n", d.Fragment(), nodegroup.ID)
		} else {
			grouppedNodes[d] = nodegroup.ID
		}
	}
	return groups, nil
}

func hasPlacementsOnly(d sst.IBNode) (bool, error) {
	var notOnlyPlacements bool
	err := d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if d == s {
			if _, ok := p.InVocabulary().(rep.KindItem); ok {
				if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
					o := o.(sst.IBNode)
					if _, ok := o.TypeOf().InVocabulary().(rep.KindPlacement); !ok {
						notOnlyPlacements = true
					}
					return nil
				}
				notOnlyPlacements = true
			}
		}
		return nil
	})
	return !notOnlyPlacements, err
}

func rootNodeTypeToPriority(rt sst.ElementInformer) int {
	switch rt.(type) {
	case sso.KindPart,
		lci.KindOrganization:
		return 0
	default:
		return 100
	}
}

func ExtractImportedGraphs(
	graph sst.NamedGraph,
	nodegroups []nodeGroup,
	predicateUsages map[sst.IBNode]map[sst.IBNode]struct{},
) error {
	for i, nodegroup := range nodegroups {
		currentNodeGroupNG := graph.Stage().CreateNamedGraph(sst.IRI(nodegroup.ID.URN()))
		log.Printf("Create New NamedGraph %s\n", currentNodeGroupNG.IRI())

		graph.AddImport(currentNodeGroupNG)

		log.Printf("%s add import %s\n", graph.IRI(), currentNodeGroupNG.IRI())

		nodegroup.importedGraph = currentNodeGroupNG

		// handle group nodes
		for n := range nodegroup.group {
			var err error
			if n.IsBlankNode() {
				err = currentNodeGroupNG.MoveIBNode(n, "")
			} else {
				err = currentNodeGroupNG.MoveIBNode(n, n.Fragment())
			}
			if err != nil {
				panic(err)
			}
		}

		nodegroups[i] = nodegroup
	}
	for _, nodegroup := range nodegroups {
		for {
			var outsideNodeAdded bool
			for d := range nodegroup.outsideNodes {
				if d.OwningGraph() == graph {
					var err error
					if d.IsBlankNode() {
						err = nodegroup.importedGraph.MoveIBNode(d, "")
						log.Printf("    Move %s to %s\n", d.ID(), nodegroup.importedGraph.IRI())
					} else {
						err = nodegroup.importedGraph.MoveIBNode(d, d.Fragment())
						log.Printf("    Move %s to %s\n", d.Fragment(), nodegroup.importedGraph.IRI())
					}

					if err != nil {
						return err
					}
					nodegroup.group[d] = struct{}{}
					if d.IsBlankNode() {
						log.Printf("Add node %s to group %s", d.ID(), nodegroup.ID)
					} else {
						log.Printf("Add node %s to group %s", d.Fragment(), nodegroup.ID)
					}
					delete(nodegroup.outsideNodes, d)
					prevOutsideNodeCount := len(nodegroup.outsideNodes)
					err = collectOutsideNodes(nodegroup, d)
					if err != nil {
						return err
					}
					if len(nodegroup.outsideNodes) != prevOutsideNodeCount {
						outsideNodeAdded = true
					}
				}
			}
			if !outsideNodeAdded {
				break
			}
		}
	}
	var injectedGraph sst.NamedGraph
	for _, nodegroup := range nodegroups {
		importedGraphs := map[sst.NamedGraph]struct{}{}
		for outsideNode := range nodegroup.outsideNodes {
			if ng := outsideNode.OwningGraph(); ng != nil && !ng.IsReferenced() {
				if _, found := importedGraphs[ng]; found {
					continue
				}
				var err error
				if outsideNode.Fragment() != "first" {
					for _, val := range nodegroup.importedGraph.DirectImports() {
						if val.IRI() == ng.IRI() {
							err = sst.ErrNamedGraphAlreadyImported
						}
					}

					for _, val := range ng.DirectImports() {
						if val.IRI() == nodegroup.importedGraph.IRI() {
							err = sst.ErrNamedGraphImportCycle
						}
					}

					if err == nil {
						nodegroup.importedGraph.AddImport(ng)
					}
				}
				if err != nil {
					if !errors.Is(err, sst.ErrNamedGraphImportCycle) && !errors.Is(err, sst.ErrNamedGraphAlreadyImported) && !errors.Is(err, sst.ErrStagesAreNotTheSame) {
						log.Printf("Failed import on %s to %s with error\n", ng.IRI(), nodegroup.importedGraph.IRI())
						// return err
						panic(err)
					}
					injectedGraph, err = moveNamedGraphNodeToInjectGraph(
						nodegroup.importedGraph,
						outsideNode, injectedGraph,
						predicateUsages,
					)
				} else {
					log.Printf("Add import on %s to %s\n", ng.IRI(), nodegroup.importedGraph.IRI())
					importedGraphs[ng] = struct{}{}
				}
			}
		}
	}
	return nil
}

func collectOutsideNodes(group nodeGroup, ib sst.IBNode) error {
	return ib.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if s == ib {
			if ng := p.OwningGraph(); ng != nil && !ng.IsReferenced() {
				if _, found := group.group[p]; !found {
					group.outsideNodes[p] = struct{}{}
					log.Printf("    Add outside node %s to group %s\n", p.Fragment(), group.ID)
				}
			}
			if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
				o := o.(sst.IBNode)
				if ng := o.OwningGraph(); ng != nil && !ng.IsReferenced() {
					if _, found := group.group[o]; !found {
						group.outsideNodes[o] = struct{}{}
						if o.IsBlankNode() {
							log.Printf("    Add outside node %s to group %s\n", o.ID(), group.ID)
						} else {
							log.Printf("    Add outside node %s to group %s\n", o.Fragment(), group.ID)
						}
					}
				}
			}
		}
		return nil
	})
}

func moveNamedGraphNodeToInjectGraph(
	importingGraph sst.NamedGraph,
	ibnode sst.IBNode,
	inInjectedGraph sst.NamedGraph,
	predicateUsages map[sst.IBNode]map[sst.IBNode]struct{},
) (returnedGraph sst.NamedGraph, err error) {
	returnedGraph = inInjectedGraph
	importedGraphs := map[sst.NamedGraph]struct{}{}
	if returnedGraph == nil {
		injectedGraphImp := importingGraph.Stage().CreateNamedGraph("")
		importingGraph.AddImport(injectedGraphImp)

		returnedGraph = injectedGraphImp

		log.Printf("Injected graph created %s\n", injectedGraphImp.IRI())
		importedGraphs[importingGraph] = struct{}{}
		log.Printf("    Added import to %s\n", importingGraph.IRI())
	}
	err = moveAllToGraph(returnedGraph, ibnode, importedGraphs, predicateUsages)
	if err != nil {
		panic(err)
	}
	return
}

func moveAllToGraph(
	targetGraph sst.NamedGraph,
	ibnode sst.IBNode,
	importedGraphs map[sst.NamedGraph]struct{},
	predicateUsages map[sst.IBNode]map[sst.IBNode]struct{},
) error {
	var err error
	if ibnode.IsBlankNode() {
		log.Printf("  Moved to injected graph %s\n", ibnode.ID())
		err = targetGraph.MoveIBNode(ibnode, "")
	} else {
		log.Printf("  Moved to injected graph %s\n", ibnode.Fragment())
		err = targetGraph.MoveIBNode(ibnode, ibnode.Fragment())
	}

	if err != nil {
		// return err
		panic(err)
	}
	if p, found := predicateUsages[ibnode]; found {
		for u := range p {
			log.Printf("    Predicate usage %s\n", u.PrefixedFragment())
			err := maybeAddImportFromUsedNode(u, targetGraph, importedGraphs)
			if err != nil {
				// return err
				panic(err)
			}
		}
	}
	return ibnode.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if s == ibnode {
			if ng := p.OwningGraph(); !ng.IsEmpty() && !ng.IsReferenced() {
				err := moveAllToGraph(targetGraph, p, importedGraphs, predicateUsages)
				if err != nil {
					return err
				}
			}
			if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
				o := o.(sst.IBNode)
				if ng := o.OwningGraph(); ng != nil && !ng.IsReferenced() {
					return moveAllToGraph(targetGraph, o, importedGraphs, predicateUsages)
				}
			}
		} else {
			log.Printf("    Inverse %s\n", s.PrefixedFragment())
			return maybeAddImportFromUsedNode(s, targetGraph, importedGraphs)
		}
		return nil
	})
}

func maybeAddImportFromUsedNode(d sst.IBNode, targetGraph sst.NamedGraph, importedGraphs map[sst.NamedGraph]struct{}) error {
	if g := d.OwningGraph(); g != (nil) && !g.IsReferenced() && g != targetGraph {
		log.Printf("    -- Trying to import graph %s\n", g.IRI())
		if _, found := importedGraphs[g]; !found {
			var ng sst.NamedGraph

			ng = g.Stage().NamedGraph(targetGraph.IRI())
			if ng == nil {
				ng = g.Stage().CreateNamedGraph(targetGraph.IRI())
			}
			log.Printf("    Create NG %s\n", ng.IRI())

			for _, val := range g.DirectImports() {
				if val.IRI() == targetGraph.IRI() {
					// return sst.ErrNamedGraphAlreadyImported
					// continue
					return nil
				}
			}

			for _, val := range targetGraph.DirectImports() {
				if val.IRI() == g.IRI() {
					return sst.ErrNamedGraphImportCycle
				}
			}

			g.AddImport(targetGraph)

			log.Printf("    Added import to %s\n", g.IRI())
			importedGraphs[g] = struct{}{}
		}
	}
	return nil
}

func commitData(sourceGraph sst.NamedGraph, targetRepository sst.Repository) error {
	var st sst.Stage
	// check if the sourceGraph is already saved in the targetRepository
	namespace, err := targetRepository.Dataset(context.TODO(), sourceGraph.IRI())
	// if not exist
	if err != nil {
		st = targetRepository.OpenStage(sst.DefaultTriplexMode)

	} else { // if exist
		st, err = namespace.CheckoutBranch(context.TODO(), sst.DefaultBranch, sst.DefaultTriplexMode)
		if err != nil {
			return err
		}
	}

	_, err = st.MoveAndMerge(context.TODO(), sourceGraph.Stage())
	if err != nil {
		return err
	}

	// for test, print ttl out
	// for _, ng := range stage.NamedGraphs() {
	// 	f, err := os.Create(filepath.Join("../../step/testdata/ewh/stepxmlRepository", ng.ID().String()+".ttl"))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer f.Close()
	// 	ng.RdfWrite(f, sst.RdfFormatTurtle)
	// }

	_, _, err = st.Commit(context.TODO(), fmt.Sprintf("filtered graph %s", sourceGraph.IRI()), sst.DefaultBranch)
	if err != nil {
		return err
	}
	return nil
}

// path is the repository path, the TTL files in the folder where the provided path is located will be checked.
func Run(repo sst.Repository, path string) {
	log.SetFlags(log.Lshortfile)
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), "_") {
			return filepath.SkipDir
		}
		if !d.IsDir() && !strings.HasPrefix(d.Name(), "_") && strings.HasSuffix(d.Name(), ".ttl") {
			log.Printf("Processing: %s\n", path)
			file, err := os.Open(path)
			defer func() {
				e := file.Close()
				if err == nil {
					err = e
				}
			}()
			st, err := sst.RdfRead(bufio.NewReader(file), sst.RdfFormatTurtle, sst.StrictHandler, sst.DefaultTriplexMode)
			if err != nil {
				log.Panic(err)
			}
			graph := st.NamedGraphs()[0]
			predicateUsages, err := CollectNodeAsPredicateUsages(graph)
			if err != nil {
				log.Panic(err)
			}
			groups, err := SearchRootNodes(graph)
			if err != nil {
				log.Panic(err)
			}

			err = ExtractImportedGraphs(graph, groups, predicateUsages)
			if err != nil {
				log.Panic(err)
			}

			// for testing - write TTLs out
			// for _, ng := range graph.Stage().NamedGraphs() {
			// 	f, err := os.Create(ng.ID().String() + ".ttl")
			// 	if err != nil {
			// 		panic(err)
			// 	}
			// 	defer f.Close()

			// 	err = ng.RdfWrite(f, sst.RdfFormatTurtle)
			// 	if err != nil {
			// 		log.Panic(err)
			// 	}
			// }

			err = commitData(graph, repo)
			if err != nil {
				log.Panic(err)
			}
		}
		return nil
	})
	if err != nil {
		log.Panic(err)
	}
}

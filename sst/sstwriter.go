// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var ErrMissingImports = errors.New("missing import(s)")

type ibNodeWriteCounts = struct {
	termCollectionMemberCount        int // number of members in the collection
	termCollectionLiteralMemberCount int // number of literal and LiteralCollection members in the collection

	nonTermCollectionIBNodeTripleCount      int // number of predicates
	nonTermCollectionLiteralTripleCount     int // number of literals
	nonTermCollectionLiteralCollectionCount int // number of literal collections

	extraCount int // number of references to this IBNode in this NamedGraph
}

type ibNodeWritingContext = struct {
	ibNodeWriteCounts
	index int
}

type countAndIndex = struct {
	count int
	index int
}

type writingNodeContext = struct {
	nodeCountIndexMap map[IBNode]countAndIndex
	sortedNodes       []IBNode
}

type varintBufT = [binary.MaxVarintLen64]byte

type graphWritingContext struct {
	blankNodeCount   int
	blankNodeSubPred map[uuid.UUID]string

	// for main graph IBNodes
	ibNodeContexts map[IBNode]ibNodeWritingContext
	sortedNodes    []IBNode

	// for imported Graphs IBNodes
	importedNodeContexts    map[uuid.UUID]writingNodeContext
	sortedImportNamedGraphs []NamedGraph

	// for referencedGraph IBNodes
	referencedGraphNodeContexts map[IRI]writingNodeContext
	sortedReferencedGraphs      []NamedGraph

	varintBuf varintBufT
}

// SstWrite writes NamedGraph into given writer using SST file format.
func (graph *namedGraph) SstWrite(w io.Writer) error {
	gc, err := sstWriteHeader(graph, w)
	if err != nil {
		panic(err)
	}
	err = sstWriteDictionary(&gc, w)
	if err != nil {
		panic(err)
	}
	err = sstWriteContent(graph, &gc, w)
	if err != nil {
		panic(err)
	}
	return nil
}

func sstWriteHeader(graph NamedGraph, w io.Writer) (gc graphWritingContext, err error) {
	_, err = w.Write(sstFileHeader[:])
	if err != nil {
		return gc, err
	}
	graphURI := graph.IRI().String()
	err = writeString(graphURI, &gc.varintBuf, w)
	if err != nil {
		return gc, err
	}
	GlobalLogger.Debug("writeString: ", zap.String("graphURI", graphURI))

	gc, err = collectGraphContext(graph)
	if err != nil {
		return gc, err
	}
	err = writeUint(uint(len(gc.sortedImportNamedGraphs)), &gc.varintBuf, w)
	if err != nil {
		return gc, err
	}
	GlobalLogger.Debug("imported Graphs: ", zap.Uint("count", uint(len(gc.sortedImportNamedGraphs))))

	for _, grImport := range gc.sortedImportNamedGraphs {
		// grID := grImport.ID()
		// switch grID.Version() {
		// case 3, 5:
		// 	ig := graph.Stage().NamedGraph(grID)
		// 	if ig == nil {
		// 		return gc, err
		// 	}
		// 	graphURI = ig.IRI().String()
		// default:
		// 	graphURI = "urn:uuid:" + grID.String()
		// }
		graphURI = grImport.(*namedGraph).baseIRI
		err = writeString(graphURI, &gc.varintBuf, w)
		if err != nil {
			return gc, err
		}
		GlobalLogger.Debug("imported Graphs: ", zap.String("graphURI", graphURI))
	}
	err = writeUint(uint(len(gc.sortedReferencedGraphs)), &gc.varintBuf, w)
	if err != nil {
		return gc, err
	}
	GlobalLogger.Debug("referenced Graphs: ", zap.Uint("count", uint(len(gc.sortedReferencedGraphs))))

	for _, exImport := range gc.sortedReferencedGraphs {
		err = writeString(exImport.IRI().String(), &gc.varintBuf, w)
		if err != nil {
			return gc, err
		}
		GlobalLogger.Debug("referenced Graphs: ", zap.String("graphURI", exImport.IRI().String()))
	}
	return gc, nil
}

// This function will extract all information about this NamedGraph and put it into graphWritingContext.
func collectGraphContext(graph NamedGraph) (gc graphWritingContext, err error) {
	gc.ibNodeContexts = map[IBNode]ibNodeWritingContext{}
	gc.importedNodeContexts = map[uuid.UUID]writingNodeContext{}
	gc.referencedGraphNodeContexts = map[IRI]writingNodeContext{}
	gc.blankNodeSubPred = map[uuid.UUID]string{}

	_ = graph.ForAllIBNodes(func(s IBNode) error {
		if s.IsBlankNode() {
			gc.blankNodeCount++
			gc.blankNodeSubPred[s.ID()] = ""
			s.ForAll(func(index int, sub, p IBNode, o Term) error {
				// is object triple
				if s == o && !sub.IsBlankNode() {
					gc.blankNodeSubPred[s.ID()] = sub.iriOrID() + p.iriOrID()
				}
				return nil
			})
		}
		tempIBNodeWritingContext := s.(*ibNode).sortAndCountTriplesForSSTWrite(&gc)
		gc.ibNodeContexts[s] = ibNodeWritingContext{
			ibNodeWriteCounts: tempIBNodeWritingContext,
			index:             0, // will be updated by assignWritingNodeIndices
		}
		return nil
	})

	for {
		emptyModified := false

		for key, val := range gc.blankNodeSubPred {
			if val == "" {
				graph.GetBlankNodeByID(key).ForAll(func(index int, sub, p IBNode, o Term) error {
					// is object triple
					switch o.TermKind() {
					case TermKindIBNode:
						if o.(IBNode).IsBlankNode() && key == o.(IBNode).ID() && gc.blankNodeSubPred[key] != "" {
							gc.blankNodeSubPred[key] = gc.blankNodeSubPred[key] + p.iriOrID()
							emptyModified = true
						}
					}

					return nil
				})
			}
		}

		// if not modified in this round, means there is no more blankNodes can be identified, then break
		if !emptyModified {
			break
		}
	}

	// mapCount := 1
	// for _, val := range gc.blankNodeSubPred {
	// 	fmt.Println("bl"+strconv.Itoa(mapCount), ":", val)
	// 	mapCount++
	// }

	gc.sortedImportNamedGraphs = graph.DirectImports()
	sort.Slice(gc.sortedImportNamedGraphs, func(i, j int) bool {
		return strings.Compare(gc.sortedImportNamedGraphs[i].ID().String(), gc.sortedImportNamedGraphs[j].ID().String()) < 0
	})
	var usedImportCount int
	for _, ngi := range gc.sortedImportNamedGraphs {
		if _, found := gc.importedNodeContexts[ngi.ID()]; found {
			usedImportCount++
		}
	}
	if usedImportCount != len(gc.importedNodeContexts) {
		return gc, ErrMissingImports
	}
	sort.Slice(gc.sortedReferencedGraphs, func(i, j int) bool {
		return strings.Compare(gc.sortedReferencedGraphs[i].IRI().String(), gc.sortedReferencedGraphs[j].IRI().String()) < 0
	})
	return
}

func sstWriteDictionary(gc *graphWritingContext, w io.Writer) error {
	var err error
	sortWritingNodes(gc)
	err = writeUint(uint(len(gc.sortedNodes)-gc.blankNodeCount), &gc.varintBuf, w)
	GlobalLogger.Debug("writeUint: ", zap.Uint("iri node count", uint(len(gc.sortedNodes)-gc.blankNodeCount)))
	if err != nil {
		return err
	}
	err = writeUint(uint(gc.blankNodeCount), &gc.varintBuf, w)
	GlobalLogger.Debug("writeUint: ", zap.Uint("blank node count", uint(gc.blankNodeCount)))
	if err != nil {
		return err
	}
	for _, t := range gc.sortedNodes {
		var fragment string
		if t.IsBlankNode() {
			fragment = ""
			// do nothing
		} else {
			fragment = t.Fragment()
			err = writeString(fragment, &gc.varintBuf, w)
			GlobalLogger.Debug("write string: ", zap.String("fragment", fragment))
			// fmt.Println("write string: ", fragment)
			if err != nil {
				return err
			}
		}

		c := gc.ibNodeContexts[t]
		tc := c.termCollectionMemberCount + c.nonTermCollectionIBNodeTripleCount + c.nonTermCollectionLiteralTripleCount + c.nonTermCollectionLiteralCollectionCount + c.extraCount
		err = writeUint(uint(tc), &gc.varintBuf, w)
		if t.IsBlankNode() {
			GlobalLogger.Debug("blankNode: ", zap.String("ID", t.fragOrID()), zap.Uint("tc", uint(tc)))
		} else {
			GlobalLogger.Debug("writeUint: ", zap.Uint("tc", uint(tc)))
		}

		if err != nil {
			return err
		}
	}
	for _, graphImport := range gc.sortedImportNamedGraphs {
		gID := graphImport.ID()
		gc.importedNodeContexts[gID], err = writeRefIBNodes(gc.importedNodeContexts[gID], &gc.varintBuf, w)
		if err != nil {
			return err
		}
	}
	for _, namedGraph := range gc.sortedReferencedGraphs {
		baseIRI := namedGraph.IRI()
		gc.referencedGraphNodeContexts[baseIRI], err = writeRefIBNodes(gc.referencedGraphNodeContexts[baseIRI], &gc.varintBuf, w)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeRefIBNodes(rc writingNodeContext, varintBuf *varintBufT, w io.Writer) (writingNodeContext, error) {
	err := writeUint(uint(len(rc.nodeCountIndexMap)), varintBuf, w)
	if err != nil {
		return rc, err
	}
	GlobalLogger.Debug("writeUint: ", zap.Uint("ref node count", uint(len(rc.nodeCountIndexMap))))
	rc.sortedNodes = make([]IBNode, 0, len(rc.nodeCountIndexMap))
	for t := range rc.nodeCountIndexMap {
		rc.sortedNodes = append(rc.sortedNodes, t)
	}
	var duplicates []string
	sort.Slice(rc.sortedNodes, func(i, j int) bool {
		if rc.sortedNodes[i].Fragment() == rc.sortedNodes[j].Fragment() {
			duplicates = append(duplicates, rc.sortedNodes[i].Fragment())
		}
		return rc.sortedNodes[i].Fragment() < rc.sortedNodes[j].Fragment()
	})
	if len(duplicates) > 0 {
		panic(fmt.Errorf("%w: %s", ErrDuplicatedNodes, duplicates))
	}
	for _, t := range rc.sortedNodes {
		// outputString := ""
		// if t.Fragment() == rdfFirstProperty.fragment {
		// 	outputString = string((t.(*ibNode).ng.baseIRI) + "#" + t.Fragment())
		// } else {
		outputString := string(t.Fragment())
		// }

		err = writeString(outputString, varintBuf, w)
		if err != nil {
			return rc, err
		}
		GlobalLogger.Debug("write string: ", zap.String("fragment", outputString))

		err = writeUint(uint(rc.nodeCountIndexMap[t].count), varintBuf, w)
		if err != nil {
			return rc, err
		}
		GlobalLogger.Debug("writeUint: ", zap.Uint("count", uint(rc.nodeCountIndexMap[t].count)))
	}

	return rc, nil
}

func sortWritingNodes(gc *graphWritingContext) {
	gc.sortedNodes = make([]IBNode, 0, len(gc.ibNodeContexts))
	for t := range gc.ibNodeContexts {
		gc.sortedNodes = append(gc.sortedNodes, t)
	}
	sort.Slice(gc.sortedNodes, func(i, j int) bool {
		return lessIBNodeThan(gc, gc.sortedNodes[i], gc.sortedNodes[j])
	})
}

func sstWriteContent(graph NamedGraph, gc *graphWritingContext, w io.Writer) error {
	assignWritingNodeIndices(gc)
	for _, s := range gc.sortedNodes {
		GlobalLogger.Debug("IBNode", zap.String("fragment", s.fragOrID()))

		sc := gc.ibNodeContexts[s]
		err := writeUint(uint(sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount), &gc.varintBuf, w)
		if err != nil {
			return err
		}
		GlobalLogger.Debug("non-literal triple count: ", zap.Uint("value", uint(sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount)))
		writeIBNodePO := func(p IBNode, o IBNode) error {
			err = writeUint(uint(writeNodeIndex(graph, gc, p)), &gc.varintBuf, w)
			if err != nil {
				return err
			}
			GlobalLogger.Debug("write Pred: ", zap.Uint("index", uint(writeNodeIndex(graph, gc, p))), zap.String("fragment", p.fragOrID()))
			err = writeUint(uint(writeNodeIndex(graph, gc, o)), &gc.varintBuf, w)
			if err != nil {
				return err
			}
			GlobalLogger.Debug("write Obj: ", zap.Uint("index", uint(writeNodeIndex(graph, gc, o))), zap.String("fragment", o.fragOrID()))
			return nil
		}
		err = s.forMemberNodeRange(0, sc.termCollectionMemberCount, writeIBNodePO)
		if err != nil {
			return err
		}
		err = s.forPropNodeRange(sc.termCollectionMemberCount, sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount, writeIBNodePO)
		if err != nil {
			return err
		}
		err = writeUint(uint(sc.termCollectionLiteralMemberCount+sc.nonTermCollectionLiteralTripleCount+sc.nonTermCollectionLiteralCollectionCount), &gc.varintBuf, w)
		if err != nil {
			return err
		}
		GlobalLogger.Debug("literal triple count: ", zap.Uint("value", uint(sc.termCollectionLiteralMemberCount+sc.nonTermCollectionLiteralTripleCount+sc.nonTermCollectionLiteralCollectionCount)))

		literalCallback := func(p IBNode, o Literal) error {
			err = writeUint(uint(writeNodeIndex(graph, gc, p)), &gc.varintBuf, w)
			if err != nil {
				return err
			}
			GlobalLogger.Debug("write Pred: ", zap.Uint("index", uint(writeNodeIndex(graph, gc, p))), zap.String("fragment", p.fragOrID()))
			err = writeLiteral(o, &gc.varintBuf, w)
			if err != nil {
				return err
			}
			GlobalLogger.Debug("write Obj Literal: ", zap.String("value", literalToString(o)))
			return nil
		}
		literalColCallback := func(p IBNode, o *literalCollection) error {
			err = writeUint(uint(writeNodeIndex(graph, gc, p)), &gc.varintBuf, w)
			if err != nil {
				return err
			}
			GlobalLogger.Debug("write Pred: ", zap.Uint("index", uint(writeNodeIndex(graph, gc, p))), zap.String("fragment", p.fragOrID()))
			err = writeUint(uint(writtenLiteralCollection), &gc.varintBuf, w)
			if err != nil {
				return err
			}
			err = writeUint(uint(o.MemberCount()), &gc.varintBuf, w)
			if err != nil {
				return err
			}
			GlobalLogger.Debug("write LiteralCollection: ", zap.Uint("flag", uint(writtenLiteralCollection)), zap.Uint("memberCount", uint(o.MemberCount())))

			o.forInternalMembers(func(_ int, l Literal) {
				if err == nil {
					err = writeLiteral(l, &gc.varintBuf, w)
					GlobalLogger.Debug("write Literal: ", zap.String("value", literalToString(l)))
				}
			})
			return err
		}
		err = s.forMemberLiteralRange(0, sc.termCollectionMemberCount, sc.termCollectionLiteralMemberCount,
			func(predicate IBNode, object Term) error {
				switch o := object.(type) {
				case Literal:
					return literalCallback(predicate, o)
				case *literalCollection:
					return literalColCallback(predicate, o)
				default:
					panic(o)
				}
			})
		if err != nil {
			return err
		}
		err = s.forPropLiteralRange(
			sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount,
			sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount+sc.nonTermCollectionLiteralTripleCount,
			literalCallback,
		)
		if err != nil {
			return err
		}
		err = s.forPropLiteralCollectionRange(
			sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount+sc.nonTermCollectionLiteralTripleCount,
			sc.termCollectionMemberCount+sc.nonTermCollectionIBNodeTripleCount+sc.nonTermCollectionLiteralTripleCount+sc.nonTermCollectionLiteralCollectionCount,
			literalColCallback,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func assignWritingNodeIndices(gc *graphWritingContext) {
	for i, s := range gc.sortedNodes {
		si := gc.ibNodeContexts[s]
		si.index = i
		gc.ibNodeContexts[s] = si
	}
	offset := len(gc.sortedNodes)
	for _, graphImport := range gc.sortedImportNamedGraphs {
		offset = assignWritingRefNodeIndices(gc.importedNodeContexts[graphImport.ID()], offset)
	}
	for _, namedGraph := range gc.sortedReferencedGraphs {
		offset = assignWritingRefNodeIndices(gc.referencedGraphNodeContexts[namedGraph.IRI()], offset)
	}
}

func assignWritingRefNodeIndices(refNode writingNodeContext, offset int) int {
	for _, s := range refNode.sortedNodes {
		si := refNode.nodeCountIndexMap[s]
		si.index, offset = offset, offset+1
		refNode.nodeCountIndexMap[s] = si
	}
	return offset
}

func writeNodeIndex(graph NamedGraph, gc *graphWritingContext, s IBNode) int {
	sg := s.OwningGraph()
	if sg != nil {
		if _, found := graph.(*namedGraph).directImports[sg.ID()]; found {
			return gc.importedNodeContexts[sg.ID()].nodeCountIndexMap[s].index
		} else if sg == graph {
			return gc.ibNodeContexts[s].index
		} else {
			return gc.referencedGraphNodeContexts[sg.IRI()].nodeCountIndexMap[s].index
		}
	} else {
		panic("err")
	}
}

func writeLiteral(o Literal, varintBuf *varintBufT, w io.Writer) error {
	switch o.DataType() {
	case &literalTypeString.ibNode:
		err := writeUint(uint(writtenLiteralString), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeString(string(o.apiValue().(String)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeLangString.ibNode:
		err := writeUint(uint(writtenLiteralLangString), varintBuf, w)
		if err != nil {
			return err
		}
		ls := o.apiValue().(LangString)
		err = writeString(ls.Val, varintBuf, w)
		if err != nil {
			return err
		}
		err = writeString(ls.LangTag, varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeInteger.ibNode:
		err := writeUint(uint(writtenLiteralInteger), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeInt64(int64(o.apiValue().(Integer)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeDouble.ibNode:
		err := writeUint(uint(writtenLiteralDouble), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeFloat64(float64(o.apiValue().(Double)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeFloat.ibNode:
		err := writeUint(uint(writtenLiteralFloat), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeFloat32(float32(o.apiValue().(Float)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeByte.ibNode:
		err := writeUint(uint(writtenLiteralByte), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeByte(int8(o.apiValue().(Byte)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeShort.ibNode:
		err := writeUint(uint(writtenLiteralShort), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeShort(int16(o.apiValue().(Short)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeInt.ibNode:
		err := writeUint(uint(writtenLiteralInt), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeInt(int32(o.apiValue().(Int)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeLong.ibNode:
		err := writeUint(uint(writtenLiteralLong), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeLong(int64(o.apiValue().(Long)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeUnsignedByte.ibNode:
		err := writeUint(uint(writtenLiteralUnsignedByte), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeUnsignedByte(uint8(o.apiValue().(UnsignedByte)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeUnsignedShort.ibNode:
		err := writeUint(uint(writtenLiteralUnsignedShort), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeUnsignedShort(uint16(o.apiValue().(UnsignedShort)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeUnsignedInt.ibNode:
		err := writeUint(uint(writtenLiteralUnsignedInt), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeUnsignedInt(uint32(o.apiValue().(UnsignedInt)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeUnsignedLong.ibNode:
		err := writeUint(uint(writtenLiteralUnsignedLong), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeUnsignedLong(uint64(o.apiValue().(UnsignedLong)), varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeBoolean.ibNode:
		err := writeUint(uint(writtenLiteralBoolean), varintBuf, w)
		if err != nil {
			return err
		}
		var b uint
		if bool(o.apiValue().(Boolean)) {
			b = 1
		}
		err = writeUint(b, varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeDateTime.ibNode:
		err := writeUint(uint(writtenLiteralDateTime), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeString(o.apiValue().(TypedString).Val, varintBuf, w)
		if err != nil {
			return err
		}
	case &literalTypeDateTimeStamp.ibNode:
		err := writeUint(uint(writtenLiteralDateTimeStamp), varintBuf, w)
		if err != nil {
			return err
		}
		err = writeString(o.apiValue().(TypedString).Val, varintBuf, w)
		if err != nil {
			return err
		}
	default:
		panic("unsupported literal data type")
	}
	return nil
}

func writeString(str string, varintBuf *varintBufT, w io.Writer) error {
	err := writeUint(uint(len(str)), varintBuf, w)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(str))
	return err
}

func writeInt64(i int64, varintBuf *varintBufT, w io.Writer) error {
	n := binary.PutVarint(varintBuf[:], i)

	// make sure all data is written successfully,
	// because io.Writer.Write does not make sure that and the err may still be nil.
	off := 0
	for off < n {
		k, err := w.Write(varintBuf[off:n])
		off += k
		if err != nil {
			return err
		}
		if k == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func writeFloat64(f float64, varintBuf *varintBufT, w io.Writer) error {
	u := math.Float64bits(f)
	binary.BigEndian.PutUint64(varintBuf[:8], u)
	_, err := w.Write(varintBuf[:8])
	return err
}

func writeFloat32(f float32, varintBuf *varintBufT, w io.Writer) error {
	u := math.Float32bits(f)
	binary.BigEndian.PutUint32(varintBuf[:4], u)
	_, err := w.Write(varintBuf[:4])
	return err
}

func writeByte(v int8, varintBuf *varintBufT, w io.Writer) error {
	var buf [1]byte
	buf[0] = byte(v)
	_, err := w.Write(buf[:])
	return err
}

func writeShort(v int16, varintBuf *varintBufT, w io.Writer) error {
	binary.BigEndian.PutUint16(varintBuf[:2], uint16(v))
	_, err := w.Write(varintBuf[:2])
	return err
}

func writeInt(v int32, varintBuf *varintBufT, w io.Writer) error {
	binary.BigEndian.PutUint32(varintBuf[:4], uint32(v))
	_, err := w.Write(varintBuf[:4])
	return err
}

func writeLong(v int64, varintBuf *varintBufT, w io.Writer) error {
	binary.BigEndian.PutUint64(varintBuf[:8], uint64(v))
	_, err := w.Write(varintBuf[:8])
	return err
}

func writeUnsignedByte(v uint8, varintBuf *varintBufT, w io.Writer) error {
	var buf [1]byte
	buf[0] = v
	_, err := w.Write(buf[:])
	return err
}

func writeUnsignedShort(v uint16, varintBuf *varintBufT, w io.Writer) error {
	binary.BigEndian.PutUint16(varintBuf[:2], v)
	_, err := w.Write(varintBuf[:2])
	return err
}

func writeUnsignedInt(v uint32, varintBuf *varintBufT, w io.Writer) error {
	binary.BigEndian.PutUint32(varintBuf[:4], v)
	_, err := w.Write(varintBuf[:4])
	return err
}

func writeUnsignedLong(v uint64, varintBuf *varintBufT, w io.Writer) error {
	binary.BigEndian.PutUint64(varintBuf[:8], v)
	_, err := w.Write(varintBuf[:8])
	return err
}

func writeUint(i uint, varintBuf *varintBufT, w io.Writer) error {
	intLen := binary.PutUvarint(varintBuf[:], uint64(i))
	_, err := w.Write(varintBuf[:intLen])
	return err
}

// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type (
	referencedGraphMapWithIBNodes = map[NamedGraph][]IBNode
	blankNodeT                    = map[string]IBNode
)

type readToStageContext struct {
	blankNodes blankNodeT
}

func convertTripleSubjectInStage(sub rdfTypeSubject, stage Stage, context readToStageContext) (IBNode, error) {
	switch sub.rdfTermType() {
	case rdfTermIRI:
		var ng NamedGraph
		var ib IBNode
		var err error

		// split the IRI into baseIRI and fragment
		base, fragment := sub.(IRI).Split()

		// for subject IBNode, the NamedGraph is searched and created in the state of local NamedGraph
		ng = stage.NamedGraph(IRI(base))
		if ng == nil {
			// if not found and there is no local graphs in the Stage, create a new local NamedGraph
			if len(stage.NamedGraphs()) == 0 {
				ng = stage.CreateNamedGraph(IRI(base))
			} else {
				// if not found and there is already local graph in the Stage, return an error
				// because current RDFRead only support one NamedGraph in one TTL file
				return nil, fmt.Errorf("with subject %v %w", sub, ErrIllegalTripleSubject)
			}
		}

		// if fragment is empty, this IBNode is the NG Node of this NamedGraph
		// otherwise, this IBNode is the normal IRI IBNode
		if fragment == "" {
			ib = ng.GetIRINodeByFragment("")
			if ib == nil {
				ib, err = ng.(*namedGraph).createIRIStringNode("")
				if err != nil {
					return nil, err
				}
			}
		} else {
			// get the IBNode by Fragment
			ib = nodeForFragment(ng, fragment)
		}

		return ib, nil
	case rdfTermBlank:
		return blankToIBNodeInStage(sub.(rdfTypeBlank), stage, context), nil
	case rdfTermLiteral:
		fallthrough
	default:
		return nil, fmt.Errorf("with subject %v: %w", sub, ErrIllegalTripleSubject)
	}
}

func convertTripleObjectInStage(obj rdfTypeObject, stage Stage, context readToStageContext) (Term, error) {
	switch obj.rdfTermType() {
	case rdfTermIRI:
		o := iriToNodeInStage(obj.(IRI), stage)
		return o, nil
	case rdfTermBlank:
		o := blankToIBNodeInStage(obj.(rdfTypeBlank), stage, context)
		return o, nil
	case rdfTermLiteral:
		l := obj.(rdfTypeLiteral)
		return convertLiteral(l)
	default:
		panic(obj)
	}
}

func convertLiteral(l rdfTypeLiteral) (Term, error) {
	switch l.DataType.String() {
	case "http://www.w3.org/2001/XMLSchema#string":
		return String(l.String()), nil
	case "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString":
		return LangStringOf(l.String(), l.Lang()), nil
	case "http://www.w3.org/2001/XMLSchema#double", "http://www.w3.org/2001/XMLSchema#decimal":
		value, err := l.Typed()
		if err != nil {
			return nil, err
		}
		var double float64
		switch value := value.(type) {
		case string:
			double, err = strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, err
			}
		case float32:
			double = float64(value)
		case float64:
			double = value
		}
		return Double(double), nil
	case "http://www.w3.org/2001/XMLSchema#float":
		value, err := l.Typed()
		if err != nil {
			return nil, err
		}
		var f float32
		switch value := value.(type) {
		case string:
			double, err := strconv.ParseFloat(value, 32)
			if err != nil {
				return nil, err
			}
			f = float32(double)
		case float32:
			f = value
		case float64:
			f = float32(value)
		}
		return Float(f), nil
	case "http://www.w3.org/2001/XMLSchema#integer", "http://www.w3.org/2001/XMLSchema#int",
		"http://www.w3.org/2001/XMLSchema#byte", "http://www.w3.org/2001/XMLSchema#long":
		value, err := l.Typed()
		if err != nil {
			return nil, err
		}
		var integer int64
		switch value := value.(type) {
		case string:
			integer, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, err
			}
		case int:
			integer = int64(value)
		case int32:
			integer = int64(value)
		case int64:
			integer = value
		}
		return Integer(integer), nil
	case "http://www.w3.org/2001/XMLSchema#short":
		value, err := l.Typed()
		if err != nil {
			return nil, err
		}
		var integer int16
		switch value := value.(type) {
		case string:
			i, err := strconv.ParseInt(value, 10, 16)
			if err != nil {
				return nil, err
			}
			integer = int16(i)
		case int16:
			integer = value
		}
		return Short(integer), nil

	case "http://www.w3.org/2001/XMLSchema#unsignedByte", "http://www.w3.org/2001/XMLSchema#unsignedInt",
		"http://www.w3.org/2001/XMLSchema#unsignedLong", "http://www.w3.org/2001/XMLSchema#unsignedShort":
		value, err := l.Typed()
		if err != nil {
			return nil, err
		}
		var integer uint64
		switch value := value.(type) {
		case string:
			integer, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, err
			}
		case uint:
			integer = uint64(value)
		case uint32:
			integer = uint64(value)
		case uint64:
			integer = value
		}
		return Integer(integer), nil
	case "http://www.w3.org/2001/XMLSchema#boolean":
		value, err := l.Typed()
		if err != nil {
			return nil, err
		}
		var b bool
		switch value := value.(type) {
		case string:
			b, err = strconv.ParseBool(value)
			if err != nil {
				return nil, err
			}
		case bool:
			b = value
		}
		return Boolean(b), nil
	case "http://www.w3.org/2001/XMLSchema#dateTime":
		return TypedStringOf(l.String(), &literalTypeDateTime.ibNode), nil
	case "http://www.w3.org/2001/XMLSchema#dateTimeStamp":
		return TypedStringOf(l.String(), &literalTypeDateTimeStamp.ibNode), nil
	default:
		return nil, fmt.Errorf("with literal %v of type %v: %w", l, l.DataType, ErrUnsupportedLiteral)
	}
}

func iriToNodeInStage(iri IRI, stage Stage) IBNode {
	var ng NamedGraph
	var ib IBNode

	// split the IRI into baseIRI and fragment
	base, fragment := iri.Split()

	// search the NamedGraph in the state of local NamedGraph
	ng = stage.NamedGraph(IRI(base))
	if ng == nil {
		// if not found in local, search the NamedGraph in the state of referenced NamedGraph
		// if not found in referenced, create a new referenced NamedGraph
		ng = stage.referencedGraphByURI(base)
	}

	// if fragment is empty, this IBNode is the NG Node of this NamedGraph
	// otherwise, this IBNode is the normal IRI IBNode
	if fragment == "" {
		ib = ng.GetIRINodeByFragment("")
		if ib == nil {
			panic("NG Node not found")
		}
	} else {
		// get the IBNode by Fragment
		ib = nodeForFragment(ng, fragment)
	}
	return ib
}

// store the blank nodes into one special NamedGraph
// Note this is not correct according to RDF as a blank node cannot be referenced from another namedGraph
// consequently, later we have to move to where they belong
func blankToIBNodeInStage(blank rdfTypeBlank, stage Stage, context readToStageContext) IBNode {
	bGraph := stage.getFirstLoadedGraph()
	if bGraph == nil {
		panic("No main NamedGraph found in the provided TTL file")
	}
	blankLabel := blank.String()
	var d IBNode
	if d, found := context.blankNodes[blankLabel]; found {
		return d
	}
	blankUUID, err := uuid.Parse(blankLabel)
	if err == nil {
		d, err = bGraph.createBlankUuidNode(blankUUID)
		if err != nil {
			panic(err)
		}
	} else {
		d = bGraph.CreateBlankNode()
	}

	context.blankNodes[blankLabel] = d
	return d
}

func nodeForFragment(eg NamedGraph, fragment string) IBNode {
	s := eg.GetIRINodeByFragment(fragment)

	if s == nil {
		s = eg.CreateIRINode(fragment)
	}

	return s
}

var errRdfCollectionUnrecognized = errors.New("rdf collection unrecognized")

func extractCollections(stage Stage) {
	rdfNilN, err := stage.IBNodeByVocabulary(rdfNil)
	if err != nil {
		return
	}
	_ = rdfNilN.ForAll(func(_ int, sub, predicate IBNode, object Term) error {
		if !predicate.Is(rdfRest) || object != rdfNilN {
			// return errRdfCollectionUnrecognized
			// do nothing, just skip it, for condition that rdfNil itself present in TermCollection
			return nil
		} else {
			GlobalLogger.Debug("extractCollections: ", zap.String("subject", sub.iriOrID()), zap.String("predicate", predicate.iriOrID()), zap.String("object", termToString(object)))
			err := crawlCollectionUsers(sub, nil, nil)
			if err != nil {
				return err
			}
			return nil
		}
	})
}

func crawlCollectionUsers(sub IBNode, Literals []Literal, Terms []Term) error {
	var lUsers []struct{ s, p IBNode }
	tempLiterals, tempTerms := Literals, Terms
	var literalAppended, termAppended bool
	if len(Literals) == 0 && Literals != nil { // Special case if lValue collection failed at the upper level
		literalAppended = true
	}
	type po struct {
		predicate IBNode
		object    Term
	}
	var extraTriples []po
	err := sub.ForAll(func(_ int, subject, predicate IBNode, object Term) error {
		// subject triple
		if sub == subject {
			if !literalAppended && predicate.Is(rdfFirst) && object.TermKind() == TermKindLiteral {
				tempLiterals = append(tempLiterals, object.(Literal))
				literalAppended = true
				GlobalLogger.Debug("tempLiterals append:", zap.String("object", termToString(object)))
				return nil
			} else if !termAppended && predicate.Is(rdfFirst) {
				tempTerms = moveLiteralValuesToObjectMembers(tempLiterals, tempTerms)
				tempTerms = append(tempTerms, object)
				GlobalLogger.Debug("tempTerms append:", zap.String("object", termToString(object)))
				literalAppended, termAppended = true, true
				return nil
			}
			if !predicate.Is(rdfRest) {
				extraTriples = append(extraTriples, po{predicate, object})
			}
			return nil
		} else {
			// object and predicate triples
			lUsers = append(lUsers, struct{ s, p IBNode }{s: subject, p: predicate})
			GlobalLogger.Debug("lUsers append:", zap.String("subject", subject.iriOrID()), zap.String("predicate", predicate.iriOrID()), zap.String("object", termToString(object)))
			return nil
		}
	})
	if err != nil {
		return err
	}
	if len(tempLiterals) > len(Literals) && len(extraTriples) == 0 {
		dt := tempLiterals[0].DataType()
		for i, v := range tempLiterals {
			if i != 0 && v.DataType() != dt {
				tempTerms = moveLiteralValuesToObjectMembers(tempLiterals, tempTerms)
				literalAppended, termAppended = false, true
				break
			}
		}
	}
	var col Term
	var canDelete bool
	for _, up := range lUsers {
		if up.p.Is(rdfRest) && len(extraTriples) == 0 {
			lDownValues := tempLiterals
			if !literalAppended {
				lDownValues = tempLiterals[:0]
			}
			err = crawlCollectionUsers(up.s, lDownValues, tempTerms)
			if err != nil {
				return err
			}
			canDelete = true
			continue
		}
		if literalAppended && !termAppended && col == nil {
			members := make([]Literal, len(tempLiterals))
			for i, m := range tempLiterals {
				members[len(tempLiterals)-i-1] = m
			}
			col = NewLiteralCollection(members[0], members[1:]...)
		}
		if termAppended && col == nil {
			members := make([]Term, len(tempTerms))
			for i, m := range tempTerms {
				members[len(tempTerms)-i-1] = m
			}
			if graph := sub.OwningGraph(); graph != (nil) {
				c := graph.CreateCollection(members...)

				et := extraTriples
				c.(*ibNode).DeleteTriples()
				c.(*ibNode).addAll(func() (predicate *ibNode, object Term, ok bool) {
					if len(et) != 0 {
						predicate, object = et[0].predicate.(*ibNode), et[0].object
						et = et[1:]
						ok = true
					}
					return
				}, false)
				col = c
			} else {
				return errRdfCollectionUnrecognized
			}
		}
		if col == nil {
			return errRdfCollectionUnrecognized
		}
		up.s.AddStatement(up.p, col)
		GlobalLogger.Debug("addStatement Craw", zap.String("subject", up.s.iriOrID()),
			zap.String("predicate", up.p.iriOrID()), zap.String("object", termToString(col)))
		canDelete = true
	}
	if canDelete {
		sub.Delete()
	}
	return nil
}

func moveLiteralValuesToObjectMembers(lValues []Literal, oMembers []Term) []Term {
	oMembersLen := len(oMembers)
	oMembers = append(oMembers, make([]Term, len(lValues))...)[0:oMembersLen] // Reserve capacity of lValues
	for _, v := range lValues {
		oMembers = append(oMembers, v)
	}
	return oMembers
}

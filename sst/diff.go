// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

type deltaNodeCallback interface {
	RemovedTriple(predicate IBNode, object Term)
	AddedTriple(predicate IBNode, object Term)
}

type deltaGraphCallback interface {
	DeletedIBNode(s IBNode) (deltaNodeCallback, error)
	CompareIBNode(s1, s2 IBNode) (deltaNodeCallback, error)
	CreatedIBNode(s IBNode) (deltaNodeCallback, error)
}

type forPropSubjectRanger interface {
	forPropNodeRange(fromTriple, toTriple int, c func(p IBNode, o IBNode) error) error
}

type forPropLiteralRanger interface {
	forPropLiteralRange(fromTriple, toTriple int, c func(p IBNode, o Literal) error) error
}

type forPropLiteralCollectionRanger interface {
	forPropLiteralCollectionRange(fromTriple, toTriple int, c func(p IBNode, o *literalCollection) error) error
}

func deltaGraph(from, to NamedGraph, c deltaGraphCallback) error {
	fromNodes := from.GetSortedIBNodes()
	toNodes := to.GetSortedIBNodes()

	i, j := 0, 0
	for i < len(fromNodes) && j < len(toNodes) {
		tFrom := fromNodes[i]
		tTo := toNodes[j]
		switch compareIBNodes(tFrom, tTo) {
		case -1:
			err := deltaDeletedNode(tFrom, c)
			if err != nil {
				return err
			}
			i++
		case 0:
			err := deltaModifiedNode(tFrom, tTo, c)
			if err != nil {
				return err
			}
			i++
			j++
		default: // +1
			err := deltaCreatedNode(tTo, c)
			if err != nil {
				return err
			}
			j++
		}
	}
	return nil
}

func deltaModifiedNode(tFrom IBNode, tTo IBNode, c deltaGraphCallback) error {
	m, err := c.CompareIBNode(tFrom, tTo)
	if err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	saFrom := tFrom
	cFrom := saFrom.sortAndCountTriples(func(t IBNode) {}, func(t IBNode) {})
	saTo := tTo
	cTo := saTo.sortAndCountTriples(func(t IBNode) {}, func(t IBNode) {})
	err = deltaPropSubjects(saFrom, 0, cFrom.nonTermCollectionIBNodeTripleCount, saTo, 0, cTo.nonTermCollectionIBNodeTripleCount, m)
	if err != nil {
		return err
	}
	err = deltaPropLiterals(saFrom, cFrom.nonTermCollectionIBNodeTripleCount, cFrom.nonTermCollectionIBNodeTripleCount+cFrom.nonTermCollectionLiteralTripleCount,
		saTo, cTo.nonTermCollectionIBNodeTripleCount, cTo.nonTermCollectionIBNodeTripleCount+cTo.nonTermCollectionLiteralTripleCount, m)
	if err != nil {
		return err
	}
	return deltaPropLiteralCollections(
		saFrom,
		cFrom.nonTermCollectionIBNodeTripleCount+cFrom.nonTermCollectionLiteralTripleCount,
		cFrom.nonTermCollectionIBNodeTripleCount+cFrom.nonTermCollectionLiteralTripleCount+cFrom.nonTermCollectionLiteralCollectionCount,
		saTo,
		cTo.nonTermCollectionIBNodeTripleCount+cTo.nonTermCollectionLiteralTripleCount,
		cTo.nonTermCollectionIBNodeTripleCount+cTo.nonTermCollectionLiteralTripleCount+cTo.nonTermCollectionLiteralCollectionCount,
		m,
	)
}

func deltaCreatedNode(tTo IBNode, c deltaGraphCallback) error {
	m, err := c.CreatedIBNode(tTo)
	if err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	saTo := tTo
	cTo := saTo.sortAndCountTriples(func(t IBNode) {}, func(t IBNode) {})
	err = deltaPropSubjects(nil, 0, 0, saTo, 0, cTo.nonTermCollectionIBNodeTripleCount, m)
	if err != nil {
		return err
	}
	err = deltaPropLiterals(nil, 0, 0,
		saTo, cTo.nonTermCollectionIBNodeTripleCount, cTo.nonTermCollectionIBNodeTripleCount+cTo.nonTermCollectionLiteralTripleCount, m)
	if err != nil {
		return err
	}
	return deltaPropLiteralCollections(
		nil, 0, 0,
		saTo, cTo.nonTermCollectionIBNodeTripleCount+cTo.nonTermCollectionLiteralTripleCount,
		cTo.nonTermCollectionIBNodeTripleCount+cTo.nonTermCollectionLiteralTripleCount+cTo.nonTermCollectionLiteralCollectionCount, m,
	)
}

func deltaDeletedNode(tFrom IBNode, c deltaGraphCallback) error {
	m, err := c.DeletedIBNode(tFrom)
	if err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	saFrom := tFrom
	cFrom := saFrom.sortAndCountTriples(func(t IBNode) {}, func(t IBNode) {})
	err = deltaPropSubjects(saFrom, 0, cFrom.nonTermCollectionIBNodeTripleCount, nil, 0, 0, m)
	if err != nil {
		return err
	}
	err = deltaPropLiterals(saFrom, cFrom.nonTermCollectionIBNodeTripleCount, cFrom.nonTermCollectionIBNodeTripleCount+cFrom.nonTermCollectionLiteralTripleCount, nil, 0, 0, m)
	if err != nil {
		return err
	}
	return deltaPropLiteralCollections(
		saFrom,
		cFrom.nonTermCollectionIBNodeTripleCount+cFrom.nonTermCollectionLiteralTripleCount,
		cFrom.nonTermCollectionIBNodeTripleCount+cFrom.nonTermCollectionLiteralTripleCount+cFrom.nonTermCollectionLiteralCollectionCount,
		nil, 0, 0, m,
	)
}

func deltaPropSubjects(
	saFrom forPropSubjectRanger, fromFromTriple, fromToTriple int,
	saTo forPropSubjectRanger, toFromTriple, toToTriple int, m deltaNodeCallback,
) error {
	type fromPropSubsT []struct{ p, o IBNode }
	var fromPropSubs fromPropSubsT
	if saFrom != nil {
		fromPropSubs = make(fromPropSubsT, 0, fromToTriple-fromFromTriple)
		err := saFrom.forPropNodeRange(fromFromTriple, fromToTriple,
			func(p, o IBNode) error {
				fromPropSubs = append(fromPropSubs, struct{ p, o IBNode }{p, o})
				return nil
			})
		if err != nil {
			return err
		}
	}
	i := 0
	if saTo != nil {
		err := saTo.forPropNodeRange(toFromTriple, toToTriple,
			func(p, o IBNode) error {
				for i < len(fromPropSubs) {
					switch compareIBNodes(fromPropSubs[i].p, p) {
					case -1:
						m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
						i++
					case 0:
						switch compareIBNodes(fromPropSubs[i].o, o) {
						case -1:
							m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
							i++
						case 0:
							i++
							return nil
						default: // +1
							m.AddedTriple(p, o)
							return nil
						}
					default: // +1
						m.AddedTriple(p, o)
						return nil
					}
				}
				if i >= len(fromPropSubs) {
					m.AddedTriple(p, o)
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
	}
	for ; i < len(fromPropSubs); i++ {
		m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
	}
	return nil
}

func deltaPropLiterals(
	saFrom forPropLiteralRanger, fromFromTriple, fromToTriple int,
	saTo forPropLiteralRanger, toFromTriple, toToTriple int, m deltaNodeCallback,
) error {
	type fromPropSubsT struct {
		p IBNode
		o Literal
	}
	var fromPropSubs []fromPropSubsT
	if saFrom != nil {
		fromPropSubs = make([]fromPropSubsT, 0, fromToTriple-fromFromTriple)
		err := saFrom.forPropLiteralRange(fromFromTriple, fromToTriple,
			func(p IBNode, o Literal) error {
				fromPropSubs = append(fromPropSubs, fromPropSubsT{p, o})
				return nil
			},
		)
		if err != nil {
			return err
		}
	}
	i := 0
	if saTo != nil {
		err := saTo.forPropLiteralRange(toFromTriple, toToTriple,
			func(p IBNode, o Literal) error {
				for i < len(fromPropSubs) {
					switch compareIBNodes(fromPropSubs[i].p, p) {
					case -1:
						m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
						i++
					case 0:
						switch compareLiterals(fromPropSubs[i].o, o) {
						case -1:
							m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
							i++
						case 0:
							i++
							return nil
						default: // +1
							m.AddedTriple(p, o)
							return nil
						}
					default: // +1
						m.AddedTriple(p, o)
						return nil
					}
				}
				if i >= len(fromPropSubs) {
					m.AddedTriple(p, o)
				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	for ; i < len(fromPropSubs); i++ {
		m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
	}
	return nil
}

func deltaPropLiteralCollections(
	saFrom forPropLiteralCollectionRanger, fromFromTriple, fromToTriple int,
	saTo forPropLiteralCollectionRanger, toFromTriple, toToTriple int, m deltaNodeCallback,
) error {
	type fromPropSubsT struct {
		p IBNode
		o *literalCollection
	}
	var fromPropSubs []fromPropSubsT
	if saFrom != nil {
		fromPropSubs = make([]fromPropSubsT, 0, fromToTriple-fromFromTriple)
		err := saFrom.forPropLiteralCollectionRange(fromFromTriple, fromToTriple,
			func(p IBNode, o *literalCollection) error {
				fromPropSubs = append(fromPropSubs, fromPropSubsT{p, o})
				return nil
			},
		)
		if err != nil {
			return err
		}
	}
	i := 0
	if saTo != nil {
		err := saTo.forPropLiteralCollectionRange(toFromTriple, toToTriple,
			func(p IBNode, o *literalCollection) error {
				for i < len(fromPropSubs) {
					switch compareIBNodes(fromPropSubs[i].p, p) {
					case -1:
						m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
						i++
					case 0:
						switch compareLiteralCollections(fromPropSubs[i].o, o) {
						case -1:
							m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
							i++
						case 0:
							i++
							return nil
						default: // +1
							m.AddedTriple(p, o)
							return nil
						}
					default: // +1
						m.AddedTriple(p, o)
						return nil
					}
				}
				if i >= len(fromPropSubs) {
					m.AddedTriple(p, o)
				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	for ; i < len(fromPropSubs); i++ {
		m.RemovedTriple(fromPropSubs[i].p, fromPropSubs[i].o)
	}
	return nil
}

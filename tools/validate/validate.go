// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// Package validate contains the code to validate SST data contained in a Stage.
package validate

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"git.semanticstep.net/x/sst/sst"
	_ "git.semanticstep.net/x/sst/vocabularies/dict"
	"git.semanticstep.net/x/sst/vocabularies/owl"
	"git.semanticstep.net/x/sst/vocabularies/rdf"
	"git.semanticstep.net/x/sst/vocabularies/rdfs"
	"git.semanticstep.net/x/sst/vocabularies/xsd"
	"go.uber.org/zap"
)

var (
	ErrRDFTypeMissing = &tripleFormatterError{message: "rdf:type missing", format: "missing%*s%*s%*s rdf:type"}
	ErrDomainMismatch = &tripleFormatterError{message: "domain mismatch", format: "domain mismatch%*s%*s%*s"}
	ErrRangeMismatch  = &tripleFormatterError{message: "range mismatch", format: "range mismatch%*s%*s%*s"}
	errBreakFor       = errors.New("break for")
)

type ValidationName string

func validateAll(graph sst.NamedGraph, report *ValidateReport, log Logger) error {
	err := RdfType(graph, report, log)
	if err != nil {
		return err
	}
	err = DomainAndRange(graph, report, log)
	if err != nil {
		return err
	}
	return nil
}

type predicateObject struct {
	p sst.IBNode
	o sst.Term
}

func FunctionalProperty(graph sst.NamedGraph, report *ValidateReport, log Logger) error {
	const validationName = ValidationName("FunctionalProperty")
	err := log.LogForGraph(InfoEnterLevel, (graph), validationName)
	if err != nil {
		return err
	}

	inverseFunctionalPropMap := make(map[predicateObject]int)

	err = graph.ForAllIBNodes(func(d sst.IBNode) error {
		err := log.Log(InfoLevel, d)
		if err != nil {
			return err
		}
		// skip NG Node
		if d.IsIRINode() && d.Fragment() == "" {
			return nil
		}

		functionalPropMap := make(map[sst.IBNode]int)
		sst.GlobalLogger.Debug("functional map empty")

		err = d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if d != s {
				return nil
			}
			sst.GlobalLogger.Debug("", zap.String("subj", s.Fragment()), zap.String("pred", p.Fragment()))
			// if p is dictionary vocabulary, replace it
			if p.InVocabulary() != nil {
				p, err = sst.StaticDictionary().IBNodeByVocabulary(p.InVocabulary())
				if err != nil {
					return err
				}
			}
			// sst.DebugIBNode(p)
			if isFunctionalProperty(p, functionalPropMap) {
				for key, val := range functionalPropMap {
					if val > 1 {
						err := log.Log(ErrorLevel, d, ErrDomainMismatch, p, o)
						if err != nil {
							return err
						}
						report.Error(string(graph.IRI()), Finding{
							Kind:    KindFunctionalProperty,
							Rule:    RulePredicateFunctionalProperty,
							Message: "FunctionalProperty " + key.PrefixedFragment() + " must be used only once for a subject",
							S:       ibNodeValuesToLogString(graph, d),
							P:       ibNodeValuesToLogString(graph, p),
							O:       valuesToLogString(graph, o),
						})
					}
				}
			} else {
				sst.GlobalLogger.Debug("Not a functionalProperty", zap.String("", p.Fragment()))
			}

			if isInverseFunctionalProperty(p, o, inverseFunctionalPropMap) {
				for key, val := range inverseFunctionalPropMap {
					if val > 1 {
						err := log.Log(ErrorLevel, d, ErrDomainMismatch, p, o)
						if err != nil {
							return err
						}
						report.Error(string(graph.IRI()), Finding{
							Kind:    KindFunctionalProperty,
							Rule:    RulePredicateInverseFunctionalProperty,
							Message: "InverseFunctionalProperty " + key.p.PrefixedFragment() + " must be used only once for an object",
							S:       ibNodeValuesToLogString(graph, d),
							P:       ibNodeValuesToLogString(graph, p),
							O:       valuesToLogString(graph, o),
						})
					}
				}

			} else {
				sst.GlobalLogger.Debug("Not a InverseFunctionalProperty", zap.String("", p.Fragment()))
			}
			return nil
		})

		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return log.LogForGraph(InfoLeaveLevel, (graph), validationName)
}

func RdfType(graph sst.NamedGraph, report *ValidateReport, log Logger) error {
	const validationName = ValidationName("ValidateRdfType")
	err := log.LogForGraph(InfoEnterLevel, (graph), validationName)
	if err != nil {
		return err
	}
	err = graph.ForAllIBNodes(func(d sst.IBNode) error {
		err := log.Log(InfoLevel, d)
		if err != nil {
			return err
		}

		var types []sst.IBNode
		err = d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if d != s {
				return nil
			}
			if p.Is(rdf.Type) && (o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection) {
				types = append(types, o.(sst.IBNode))
				return nil
			}
			return nil
		})
		if err != nil {
			return err
		}

		// skip NG Node
		if d.IsIRINode() && d.Fragment() == "" {
			return nil
		}

		// skip TermCollection rdf:type checking
		if d.IsTermCollection() {
			return nil
		}

		if len(types) == 0 {
			err := log.Log(ErrorLevel, d, ErrRDFTypeMissing)
			if err != nil {
				return err
			}
			report.Error(string(graph.IRI()), Finding{
				Kind:    KindRdfType,
				Rule:    RuleRdfTypeMissing,
				Message: ibNodeValuesToLogString(graph, d) + " has no rdf:type",
				S:       ibNodeValuesToLogString(graph, d),
			})
		} else {
			isMainClass := false
			isPropertyType := false

			for _, t := range types {
				tElementInfo := t.InVocabulary()

				// a class from sst-Vocabulary that is identified as a ssmeta:MainClass
				if tElementInfo != nil && tElementInfo.IsMainClass(sst.Element{}) {
					isMainClass = true
					break
				}
			}

			// a rdf:Property, owl:ObjectProperty or owl:DatatypeProperty together with a statement
			// that the subject is a rdfs:subPropertyOf of a property defined in one of the SST Vocabularies or any subProperty of these.
			// The answer has to be found within the current dataset, otherwise it is an error.
			isPropertyType = isValidProperty(d)

			if isMainClass {
				sst.GlobalLogger.Debug("isMainClass found", zap.String("node", ibNodeValuesToLogString(graph, d)))
			}

			if isPropertyType {
				sst.GlobalLogger.Debug("isPropertyType found", zap.String("node", ibNodeValuesToLogString(graph, d)))
			}

			if !isMainClass && !isPropertyType {
				err := log.Log(ErrorLevel, d, ErrRDFTypeMissing)
				if err != nil {
					return err
				}
				report.Error(string(graph.IRI()), Finding{
					Kind:    KindRdfType,
					Rule:    RuleRdfTypeWrong,
					Message: ibNodeValuesToLogString(graph, d) + " does not contain a type that is either property or main class",
					S:       ibNodeValuesToLogString(graph, d),
				})
			}

		}
		return nil
	})
	if err != nil {
		return err
	}
	return log.LogForGraph(InfoLeaveLevel, (graph), validationName)
}

// value pass
func isFunctionalProperty(subj sst.IBNode, all map[sst.IBNode]int) bool {
	isFunctionalPropBool := false
	subj.ForAll(func(_ int, s, predPred sst.IBNode, o sst.Term) error {
		if subj == s {
			if predPred.Is(rdf.Type) && o.TermKind() == sst.TermKindIBNode {
				if o.(sst.IBNode).Is(owl.FunctionalProperty) {
					_, ok := all[subj]
					if ok {
						all[subj]++
					} else {
						all[subj] = 1
					}
					// sst.DebugIBNode(subj)
					sst.GlobalLogger.Debug("functional map modified", zap.String("", subj.Fragment()), zap.Int("", all[subj]))
					isFunctionalPropBool = true
					return nil
				}
			}
		}
		return nil
	})

	// if !isFunctionalPropBool {
	subj.ForAll(func(_ int, s, predPred sst.IBNode, oo sst.Term) (err error) {
		if subj == s {
			if predPred.Is(rdfs.SubPropertyOf) && oo.TermKind() == sst.TermKindIBNode {
				if oo.(sst.IBNode).InVocabulary() != nil {
					oo, err = sst.StaticDictionary().IBNodeByVocabulary(oo.(sst.IBNode).InVocabulary())
					if err != nil {
						return err
					}
				}
				isFunctionalPropBool = isFunctionalPropBool || isFunctionalProperty(oo.(sst.IBNode), all)
			}
		}
		return nil
	})
	// }

	return isFunctionalPropBool
}

func isInverseFunctionalProperty(subj sst.IBNode, obj sst.Term, all map[predicateObject]int) bool {
	isInverseFunctionalPropBool := false
	subj.ForAll(func(_ int, s, predPred sst.IBNode, o sst.Term) error {
		if subj == s {
			if predPred.Is(rdf.Type) && o.TermKind() == sst.TermKindIBNode {
				if o.(sst.IBNode).Is(owl.InverseFunctionalProperty) {
					_, ok := all[predicateObject{subj, obj}]
					if ok {
						all[predicateObject{subj, obj}]++
					} else {
						all[predicateObject{subj, obj}] = 1
					}

					sst.GlobalLogger.Debug("inverseFunctional map modified",
						zap.String("", subj.Fragment()), zap.String("", obj.(sst.IBNode).Fragment()),
						zap.Int("", all[predicateObject{subj, obj}]))
					isInverseFunctionalPropBool = true
					return nil
				}
			}
		}
		return nil
	})

	// if !isInverseFunctionalPropBool {
	subj.ForAll(func(_ int, s, predPred sst.IBNode, oo sst.Term) (err error) {
		if subj == s {
			if predPred.Is(rdfs.SubPropertyOf) && oo.TermKind() == sst.TermKindIBNode {
				if oo.(sst.IBNode).InVocabulary() != nil {
					oo, err = sst.StaticDictionary().IBNodeByVocabulary(oo.(sst.IBNode).InVocabulary())
					if err != nil {
						return err
					}
				}
				isInverseFunctionalPropBool = isInverseFunctionalPropBool || isInverseFunctionalProperty(oo.(sst.IBNode), obj, all)
			}
		}
		return nil
	})
	// }

	return isInverseFunctionalPropBool
}

func isValidProperty(subj sst.IBNode) bool {
	isPropertyBool := false
	subj.ForAll(func(_ int, s, predPred sst.IBNode, o sst.Term) error {
		if subj == s {
			if predPred.Is(rdf.Type) && o.TermKind() == sst.TermKindIBNode {
				if o.(sst.IBNode).Is(rdf.Property) || o.(sst.IBNode).Is(owl.ObjectProperty) || o.(sst.IBNode).Is(owl.DatatypeProperty) {

					subj.ForAll(func(_ int, s, predPred sst.IBNode, oo sst.Term) error {
						if subj == s {
							if predPred.Is(rdfs.SubPropertyOf) && oo.TermKind() == sst.TermKindIBNode {
								oVoc := oo.(sst.IBNode).InVocabulary()
								if oVoc != nil {
									if oVoc.IsProperty() {
										isPropertyBool = true
										return nil
									}
								} else {
									isPropertyBool = isValidProperty(oo.(sst.IBNode))
									return nil
								}
							}
						}
						return nil
					})

				}
				return nil
			}
		}

		return nil
	})

	return isPropertyBool
}

func literalCheck(rang sst.ElementInformer, li sst.Literal) (expected string, real string, ok bool) {
	// if rang.IsDatatype() {
	switch rang.(type) {
	case rdf.KindLangString:
		expected = "rdf:langString"
	case xsd.KindString:
		expected = "xsd:string"
	case xsd.KindDouble:
		expected = "xsd:double"
	case xsd.KindInteger:
		expected = "xsd:integer"
	case xsd.KindBoolean:
		expected = "xsd:boolean"
	case rdfs.KindLiteral: // range is Literal will always pass, because li is Literal
		expected = "rdfs:Literal"
		return "", "", true
	}
	// }

	switch li.(type) {
	case sst.LangString:
		_, ok = rang.(rdf.KindLangString)
		real = "rdf:langString"
	case sst.String:
		_, ok = rang.(xsd.KindString)
		real = "xsd:string"
	case sst.Double:
		_, ok = rang.(xsd.KindDouble)
		real = "xsd:double"
	case sst.Integer:
		_, ok = rang.(xsd.KindInteger)
		real = "xsd:integer"
	case sst.Boolean:
		_, ok = rang.(xsd.KindBoolean)
		real = "xsd:boolean"
	default:
		// ok = true
	}

	return
}

func DomainAndRange(graph sst.NamedGraph, report *ValidateReport, log Logger) error {
	const validationName = ValidationName("ValidateDomainAndRange")
	err := log.LogForGraph(InfoEnterLevel, (graph), validationName)
	if err != nil {
		return err
	}
	err = graph.ForAllIBNodes(func(d sst.IBNode) error {
		var types []sst.IBNode
		type predicateObject struct {
			p sst.IBNode
			o sst.Term
		}
		var predicateObjectTuples []predicateObject
		err = d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if d != s {
				return nil
			}

			// skip TermCollection rdf:type checking
			// if d.IsTermCollection() {
			// return nil
			// }

			if p.Is(rdf.Type) && (o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection) {
				types = append(types, o.(sst.IBNode))
				return nil
			}
			predicateObjectTuples = append(predicateObjectTuples, predicateObject{p: p, o: o})

			pElementInfo := p.InVocabulary()
			pIsProperty := false

			// check in dictionary
			if pElementInfo != nil {
				if pElementInfo.IsProperty() {
					pIsProperty = true
				}
			} else { // if p is not in vocabulary and from referenced graph
				if p.OwningGraph().IsReferenced() {
					err := log.Log(ErrorLevel, d, ErrDomainMismatch, p, o)
					if err != nil {
						return err
					}
					report.Error(string(graph.IRI()), Finding{
						Kind:    KindDomainRange,
						Rule:    RulePredicateNotKnown,
						Message: "predicate of an unknown type",
						S:       ibNodeValuesToLogString(graph, d),
						P:       ibNodeValuesToLogString(graph, p),
						O:       valuesToLogString(graph, o),
					})
					return nil
				}
			}

			// check in application Data
			if !pIsProperty {
				pIsProperty = isValidProperty(p)
			}

			if !pIsProperty {
				err := log.Log(ErrorLevel, d, ErrDomainMismatch, p, o)
				if err != nil {
					return err
				}
				report.Error(string(graph.IRI()), Finding{
					Kind:    KindDomainRange,
					Rule:    RulePredicateNotProperty,
					Message: "the predicate is not a valid property",
					S:       ibNodeValuesToLogString(graph, d),
					P:       ibNodeValuesToLogString(graph, p),
					O:       valuesToLogString(graph, o),
				})
			}

			return nil
		})
		if err != nil {
			return err
		}
		for _, po := range predicateObjectTuples {
			pi := po.p.InVocabulary()
			// when p is rdf.first, means the subject is the TermCollection BlankNode, so skip checking its domain
			if pi != nil && !po.p.Is(rdf.First) {
				if domain := pi.Domain(); domain != nil {
					err := log.Log(InfoLevel, d, "domain", po.p, po.o)
					if err != nil {
						return err
					}
					var found bool
					for _, t := range types {
						if t.IsKind(domain) {
							found = true
							break
						}
					}
					if !found {
						err := log.Log(ErrorLevel, d, ErrDomainMismatch, po.p, po.o)
						if err != nil {
							return err
						}
						report.Error(string(graph.IRI()), Finding{
							Kind:    KindDomainRange,
							Rule:    RuleDomainMismatch,
							Message: "domain mismatch",
							S:       ibNodeValuesToLogString(graph, d),
							P:       ibNodeValuesToLogString(graph, po.p),
							O:       valuesToLogString(graph, po.o),
						})
					}
				}
				if rang := pi.Range(); rang != nil {
					switch po.o.TermKind() {
					case sst.TermKindIBNode:
						o := po.o.(sst.IBNode)
						if o.OwningGraph().IsReferenced() {
							v, err := sst.StaticDictionary().Element(o.InVocabulary().VocabularyElement())
							if v != nil && err == nil {
								o = v
							}
						}
						if !o.OwningGraph().IsReferenced() {
							err := log.Log(InfoLevel, d, "range", po.p, po.o)
							if err != nil {
								return err
							}
							var found bool
							err = o.ForAll(func(_ int, os, op sst.IBNode, oo sst.Term) error {
								if o != os {
									return nil
								}
								if op.Is(rdf.Type) && (oo.TermKind() == sst.TermKindIBNode || oo.TermKind() == sst.TermKindTermCollection) {
									if oo.(sst.IBNode).IsKind(rang) {
										found = true
										return errBreakFor
									}
								}
								return nil
							})
							if err != nil && err != errBreakFor { // nolint:errorlint
								return err
							}
							if !found {
								err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, o)
								if err != nil {
									return err
								}
								report.Error(string(graph.IRI()), Finding{
									Kind:    KindDomainRange,
									Rule:    RuleRangeMismatch,
									Message: "Range mismatch",
									S:       ibNodeValuesToLogString(graph, d),
									P:       ibNodeValuesToLogString(graph, po.p),
									O:       valuesToLogString(graph, po.o),
								})
							}
						}

					case sst.TermKindTermCollection:
						tc := po.o.(sst.TermCollection)

						_, isList := rang.(rdf.KindList)

						if !isList {
							err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, po.o)
							if err != nil {
								return err
							}
							report.Error(string(graph.IRI()), Finding{
								Kind:    KindDomainRange,
								Rule:    RuleRangeMismatch,
								Message: "Range of predicate" + valuesToLogString(graph, po.p) + " is not a TermCollection",
								S:       ibNodeValuesToLogString(graph, d),
								P:       ibNodeValuesToLogString(graph, po.p),
								O:       "",
							})
							return nil
						}

						rang = rang.CollectionMember()

						tc.ForMembers(func(e int, o sst.Term) {
							foundDesiredType := false
							switch o.TermKind() {
							case sst.TermKindIBNode:
								err = o.(sst.IBNode).ForAll(func(_ int, os, op sst.IBNode, oo sst.Term) error {
									if o != os {
										return nil
									}
									if op.Is(rdf.Type) && (oo.TermKind() == sst.TermKindIBNode || oo.TermKind() == sst.TermKindTermCollection) {
										if oo.(sst.IBNode).IsKind(rang) {
											foundDesiredType = true
											return errBreakFor
										}
									}
									return nil
								})
								if !foundDesiredType {
									err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, o)
									if err != nil {
										return
									}
									report.Error(string(graph.IRI()), Finding{
										Kind:    KindDomainRange,
										Rule:    RuleRangeMismatch,
										Message: "Term Collection member type mismatch" + valuesToLogString(graph, o),
										S:       ibNodeValuesToLogString(graph, d),
										P:       ibNodeValuesToLogString(graph, po.p),
										O:       valuesToLogString(graph, o),
									})
								}
							case sst.TermKindTermCollection:
								fmt.Printf("KindIBNode:   %s\n", o.(sst.IBNode).IRI())

							case sst.TermKindLiteral:
								// fmt.Printf("KindLiteral:  %q^^%s\n", o.(sst.Literal), o.(sst.Literal).DataType().IRI())
								err := log.Log(InfoLevel, d, "range", po.p, o)
								if err != nil {
									return
								}

								expected, real, ok := literalCheck(rang, o.(sst.Literal))

								if !ok {
									err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, po.o)
									if err != nil {
										return
									}
									report.Error(string(graph.IRI()), Finding{
										Kind:    KindDomainRange,
										Rule:    RuleRangeMismatch,
										Message: "Range mismatch, expected datatype " + expected + ", found datatype " + real,
										S:       ibNodeValuesToLogString(graph, d),
										P:       ibNodeValuesToLogString(graph, po.p),
										O:       valuesToLogString(graph, o),
									})
								}

							case sst.TermKindLiteralCollection:
								fmt.Printf("KindLiteralCollection:   %s\n", reflect.TypeOf(o))

							default:
								fmt.Printf("default:    %s\n", o)
							}

						})

					case sst.TermKindLiteral:
						err := log.Log(InfoLevel, d, "range", po.p, po.o)
						if err != nil {
							return err
						}

						expected, real, ok := literalCheck(rang, po.o.(sst.Literal))

						if !ok {
							err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, po.o)
							if err != nil {
								return err
							}
							report.Error(string(graph.IRI()), Finding{
								Kind:    KindDomainRange,
								Rule:    RuleRangeMismatch,
								Message: "Range mismatch, expected datatype " + expected + ", found datatype " + real,
								S:       ibNodeValuesToLogString(graph, d),
								P:       ibNodeValuesToLogString(graph, po.p),
								O:       valuesToLogString(graph, po.o),
							})
						}
					case sst.TermKindLiteralCollection:
						err := log.Log(InfoLevel, d, "range", po.p, po.o)
						if err != nil {
							return err
						}
						if _, ok := rang.(rdf.KindList); !ok {
							err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, po.o)
							if err != nil {
								return err
							}
							report.Error(string(graph.IRI()), Finding{
								Kind:    KindDomainRange,
								Rule:    RuleRangeMismatch,
								Message: "Range mismatch",
								S:       ibNodeValuesToLogString(graph, d),
								P:       ibNodeValuesToLogString(graph, po.p),
								O:       valuesToLogString(graph, po.o),
							})
							return nil
						}

						rang = rang.CollectionMember()

						tlc := po.o.(sst.LiteralCollection)

						expected, real, ok := literalCheck(rang, tlc.Member(0))

						if !ok {
							err := log.Log(ErrorLevel, d, ErrRangeMismatch, po.p, po.o)
							if err != nil {
								return err
							}

							report.Error(string(graph.IRI()), Finding{
								Kind:    KindDomainRange,
								Rule:    RuleRangeMismatch,
								Message: "Range mismatch, expected datatype " + expected + ", found datatype " + real,
								S:       ibNodeValuesToLogString(graph, d),
								P:       ibNodeValuesToLogString(graph, po.p),
								O:       valuesToLogString(graph, po.o),
							})
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return log.LogForGraph(InfoLeaveLevel, (graph), validationName)
}

func ExperimentalNamedGraphForTypeDefinitions(graph sst.NamedGraph, log Logger) error {
	const validationName = ValidationName("ValidateNamedGraphForTypeDefinitions")
	err := log.LogForGraph(InfoEnterLevel, (graph), validationName)
	if err != nil {
		return err
	}
	err = graph.ForIRINodes(func(ibS sst.IBNode) error {
		var isClass bool
		var isDatatypeProp bool
		var isObjectProperty bool
		var isIndividual bool
		var out []string
		err := ibS.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if s == ibS { // not inverse
				switch o.TermKind() {
				case sst.TermKindIBNode, sst.TermKindTermCollection:
					// fmt.Printf("    %s %s\n", p.IRI(), o.(sst.IBNode).IRI())
					o := o.(sst.IBNode)
					if p.Is(rdf.Type) {
						isIndividual = true
						if o.Is(owl.DatatypeProperty) {
							isDatatypeProp = true
						}
						if o.Is(owl.ObjectProperty) {
							isObjectProperty = true
						}
						if o.Is(owl.Class) {
							isClass = true
						}
					}
					if p.Is(rdfs.SubClassOf) {
						isIndividual = true
					}
					if p.Is(rdfs.SubPropertyOf) {
						isObjectProperty = true
					}
				case sst.TermKindLiteral, sst.TermKindLiteralCollection:
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if isIndividual {
			out = append(out, "individual")
		}
		if isClass {
			out = append(out, "class")
		}
		if isObjectProperty {
			out = append(out, "objectProperty")
		}
		if isDatatypeProp {
			out = append(out, "class")
		}
		return log.Log(InfoLevel, ibS, strings.Join(out, " "))
	})
	if err != nil {
		return err
	}
	return log.LogForGraph(InfoLeaveLevel, (graph), validationName)
}

func ExperimentalNamedGraphForAcyclic(graph sst.NamedGraph, log Logger) error {
	const validationName = ValidationName("ValidateNamedGraphForAcyclic")
	err := log.LogForGraph(InfoEnterLevel, (graph), validationName)
	if err != nil {
		return err
	}
	err = graph.ForIRINodes(func(ibS sst.IBNode) error {
		return ibS.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if s == ibS { // not inverse
				switch o.TermKind() {
				case sst.TermKindIBNode, sst.TermKindTermCollection:
					err := log.Logf(InfoLevel, ibS, "%s %s", p.IRI(), o.(sst.IBNode).IRI())
					if err != nil {
						return err
					}
				case sst.TermKindLiteral:
					err := log.Logf(InfoLevel, ibS, "%s %q^^%s", p.IRI(), o.(sst.Literal), o.(sst.Literal).DataType().IRI())
					if err != nil {
						return err
					}
				case sst.TermKindLiteralCollection:
					err := log.Logf(InfoLevel, ibS, "%s %q^^%s\n", p.IRI(), o.(sst.LiteralCollection).Values(), o.(sst.LiteralCollection).Member(0).DataType().IRI())
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	return log.LogForGraph(InfoLeaveLevel, (graph), validationName)
}

func wave(done map[sst.IBNode]int, ib sst.IBNode, iGraph int) (int, error) {
	var count int
	_, found := done[ib]
	if !found { // not done yet?
		done[ib] = iGraph // mark done
		count++
		err := ib.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if ib.OwningGraph() == p.OwningGraph() {
				c, err := wave(done, p, iGraph)
				if err != nil {
					return err
				}
				count += c
			}
			if s == ib { // not inverse
				switch o.TermKind() {
				case sst.TermKindIBNode, sst.TermKindTermCollection:
					if ib.OwningGraph() == o.(sst.IBNode).OwningGraph() {
						c, err := wave(done, o.(sst.IBNode), iGraph)
						if err != nil {
							return err
						}
						count += c
					}
				case sst.TermKindLiteral, sst.TermKindLiteralCollection:
				}
			} else if ib.OwningGraph() == s.OwningGraph() {
				c, err := wave(done, s, iGraph)
				if err != nil {
					return err
				}
				count += c
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func ExperimentalNamedGraphForConnectedGraph(graph sst.NamedGraph, log Logger) error {
	const validationName = ValidationName("ValidateNamedGraphForConnectedGraph")
	err := log.LogForGraph(InfoEnterLevel, (graph), validationName)
	if err != nil {
		return err
	}
	var iGraph int
	done := make(map[sst.IBNode]int)
	err = graph.ForIRINodes(func(ibS sst.IBNode) error {
		_, found := done[ibS]
		if !found {
			iGraph++
			count, err := wave(done, ibS, iGraph)
			if err != nil {
				return err
			}
			err = log.Logf(InfoLevel, ibS, "%d ", count)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = log.LogfForGraph(InfoLevel, (graph), "No of connected graphs = %d ", iGraph)
	if err != nil {
		return err
	}
	err = graph.ForIRINodes(func(ibS sst.IBNode) error {
		i, found := done[ibS]
		if !found {
			i = 0
		}
		err := log.Logf(InfoLevel, ibS, "%d", i)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return log.LogForGraph(InfoLeaveLevel, (graph), validationName)
}

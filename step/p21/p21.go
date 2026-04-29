// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// import STEP file in p21/stp format containing AP242 data (ISO 10303-242) into SST
package p21

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/semanticstep/sst-core/sst"
	"github.com/semanticstep/sst-core/vocabularies/lci"
	"github.com/semanticstep/sst-core/vocabularies/owl"
	"github.com/semanticstep/sst-core/vocabularies/qau"
	"github.com/semanticstep/sst-core/vocabularies/rdf"
	"github.com/semanticstep/sst-core/vocabularies/rdfs"
	"github.com/semanticstep/sst-core/vocabularies/rep"
	"github.com/semanticstep/sst-core/vocabularies/ssmeta"
	"github.com/semanticstep/sst-core/vocabularies/sso"
)

// Install ebnf2y with the following command
// > go install modernc.org/ebnf2y@latest
//go:generate -command ebnf2y $GOPATH/bin/ebnf2y

// Install goyacc with the following command
// > go install golang.org/x/tools/cmd/goyacc@latest
//go:generate -command goyacc $GOPATH/bin/goyacc

// Install golex with the following command
// > go install modernc.org/golex@latest
//go:generate -command golex $GOPATH/bin/golex

//-go:generate ebnf2y -start EXCHANGE_FILE -o iso10303p21y2016-template.y iso10303p21y2016.ebnf
//go:generate golex -o iso10303p21y2016lexer.go iso10303p21y2016.l
//go:generate goyacc -l -o iso10303p21y2016parser.go iso10303p21y2016.y

type parserInstanceMode int

const (
	parserNoInstanceMode parserInstanceMode = iota
	parserSimpleEntityInstance
	parserComplexEntityInstance
)

var ErrInvalidPosition = errors.New("invalid position")

type parserKeywordType int

const (
	parserKeywordNoType parserKeywordType = iota
	parserEntity
	parserDefinedType
)

type sstData struct {
	graph                sst.NamedGraph
	instanceMap          map[int64]sst.IBNode
	entityMap            map[string]sst.IBNode
	definedTypeMap       map[string]sst.IBNode
	enumerationValueMap  map[string]sst.IBNode
	instanceMode         parserInstanceMode
	instanceValue        sst.IBNode
	instanceActual       sst.IBNode
	entityActual         sst.IBNode
	entityValueActual    sst.IBNode
	definedTypeActual    sst.IBNode
	definedTypeNesting   int
	stringValue          string // also standard_keyword, enumeration, logical
	realValue            float64
	integerValue         int64
	booleanValue         bool
	attributeValues      []sst.Term
	attributeValuesStack [][]sst.Term
	keywordType          parserKeywordType
}

func (d *sstData) pushAttributeValues() {
	d.attributeValuesStack = append(d.attributeValuesStack, d.attributeValues)
	d.attributeValues = nil
}

func (d *sstData) popAttributeValues() []sst.Term {
	v := d.attributeValues
	d.attributeValues = d.attributeValuesStack[len(d.attributeValuesStack)-1]
	d.attributeValuesStack = d.attributeValuesStack[:len(d.attributeValuesStack)-1]
	return v
}

func newSSTData(graph sst.NamedGraph) *sstData {
	return &sstData{
		graph:               graph,
		instanceMap:         map[int64]sst.IBNode{},
		entityMap:           map[string]sst.IBNode{},
		definedTypeMap:      map[string]sst.IBNode{},
		enumerationValueMap: map[string]sst.IBNode{},
	}
}

var ErrParsingFailed = errors.New("parsing failed")

type ErrorReporter interface {
	Println(v ...interface{})
}

type (
	OntologyObjectType int
	OntologyType       int
)

const (
	EntityType OntologyObjectType = iota
	EnumerationValueType
	DefinedTypeType
)

const (
	MainClass OntologyType = iota
	ObjectProperty
)

const (
	PART                                               = "part"
	SUPER                                              = "SUPER"
	RANGE                                              = "range"
	CUBIC                                              = "cubic"
	SQUARE                                             = "square"
	DOMAIN                                             = "domain"
	DESIGN                                             = "design"
	PRODUCT                                            = "PRODUCT"
	SI_UNIT                                            = "SI_UNIT"
	VERSION                                            = "VERSION"
	EDGE_LOOP                                          = "EDGE_LOOP"
	ATTRIBUTE                                          = "ATTRIBUTE"
	NAMED_UNIT                                         = "NAMED_UNIT"
	SHAPE_ASPECT                                       = "SHAPE_ASPECT"
	PRODUCT_DEFINITION                                 = "PRODUCT_DEFINITION"
	SHAPE_REPRESENTATION                               = "SHAPE_REPRESENTATION"
	PRODUCT_DEFINITION_SHAPE                           = "PRODUCT_DEFINITION_SHAPE"
	PRODUCT_DEFINITION_CONTEXT                         = "PRODUCT_DEFINITION_CONTEXT"
	REPRESENTATION_RELATIONSHIP                        = "REPRESENTATION_RELATIONSHIP"
	MEASURE_REPRESENTATION_ITEM                        = "MEASURE_REPRESENTATION_ITEM"
	ITEM_DEFINED_TRANSFORMATION                        = "ITEM_DEFINED_TRANSFORMATION"
	GLOBAL_UNIT_ASSIGNED_CONTEXT                       = "GLOBAL_UNIT_ASSIGNED_CONTEXT"
	PRODUCT_DEFINITION_FORMATION                       = "PRODUCT_DEFINITION_FORMATION"
	NEXT_ASSEMBLY_USAGE_OCCURRENCE                     = "NEXT_ASSEMBLY_USAGE_OCCURRENCE"
	SHAPE_DEFINITION_REPRESENTATION                    = "SHAPE_DEFINITION_REPRESENTATION"
	PRODUCT_RELATED_PRODUCT_CATEGORY                   = "PRODUCT_RELATED_PRODUCT_CATEGORY"
	SHAPE_REPRESENTATION_RELATIONSHIP                  = "SHAPE_REPRESENTATION_RELATIONSHIP"
	PROPERTY_DEFINITION_REPRESENTATION                 = "PROPERTY_DEFINITION_REPRESENTATION"
	CONTEXT_DEPENDENT_SHAPE_REPRESENTATION             = "CONTEXT_DEPENDENT_SHAPE_REPRESENTATION"
	REPRESENTATION_RELATIONSHIP_WITH_TRANSFORMATION    = "REPRESENTATION_RELATIONSHIP_WITH_TRANSFORMATION"
	PRODUCT_DEFINITION_FORMATION_WITH_SPECIFIED_SOURCE = "PRODUCT_DEFINITION_FORMATION_WITH_SPECIFIED_SOURCE"
)

type ExpressObject struct {
	name       string
	objectType OntologyObjectType
}

type DefinedType struct {
	ExpressObject
}

type EnumerationValue struct {
	ExpressObject
}

type RawAttributeValues struct {
	name        string
	MixedValues []interface{}
}

type SingleEntity struct {
	name            string
	superType       bool
	ontologyType    OntologyType
	ontologyObject  sst.IBNode
	attributeOrders []sst.IBNode
}
type ExtraEntity struct {
	name           string
	ontologyObject sst.IBNode
}

// TreeNode is a structure that will represent either an individual IBNode or a collection of TreeNodes
type TreeNode struct {
	Value    sst.Term
	Children []*TreeNode
}

type conversionParameters struct {
	graph                 sst.NamedGraph
	singleEntityMap       map[sst.IBNode]SingleEntity
	extraEntityMap        map[sst.IBNode]ExtraEntity
	enumerationValueMap   map[sst.IBNode]EnumerationValue
	definedTypeMap        map[sst.IBNode]DefinedType
	rawAttributeValuesMap map[sst.IBNode]RawAttributeValues
	complexInstanceValues map[sst.IBNode][]sst.IBNode
	singleInstanceValues  map[sst.IBNode][]sst.IBNode
	collectComplexNodes   map[sst.IBNode][]sst.IBNode
	enumerationCache      map[string]sst.IBNode
}

func newConversionParameters(graph sst.NamedGraph) *conversionParameters {
	cp := new(conversionParameters)
	cp.graph = graph
	cp.singleEntityMap = make(map[sst.IBNode]SingleEntity)
	cp.extraEntityMap = make(map[sst.IBNode]ExtraEntity)
	cp.enumerationValueMap = make(map[sst.IBNode]EnumerationValue)
	cp.definedTypeMap = make(map[sst.IBNode]DefinedType)
	cp.rawAttributeValuesMap = make(map[sst.IBNode]RawAttributeValues)
	cp.complexInstanceValues = make(map[sst.IBNode][]sst.IBNode)
	cp.singleInstanceValues = make(map[sst.IBNode][]sst.IBNode)
	cp.collectComplexNodes = make(map[sst.IBNode][]sst.IBNode)
	cp.enumerationCache = make(map[string]sst.IBNode)

	return cp
}

func dumpParserResult(parserResult parserResultT) {
	s := fmt.Sprintf("%#v", parserResult)
	var buf bytes.Buffer
	subS := s
	var indent int
	for {
		i := strings.IndexAny(subS, "{}")
		if i < 0 {
			break
		}
		buf.WriteString(subS[0 : i+1])
		switch subS[i] {
		case '{':
			indent += 2
			buf.WriteByte('\n')
			for j := 0; j < indent; j++ {
				buf.WriteByte(' ')
			}
		case '}':
			indent -= 2
			buf.WriteByte('\n')
			for j := 0; j < indent; j++ {
				buf.WriteByte(' ')
			}
		}
		subS = subS[i+1:]
	}
	buf.WriteString("\n")
	a := strings.Split(buf.String(), "\n")
	for _, v := range a {
		if strings.HasSuffix(v, "(nil)") || strings.HasSuffix(v, "(nil),") {
			continue
		}

		fmt.Println(v)
	}
}

func Parse(src *bufio.Reader, errorReporter ErrorReporter) (graph sst.NamedGraph, err error) {
	stage := sst.OpenStage(sst.DefaultTriplexMode)
	if stage == nil {
		return nil, fmt.Errorf("stage is nil after OpenStage")
	}

	graph = stage.CreateNamedGraph("")

	if graph == nil {
		return nil, fmt.Errorf("graph is nil after CreateNamedGraph")
	}

	if err != nil {
		return graph, err
	}

	l := newLexer(src, newSSTData(graph))
	if l == nil {
		return graph, fmt.Errorf("lexer is nil")
	}

	// yyDebug = 4
	yyErrorVerbose = true
	defer func() {
		if e := recover(); e != nil {
			if e, ok := e.(error); ok {
				err = e
				return
			}
			err = fmt.Errorf("p21.Parse failed: %v", e)
		}
	}()
	if y := yyParse(l); y != 0 {
		for _, err := range l.errs {
			errorReporter.Println(err)
		}
		return graph, ErrParsingFailed
	}

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	inputFile := filepath.Join(cwd, "/step/p21/testdata/first.ttl")
	f, err := os.Create(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = graph.RdfWrite(f, sst.RdfFormatTurtle)
	if err != nil {
		log.Fatal(err)
	}

	cp := newConversionParameters(graph)
	if cp == nil {
		return graph, fmt.Errorf("conversionParameters is nil")
	}

	cp.extractMetaDataFromP21Dataset(graph)
	cp.extractAttributeValues(graph)
	cp.processEntityInstance()
	cp.processOntologyOrder()
	cp.removeEntityInstance(graph)
	cp.removeAttributeValues(graph)

	return graph, nil
}

func (cp *conversionParameters) extractMetaDataFromP21Dataset(graph sst.NamedGraph) {
	graph.ForIRINodes(func(node sst.IBNode) error {
		typeOf := node.TypeOf()
		if typeOf == nil {
			return nil
		}
		inVocab := typeOf.InVocabulary()
		if inVocab == nil {
			return nil
		}
		switch inVocab.(type) {
		case ssmeta.IsEntity:
			nodeFound := cp.getSSREP(cp.getName(node))
			if nodeFound != nil {
				singleEntity := SingleEntity{}
				singleEntity.name = cp.getName(node)
				singleEntity.ontologyObject = nodeFound
				cp.extractNode(nodeFound, &singleEntity, "")
				cp.singleEntityMap[node] = singleEntity
			} else {
				cp.extraEntityMap[node] = ExtraEntity{
					name:           cp.getName(node),
					ontologyObject: node,
				}
			}
		case ssmeta.IsSingleEntityValue:
			parentNode := node.GetObjects(ssmeta.SingleEntityValueType)
			for _, parent := range parentNode {
				nodeFound := cp.getSSREP(cp.getName(parent.(sst.IBNode)))
				singleEntity := SingleEntity{}
				singleEntity.name = cp.getName(parent.(sst.IBNode))
				singleEntity.ontologyObject = nodeFound
				if nodeFound != nil {
					cp.extractNode(nodeFound, &singleEntity, "")
				}
				cp.singleEntityMap[node] = singleEntity
			}
		case ssmeta.IsEnumerationValue:
			ev := EnumerationValue{
				ExpressObject: ExpressObject{
					name:       cp.getName(node),
					objectType: EnumerationValueType,
				},
			}
			cp.enumerationValueMap[node] = ev
		case ssmeta.IsDefinedType:
			dt := DefinedType{
				ExpressObject: ExpressObject{
					name:       cp.getName(node),
					objectType: DefinedTypeType,
				},
			}
			cp.definedTypeMap[node] = dt
		}
		return nil
	})
}

func (cp *conversionParameters) extractNode(node sst.IBNode, singleEntity *SingleEntity, orderType string) {
	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if node != s {
			return nil
		}

		if o.TermKind() == sst.TermKindIBNode {
			if o.(sst.IBNode).Is(ssmeta.MainClass) {
				singleEntity.ontologyType = MainClass
			}
			if o.(sst.IBNode).Is(owl.ObjectProperty) {
				singleEntity.ontologyType = ObjectProperty
			}
		}

		if p.Is(ssmeta.StepImMapAttributeOrder) {
			cp.extractStepImMapAttributeOrder(o.(sst.IBNode), singleEntity, orderType)
		}
		if p.Is(rdfs.SubClassOf) && !o.(sst.IBNode).Is(lci.Individual) {
			orderType = SUPER
			cp.extractNode(o.(sst.IBNode), singleEntity, orderType)
		}
		return nil
	})
}

func (cp *conversionParameters) extractStepImMapAttributeOrder(node sst.IBNode, singleEntity *SingleEntity, orderType string) {
	internal := []sst.IBNode{}
	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if node != s {
			return nil
		}

		if o.TermKind() == sst.TermKindIBNode {
			if o.(sst.IBNode).Fragment() != "" && o.(sst.IBNode).Fragment() != "nil" {
				if orderType == SUPER {
					internal = append(internal, o.(sst.IBNode))
				} else {
					singleEntity.attributeOrders = append(singleEntity.attributeOrders, o.(sst.IBNode))
				}
			}
		}
		if p.Is(rdf.Rest) {
			cp.extractStepImMapAttributeOrder(o.(sst.IBNode), singleEntity, orderType)
		}
		return nil
	})

	// handle attribute order that comes from superclass
	if len(internal) > 0 && len(singleEntity.attributeOrders) > 0 {
		singleEntity.superType = true
		singleEntity.attributeOrders = append(internal, singleEntity.attributeOrders...)
	} else if len(internal) > 0 && len(singleEntity.attributeOrders) == 0 {
		singleEntity.superType = true
		singleEntity.attributeOrders = append(singleEntity.attributeOrders, internal...)
	}
}

func (cp *conversionParameters) getName(node sst.IBNode) string {
	var name string
	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if o.TermKind() == sst.TermKindLiteral {
			name = string(o.(sst.String))
		}
		return nil
	})
	return name
}

func (cp *conversionParameters) getSSREP(passedValue string) sst.IBNode {
	var result sst.IBNode
	Vocgraph, _ := sst.StaticDictionary().Vocabulary(rep.REPVocabulary)
	Vocgraph.ForIRINodes(func(node sst.IBNode) error {
		node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if p.Is(ssmeta.StepImEntityMap) {
				if o.TermKind() == sst.TermKindLiteral {
					keyValue := strings.ToUpper(string(o.(sst.String)))

					if keyValue != "" && passedValue == keyValue {
						result = s
					}
				}
			}
			return nil
		})
		return nil
	})

	return result
}

func (cp *conversionParameters) getqau(unitName string) sst.IBNode {
	if result, found := cp.enumerationCache[unitName]; found {
		return result // Return cached result
	}

	var result sst.IBNode
	Vocgraph, _ := sst.StaticDictionary().Vocabulary(qau.QAUVocabulary)
	Vocgraph.ForIRINodes(func(node sst.IBNode) error {
		node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if p.Is(rdfs.Label) {
				var keyValue string
				// Safely extract string value from literal (could be String or LangString)
				if literal, ok := o.(sst.Literal); ok {
					switch l := literal.(type) {
					case sst.String:
						keyValue = string(l)
					case sst.LangString:
						keyValue = l.Val
					default:
						// Skip non-string literals
						return nil
					}
				} else {
					// Not a literal, skip
					return nil
				}
				if keyValue != "" && strings.EqualFold(unitName, keyValue) {
					result = s
					cp.enumerationCache[unitName] = result // Cache result
				}
			}
			return nil
		})
		return nil
	})
	return result
}

// --------------------- end collect and prepare attribute order including from super types ----------------------------------------

// --------------------- start extract attribute values ----------------------------------------
func (cp *conversionParameters) extractAttributeValues(graph sst.NamedGraph) {
	graph.ForIRINodes(func(node sst.IBNode) error {
		node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if p.Is(ssmeta.AttributeValues) {
				var rawAttributeValues RawAttributeValues
				rawAttributeValues = cp.processCollection(o.(sst.IBNode), rawAttributeValues)
				cp.rawAttributeValuesMap[s] = rawAttributeValues
			}
			if p.Is(ssmeta.EntityInstanceType) {
				cp.addUnique(cp.singleInstanceValues, s, o.(sst.IBNode))
			}
			if p.Is(ssmeta.ComplexInstanceValue) {
				cp.addUnique(cp.complexInstanceValues, s, o.(sst.IBNode))
			}
			return nil
		})

		// get family name for future reference
		node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if o.TermKind() == sst.TermKindIBNode {
				if cp.singleEntityMap[s].name != "" {
					assignValue := cp.rawAttributeValuesMap[s]
					assignValue.name = cp.singleEntityMap[s].name
					cp.rawAttributeValuesMap[s] = assignValue
				} else if cp.singleEntityMap[o.(sst.IBNode)].name != "" {
					assignValue := cp.rawAttributeValuesMap[s]
					assignValue.name = cp.singleEntityMap[o.(sst.IBNode)].name
					cp.rawAttributeValuesMap[s] = assignValue
				} else if cp.extraEntityMap[s].name != "" {
					assignValue := cp.rawAttributeValuesMap[s]
					assignValue.name = cp.extraEntityMap[s].name
					cp.rawAttributeValuesMap[s] = assignValue
				} else if cp.extraEntityMap[o.(sst.IBNode)].name != "" {
					assignValue := cp.rawAttributeValuesMap[s]
					assignValue.name = cp.extraEntityMap[o.(sst.IBNode)].name
					cp.rawAttributeValuesMap[s] = assignValue
				}
			}
			return nil
		})
		return nil
	})
}

func (cp *conversionParameters) addUnique(values map[sst.IBNode][]sst.IBNode, key sst.IBNode, newValue sst.IBNode) {
	slice := values[key]
	for _, v := range slice {
		if v == newValue {
			return
		}
	}
	// newValue was not found; append it to the slice
	values[key] = append(slice, newValue)
}

func (cp *conversionParameters) processCollection(node sst.IBNode, rawAttributeValues RawAttributeValues) RawAttributeValues {
	if literalCollection, ok := node.AsCollection(); ok {
		literalCollection.ForMembers(func(_ int, o sst.Term) {
			switch o.TermKind() {
			case sst.TermKindLiteral:
				rawAttributeValues.MixedValues = append(rawAttributeValues.MixedValues, o.(sst.Literal))
			case sst.TermKindIBNode, sst.TermKindTermCollection:
				rawAttributeValues.MixedValues = append(rawAttributeValues.MixedValues, o.(sst.IBNode))
			}
		})
	}
	return rawAttributeValues
}

// --------------------- end extract attribute values ----------------------------------------

// ------------------------- start handle measure representation item and global unit assigned context -------------------------

func (cp *conversionParameters) handleGlobalUnitAssignedContext(parentNode sst.IBNode, node sst.IBNode) {
	rawAttrValues, exists := cp.rawAttributeValuesMap[node]
	if !exists || rawAttrValues.MixedValues == nil {
		return
	}
	for _, v := range rawAttrValues.MixedValues {
		if ibnodeVal, ok := v.(sst.IBNode); ok {
			if ibnodeCollection, ok := ibnodeVal.AsCollection(); ok {
				ibnodeCollection.ForMembers(func(_ int, o sst.Term) {
					if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
						groupedStrings := cp.processIBNodeForGlobalUnit(o.(sst.IBNode))
						for _, group := range groupedStrings {
							if len(group) == 2 {
								mergedValue := strings.ToLower(group[0] + " " + group[1])
								getReferenceNode := cp.getqau(mergedValue)
								if getReferenceNode != nil {
									inVocab := getReferenceNode.InVocabulary()
									if inVocab != nil {
										parentNode.AddStatement(rep.GlobalUnit, inVocab.VocabularyElement())
									}
								}
							} else if len(group) == 1 {
								getReferenceNode := cp.getqau(group[0])
								if getReferenceNode != nil {
									inVocab := getReferenceNode.InVocabulary()
									if inVocab != nil {
										parentNode.AddStatement(rep.GlobalUnit, inVocab.VocabularyElement())
									}
								}
							}
						}
					}
				})
			}
		}
	}
}

func (cp *conversionParameters) processIBNodeForGlobalUnit(node sst.IBNode) [][]string {
	var groupedStrings [][]string

	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
			var currentGroup []string
			if len(cp.rawAttributeValuesMap[o.(sst.IBNode)].MixedValues) > 0 {
				for _, v := range cp.rawAttributeValuesMap[o.(sst.IBNode)].MixedValues {
					if ibnodeVal, ok := v.(sst.IBNode); ok {
						ibnodeVal.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
							if o.TermKind() == sst.TermKindLiteral {
								currentGroup = append(currentGroup, string(o.(sst.String)))
							}
							return nil
						})
					}
				}
			}
			if len(currentGroup) > 0 {
				groupedStrings = append(groupedStrings, currentGroup)
			}
		}
		return nil
	})

	return groupedStrings
}

func (cp *conversionParameters) handleMeasureRepresentationItem(node sst.IBNode) {
	var measureType string
	var measureUnit string
	var measureValue float64

	measureTypeMap := map[string]sst.Element{
		"mass_measure":                      qau.Mass,
		"area_measure":                      qau.Area,
		"time_measure":                      qau.Time,
		"force_measure":                     qau.Force,
		"plane_angle_measure":               qau.Angle,
		"positive_plane_angle_measure":      qau.Angle, // Same as plane_angle_measure
		"power_measure":                     qau.Power,
		"volume_measure":                    qau.Volume,
		"energy_measure":                    qau.Energy,
		"length_measure":                    qau.Length,
		"non_negative_length_measure":       qau.Length, // Same as length_measure
		"positive_length_measure":           qau.Length, // Same as length_measure
		"radioactivity_measure":             qau.Activity,
		"velocity_measure":                  qau.Velocity,
		"frequency_measure":                 qau.Frequency,
		"solid_angle_measure":               qau.SolidAngle,
		"resistance_measure":                qau.Resistance,
		"inductance_measure":                qau.Inductance,
		"illuminance_measure":               qau.Illuminance,
		"luminous_flux_measure":             qau.LuminousFlux,
		"conductance_measure":               qau.Conductance,
		"magnetic_flux_measure":             qau.MagneticFlux,
		"capacitance_measure":               qau.Capacitance,
		"acceleration_measure":              qau.Acceleration,
		"absorbed_dose_measure":             qau.AbsorbedDose,
		"count_measure":                     qau.CountQuantity,
		"pressure_measure":                  qau.StaticPressure,
		"electric_charge_measure":           qau.ElectricCharge,
		"dose_equivalent_measure":           qau.DoseEquivalent,
		"electric_current_measure":          qau.ElectricCurrent,
		"luminous_intensity_measure":        qau.LuminousIntensity,
		"electric_potential_measure":        qau.ElectricPotential,
		"amount_of_substance_measure":       qau.AmountOfSubstance,
		"celsius_temperature_measure":       qau.ThermodynamicTemperature,
		"magnetic_flux_density_measure":     qau.MagneticFluxDensity,
		"thermodynamic_temperature_measure": qau.ThermodynamicTemperature,
		"ratio_measure":                     lci.NumericQuantityValue.Element, // Special case
		"parameter_value":                   lci.NumericQuantityValue.Element, // Special case
		"numeric_measure":                   lci.NumericQuantityValue.Element, // Special case
		"positive_ratio_measure":            lci.NumericQuantityValue.Element, // Special case
	}

	getMixedValues := cp.rawAttributeValuesMap[node].MixedValues

	// getPartDesign := cp.findMeasureRepresentationPartDesign(node)

	for _, value := range getMixedValues {
		if strVal, ok := value.(string); ok {
			if cp.isValid(strVal) {
				node.AddStatement(rdfs.Label, sst.String(strVal))
			}
		}

		if ibnodeVal, ok := value.(sst.IBNode); ok {
			if literalCollection, ok := ibnodeVal.AsCollection(); ok {
				literalCollection.ForMembers(func(_ int, o sst.Term) {
					switch o.TermKind() {
					case sst.TermKindLiteral:
						measureValue = float64(o.(sst.Double))
					case sst.TermKindIBNode, sst.TermKindTermCollection:
						measureType = strings.ToLower(cp.definedTypeMap[o.(sst.IBNode)].name)
					}
				})
			} else {
				for _, v := range cp.rawAttributeValuesMap[ibnodeVal].MixedValues {
					if ibnodeVal, ok := v.(sst.IBNode); ok {
						if ibnodeCollection, ok := ibnodeVal.AsCollection(); ok {
							ibnodeCollection.ForMembers(func(_ int, o sst.Term) {
								if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
									var groupedStrings [][]string
									var numberType string

									// extract data from ibnode collection to process
									for _, v := range cp.rawAttributeValuesMap[o.(sst.IBNode)].MixedValues {
										if ibnodeVal, ok := v.(sst.IBNode); ok {
											groupedStrings = cp.processIBNodeForGlobalUnit(ibnodeVal)
										}
										if intVal, ok := v.(float64); ok {
											if intVal == 2 {
												numberType = SQUARE
											} else if intVal == 3 {
												numberType = CUBIC
											}
										}
									}
									// handle measurement unit type with square and cubic
									var merged string
									for _, group := range groupedStrings {
										if len(group) == 2 {
											merged = strings.ToLower(group[0] + " " + group[1])
										}
									}
									measureUnit = strings.ToLower(numberType + " " + merged)
								}
							})
						}
					}
				}
			}
		}
	}

	// use values measureType and measureValue
	if element, exists := measureTypeMap[measureType]; exists {
		measureItem := cp.graph.CreateIRINode("", element)
		getReferenceNode := cp.getqau(measureUnit)
		if getReferenceNode != nil {
			measureItem.AddStatement(getReferenceNode.InVocabulary().VocabularyElement(), sst.Double(measureValue))
			node.AddStatement(rep.MeasureValue, measureItem)
		}
	}
}

func (cp *conversionParameters) findMeasureRepresentationPartDesign(node sst.IBNode) sst.IBNode {
	repFound := cp.searchNode(node)
	if repFound[0] != nil {
		itemFound := cp.searchNode(repFound[0])
		if itemFound[0] != nil {
			for _, value := range cp.rawAttributeValuesMap[itemFound[0]].MixedValues {
				if ibnodeVal, ok := value.(sst.IBNode); ok && ibnodeVal != repFound[0] {
					for _, value := range cp.rawAttributeValuesMap[ibnodeVal].MixedValues {
						if ibnodeVal, ok := value.(sst.IBNode); ok {
							for _, value := range cp.rawAttributeValuesMap[ibnodeVal].MixedValues {
								if ibnodeVal, ok := value.(sst.IBNode); ok {
									if cp.rawAttributeValuesMap[ibnodeVal].name == PRODUCT_DEFINITION_SHAPE {
										for _, value := range cp.rawAttributeValuesMap[ibnodeVal].MixedValues {
											if ibnodeVal, ok := value.(sst.IBNode); ok {
												// fmt.Println("==========bbb==", ibnodeVal.Fragment())
												return ibnodeVal
											}
										}
									} else if cp.rawAttributeValuesMap[ibnodeVal].name == PRODUCT_DEFINITION {
										// fmt.Println("==========ccc==", ibnodeVal.Fragment())
										return ibnodeVal
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return nil
}

// ------------------------- end handle measure representation item and global unit assigned context -------------------------

// ------------------------- start handling single leave and multi leave conversion --------------------------------

func (cp *conversionParameters) processEntityInstance() {
	// single leave conversion
	for node, singleValue := range cp.singleInstanceValues {
		for _, instanceType := range singleValue {
			if cp.singleEntityMap[instanceType].ontologyObject != nil {
				switch cp.singleEntityMap[instanceType].ontologyType {
				case MainClass:
					node.AddStatement(rdf.Type, cp.singleEntityMap[instanceType].ontologyObject.InVocabulary().VocabularyElement())
					if cp.singleEntityMap[instanceType].name == MEASURE_REPRESENTATION_ITEM {
						cp.handleMeasureRepresentationItem(node)
					} else if cp.singleEntityMap[instanceType].name == EDGE_LOOP {
						attributeOrders := cp.singleEntityMap[instanceType].attributeOrders[:len(cp.singleEntityMap[instanceType].attributeOrders)-1]
						cp.processLeave(node, attributeOrders, cp.rawAttributeValuesMap[node].MixedValues)
					} else {
						cp.processLeave(node, cp.singleEntityMap[instanceType].attributeOrders, cp.rawAttributeValuesMap[node].MixedValues)
					}
				case ObjectProperty:
					if cp.singleEntityMap[instanceType].name == PROPERTY_DEFINITION_REPRESENTATION {
						node.AddStatement(rdf.Type, owl.ObjectProperty)
						node.AddStatement(rdfs.SubPropertyOf, rep.IndividualRepresentation)
						for i, value := range cp.rawAttributeValuesMap[node].MixedValues {
							if ibnodeVal, ok := value.(sst.IBNode); ok {
								if i == 1 {
									node.AddStatement(rep.Representation, ibnodeVal)
								}
							}
						}
					} else {
						// fmt.Println("---ff--", node.Fragment(), cp.singleEntityMap[instanceType].name, cp.singleEntityMap[instanceType].ontologyObject.Fragment(), , cp.rawAttributeValuesMap[node].MixedValues)
					}
				}
			} else {
				if cp.extraEntityMap[instanceType].ontologyObject != nil {
					switch cp.extraEntityMap[instanceType].name {
					case PRODUCT:
						cp.convertPDMCollection(node)
					case NEXT_ASSEMBLY_USAGE_OCCURRENCE:
						cp.convertNextOccurrence(node)
					default:
						// fmt.Println("---ff--", node.Fragment(), cp.extraEntityMap[instanceType].name, cp.extraEntityMap[instanceType].ontologyObject.Fragment())
					}
				}
			}
		}
	}

	// multi leave conversion
	for node, complexValue := range cp.complexInstanceValues {
		for _, instanceType := range complexValue {
			switch cp.rawAttributeValuesMap[instanceType].name {
			case GLOBAL_UNIT_ASSIGNED_CONTEXT:
				cp.handleGlobalUnitAssignedContext(node, instanceType)
			case REPRESENTATION_RELATIONSHIP,
				SHAPE_REPRESENTATION_RELATIONSHIP,
				REPRESENTATION_RELATIONSHIP_WITH_TRANSFORMATION:
			default:
				if cp.singleEntityMap[instanceType].ontologyObject != nil {
					if !cp.containsIBNode(cp.collectComplexNodes[node], cp.singleEntityMap[instanceType].ontologyObject) {
						cp.collectComplexNodes[node] = append(cp.collectComplexNodes[node], cp.singleEntityMap[instanceType].ontologyObject)
					}
					cp.processLeave(node, cp.singleEntityMap[instanceType].attributeOrders, cp.rawAttributeValuesMap[instanceType].MixedValues)
				}
			}
		}
	}
}

func (cp *conversionParameters) processLeave(node sst.IBNode, ontologyAttributes []sst.IBNode, mixedValues []interface{}) {
	if len(mixedValues) == 0 || len(ontologyAttributes) == 0 {
		return
	}

	if len(ontologyAttributes) > len(mixedValues) {
		offset := len(ontologyAttributes) - len(mixedValues)
		ontologyAttributes = ontologyAttributes[offset:]
	}

	for i, mixedVal := range mixedValues {
		switch v := mixedVal.(type) {
		case string:
			if cp.isValid(v) {
				node.AddStatement(ontologyAttributes[i].InVocabulary().VocabularyElement(), sst.String(v))
			}
		case sst.IBNode:
			if _, ok := v.AsCollection(); ok {
				cp.processCollectionIbNode(node, v, ontologyAttributes[i])
			} else {
				cp.processSingleIbNode(node, v, ontologyAttributes[i])
			}
		case float64:
			node.AddStatement(ontologyAttributes[i].InVocabulary().VocabularyElement(), sst.Double(v))
		case int64:
			node.AddStatement(ontologyAttributes[i].InVocabulary().VocabularyElement(), sst.Double(v))
		}
	}
}

func (cp *conversionParameters) processSingleIbNode(node sst.IBNode, ibnodeVal sst.IBNode, attribute sst.IBNode) {
	allElements := map[string]map[string]sst.Element{
		"surfaceForm": {
			"plane_surf":               rep.BSplineSurfaceForm_PlaneSurf.Element,
			"cylindrical_surf":         rep.BSplineSurfaceForm_CylindricalSurf.Element,
			"conical_surf":             rep.BSplineSurfaceForm_ConicalSurf.Element,
			"spherical_surf":           rep.BSplineSurfaceForm_SphericalSurf.Element,
			"toroidal_surf":            rep.BSplineSurfaceForm_ToroidalSurf.Element,
			"surf_of_revolution":       rep.BSplineSurfaceForm_SurfOfRevolution.Element,
			"ruled_surf":               rep.BSplineSurfaceForm_RuledSurf.Element,
			"generalised_cone":         rep.BSplineSurfaceForm_GeneralisedCone.Element,
			"quadric_surf":             rep.BSplineSurfaceForm_QuadricSurf.Element,
			"surf_of_linear_extrusion": rep.BSplineSurfaceForm_SurfOfLinearExtrusion.Element,
			"unspecified":              rep.BSplineSurfaceForm_Unspecified.Element,
		},
		"curveForm": {
			"polyline_form":  rep.BSplineCurveForm_PolylineForm.Element,
			"circular_arc":   rep.BSplineCurveForm_CircularArc.Element,
			"elliptic_arc":   rep.BSplineCurveForm_EllipticArc.Element,
			"parabolic_arc":  rep.BSplineCurveForm_ParabolicArc.Element,
			"hyperbolic_arc": rep.BSplineCurveForm_HyperbolicArc.Element,
			"unspecified":    rep.BSplineCurveForm_Unspecified.Element,
		},
		"knotSpec": {
			"uniform_knots":          rep.KnotType_UniformKnots.Element,
			"quasi_uniform_knots":    rep.KnotType_QuasiUniformKnots.Element,
			"piecewise_bezier_knots": rep.KnotType_PiecewiseBezierKnots.Element,
			"unspecified":            rep.KnotType_Unspecified.Element,
		},
		"TrimmingPreference": {
			"cartesian":   rep.TrimmingPreference_Cartesian.Element,
			"parameter":   rep.TrimmingPreference_Parameter.Element,
			"unspecified": rep.TrimmingPreference_Unspecified.Element,
		},
	}

	if value, ok := cp.enumerationValueMap[ibnodeVal]; ok {
		if categoryMap, ok := allElements[string(attribute.Fragment())]; ok {
			if element, exists := categoryMap[strings.ToLower(value.name)]; exists {
				node.AddStatement(attribute.InVocabulary().VocabularyElement(), element)
			}
		} else {
			node.AddStatement(attribute.InVocabulary().VocabularyElement(), sst.Boolean(cp.getBooleanValue(value.name)))
		}
	} else {
		if !ibnodeVal.Is(ssmeta.IndeterminateValue) {
			node.AddStatement(attribute.InVocabulary().VocabularyElement(), ibnodeVal)
		}
	}
}

func (cp *conversionParameters) processCollectionIbNode(node sst.IBNode, collectionNode sst.IBNode, attribute sst.IBNode) {
	integerPoints := cp.getIntegerCollection(collectionNode)
	floatPoints := cp.getFloatCollection(collectionNode)
	ibnodeCollection := cp.getIbnodeCollection(collectionNode)

	if len(floatPoints) > 0 {
		col := sst.NewLiteralCollection(floatPoints[0], floatPoints[1:]...)
		node.AddStatement(attribute.InVocabulary().VocabularyElement(), col)
	} else if len(integerPoints) > 0 {
		col := sst.NewLiteralCollection(integerPoints[0], integerPoints[1:]...)
		node.AddStatement(attribute.InVocabulary().VocabularyElement(), col)
	} else if ibnodeCollection.Children != nil && len(ibnodeCollection.Children) > 0 {
		col, err := cp.createMultiDimensionalCollectionFromTree(ibnodeCollection)
		if err != nil {
			fmt.Println("Error creating collection:", err)
			return
		}
		if attribute.Is(ssmeta.StepImMapAttributeSpecial) {
			// target edge_loop to use EdgeList instead of StepImMapAttributeSpecial
			node.AddStatement(rep.EdgeList, col)
		} else {
			node.AddStatement(attribute.InVocabulary().VocabularyElement(), col)
		}
	}
}

func (cp *conversionParameters) getFloatCollection(value sst.IBNode) []sst.Literal {
	var points []sst.Double
	if floatCollection, ok := value.AsCollection(); ok {
		floatCollection.ForMembers(func(_ int, o sst.Term) {
			if o.TermKind() == sst.TermKindLiteral {
				if point, ok := o.(sst.Double); ok {
					points = append(points, sst.Double(point))
				}
			}
		})
	}

	members := make([]sst.Literal, len(points))
	for i := 0; i < len(points); i++ {
		members[i] = sst.Double(points[i])
	}
	return members
}

func (cp *conversionParameters) getBooleanValue(boolValue string) bool {
	var orientation bool
	if boolValue == "T" {
		orientation = true
	} else if boolValue == "F" {
		orientation = false
	}
	return orientation
}

func (cp *conversionParameters) getIntegerCollection(value sst.IBNode) []sst.Literal {
	var points []sst.Integer
	if floatCollection, ok := value.AsCollection(); ok {
		floatCollection.ForMembers(func(_ int, o sst.Term) {
			if o.TermKind() == sst.TermKindLiteral {
				if point, ok := o.(sst.Integer); ok {
					points = append(points, sst.Integer(point))
				}
			}
		})
	}

	members := make([]sst.Literal, len(points))
	for i := 0; i < len(points); i++ {
		members[i] = sst.Integer(points[i])
	}
	return members
}

func (cp *conversionParameters) getIbnodeCollection(value sst.IBNode) *TreeNode {
	root := &TreeNode{Value: value}
	cp.helperIbnodeCollection(value, root)
	return root
}

func (cp *conversionParameters) helperIbnodeCollection(value sst.IBNode, currentNode *TreeNode) {
	if ibnodeCollection, ok := value.AsCollection(); ok {
		ibnodeCollection.ForMembers(func(_ int, o sst.Term) {
			child := &TreeNode{Value: o}
			currentNode.Children = append(currentNode.Children, child)
			if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
				if _, isCollection := o.(sst.IBNode).AsCollection(); isCollection {
					cp.helperIbnodeCollection(o.(sst.IBNode), child)
				}
			}
		})
	}
}

func (cp *conversionParameters) createMultiDimensionalCollectionFromTree(root *TreeNode) (sst.Term, error) {
	if root.Children == nil || len(root.Children) == 0 {
		return root.Value, nil
	}

	var innerCols []sst.Term
	for _, child := range root.Children {
		col, err := cp.createMultiDimensionalCollectionFromTree(child)
		if err != nil {
			return nil, err
		}
		innerCols = append(innerCols, col)
	}

	outerCol := cp.graph.CreateCollection(innerCols...)

	return outerCol, nil
}

// ------------------------- end handling single leave and multi leave conversion --------------------------------

// -------------------------  PDM - start handle part, part version and part design -------------------------
func (cp *conversionParameters) convertPDMCollection(node sst.IBNode) {
	productReferences := cp.searchNode(node)
	for _, product := range productReferences {
		if product != nil {
			switch cp.rawAttributeValuesMap[product].name {
			case PRODUCT_RELATED_PRODUCT_CATEGORY:
				// handle part - PRODUCT - PRODUCT_RELATED_PRODUCT_CATEGORY
				node.AddStatement(rdf.Type, sso.Part)
				cp.processCommonData(node, "")
			case PRODUCT_DEFINITION_FORMATION:
				// handle part version - PRODUCT_DEFINITION_FORMATION - PRODUCT_DEFINITION_FORMATION_WITH_SPECIFIED_SOURCE
				product.AddStatement(rdf.Type, sso.PartVersion)
				node.AddStatement(sso.HasPartVersion, product)
				cp.processCommonData(product, VERSION)

				// handle part design - PRODUCT_DEFINITION - PRODUCT_DEFINITION_CONTEXT
				partDesign := cp.searchNode(product)
				for _, design := range partDesign {
					if design != nil {
						nextOccurrenceReferenceExist := false
						product.AddStatement(sso.HasProductDefinition, design)
						cp.processCommonData(design, DESIGN)

						// shape representation - PRODUCT_DEFINITION_SHAPE - SHAPE_DEFINITION_REPRESENTATION
						productDefinitionShape := cp.searchNode(design)
						for _, productShape := range productDefinitionShape {
							if productShape != nil {
								shapeDefinitionRepresentation := cp.searchNode(productShape)
								if cp.rawAttributeValuesMap[productShape].name == PRODUCT_DEFINITION_SHAPE {
									for _, shapeRepresentation := range shapeDefinitionRepresentation {
										if cp.rawAttributeValuesMap[shapeRepresentation].name == SHAPE_DEFINITION_REPRESENTATION {
											for i, shapeRep := range cp.rawAttributeValuesMap[shapeRepresentation].MixedValues {
												if shapeRep, ok := shapeRep.(sst.IBNode); ok && i == 1 {
													design.AddStatement(sso.DefiningGeometry, shapeRep)
												}
											}
										}
										if cp.rawAttributeValuesMap[shapeRepresentation].name == SHAPE_ASPECT {
										}
									}
								}
								if cp.rawAttributeValuesMap[productShape].name == NEXT_ASSEMBLY_USAGE_OCCURRENCE {
									nextOccurrenceReferenceExist = true
								}
							}
						}

						// if product_definition_shape reference exist inside next_assembly_usage_occurrence then use sso.AssemblyDesign
						if nextOccurrenceReferenceExist {
							design.AddStatement(rdf.Type, sso.AssemblyDesign)
						} else {
							design.AddStatement(rdf.Type, sso.PartDesign)
						}
					}
				}
			}
		}
	}
}

func (cp *conversionParameters) processCommonTextData(node sst.IBNode, representationRelationship interface{}, position int) {
	if representationText, ok := representationRelationship.(string); ok && cp.isValid(representationText) {
		if position == 0 {
			node.AddStatement(rdfs.Label, sst.String(representationText))
		} else if position == 1 {
			node.AddStatement(rdfs.Comment, sst.String(representationText))
		}
	}
}

func (cp *conversionParameters) processCommonData(node sst.IBNode, partType string) {
	for i, partData := range cp.rawAttributeValuesMap[node].MixedValues {
		if part, ok := partData.(string); ok && cp.isValid(part) {
			if i == 0 {
				if partType == VERSION {
					node.AddStatement(sso.ViewID, sst.String(part))
				} else if partType == DESIGN {
					node.AddStatement(sso.VersionID, sst.String(part))
				} else {
					node.AddStatement(sso.ID, sst.String(part))
				}
			}
			if i == 1 {
				node.AddStatement(rdfs.Label, sst.String(part))
			}
			if i == 2 {
				node.AddStatement(rdfs.Comment, sst.String(part))
			}
		}
	}
}

func (cp *conversionParameters) searchNode(searchNode sst.IBNode) []sst.IBNode {
	collectFoundNodes := []sst.IBNode{}
	for key, values := range cp.rawAttributeValuesMap {
		for _, value := range values.MixedValues {
			if found, ok := value.(sst.IBNode); ok {
				if found == searchNode {
					collectFoundNodes = append(collectFoundNodes, key)
				} else if ibnodeCollection, ok := found.AsCollection(); ok {
					ibnodeCollection.ForMembers(func(_ int, o sst.Term) {
						if o.TermKind() == sst.TermKindIBNode && o.(sst.IBNode) == searchNode {
							collectFoundNodes = append(collectFoundNodes, key)
						}
					})
				}
			}
		}
	}
	return collectFoundNodes
}

func (cp *conversionParameters) convertNextOccurrence(node sst.IBNode) error {
	node.AddStatement(rdf.Type, owl.ObjectProperty)
	node.AddStatement(rdfs.SubPropertyOf, sso.NextAssemblyOccurrenceUsage)

	// create single instance singleOccurrence
	singleInstance := cp.graph.CreateIRINode("", sso.SingleOccurrence)

	for i, nextOccurrence := range cp.rawAttributeValuesMap[node].MixedValues {
		if nextText, ok := nextOccurrence.(string); ok && cp.isValid(nextText) {
			if i == 0 || i == 5 {
				if i == 5 { // if reference_designator exist on this position
					singleInstance.AddStatement(sso.ID, sst.String(nextText))
				} else {
					singleInstance.AddStatement(sso.ID, sst.String(nextText))
				}
			} else if i == 1 {
				singleInstance.AddStatement(rdfs.Label, sst.String(nextText))
			} else if i == 2 {
				singleInstance.AddStatement(rdfs.Comment, sst.String(nextText))
			}
		}
		if nextIbnode, ok := nextOccurrence.(sst.IBNode); ok {
			if i == 3 {
				// create punning for next_assembly_usage_occurrence and single_occurrence
				nextIbnode.AddStatement(node, singleInstance)
			}
			if i == 4 {
				singleInstance.AddStatement(lci.IsDefinedBy, nextIbnode)
			}
		}
	}

	// process complex leave instance
	nextOccurrenceUsage := cp.searchNode(node)
	for _, nextOccurrence := range nextOccurrenceUsage {
		if nextOccurrence != nil {
			contextDependentShape := cp.searchNode(nextOccurrence)
			for _, contextDependentShape := range contextDependentShape {
				cp.handleComplexLeave(node, contextDependentShape)
			}
		}
	}

	return nil
}

func (cp *conversionParameters) handleComplexLeave(node sst.IBNode, contextDependentShape sst.IBNode) {
	for _, complexInstance := range cp.rawAttributeValuesMap[contextDependentShape].MixedValues {
		if complexNode, ok := complexInstance.(sst.IBNode); ok && len(cp.complexInstanceValues[complexNode]) > 0 {
			node.AddStatement(rep.ContextDependentShapeRepresentation, complexNode)
			complexNode.AddStatement(rdf.Type, owl.ObjectProperty)

			for _, ibnodeVal := range cp.complexInstanceValues[complexNode] {
				if cp.rawAttributeValuesMap[ibnodeVal].name == REPRESENTATION_RELATIONSHIP_WITH_TRANSFORMATION {
					complexNode.AddStatement(rdfs.SubPropertyOf, rep.ShapeRepresentationRelationshipWithPlacementTransformation)
					for _, transformationOperator := range cp.rawAttributeValuesMap[ibnodeVal].MixedValues {
						if transformation, ok := transformationOperator.(sst.IBNode); ok {
							// handle item_defined_transformation
							complexNode.AddStatement(rep.TransformationOperator, transformation)
							transformation.AddStatement(rdf.Type, owl.ObjectProperty)
							transformation.AddStatement(rdfs.SubPropertyOf, rep.ItemDefinedTransformation)

							for i, itemDefinedTransformation := range cp.rawAttributeValuesMap[transformation].MixedValues {
								// handle punning for item_defined_transformation
								cp.processCommonTextData(transformation, itemDefinedTransformation, i)
								if itemDefinedNode, ok := itemDefinedTransformation.(sst.IBNode); ok && cp.rawAttributeValuesMap[transformation].MixedValues[2].(sst.IBNode) != nil && i == 3 {
									itemDefinedNode.AddStatement(transformation, cp.rawAttributeValuesMap[transformation].MixedValues[2].(sst.IBNode))
								}
							}
						}
					}
				} else if cp.rawAttributeValuesMap[ibnodeVal].name == REPRESENTATION_RELATIONSHIP {
					for i, representationRelationship := range cp.rawAttributeValuesMap[ibnodeVal].MixedValues {
						// handle punning for representation_relationship_with_placement_transformation
						cp.processCommonTextData(complexNode, representationRelationship, i)
						if representationNode, ok := representationRelationship.(sst.IBNode); ok && cp.rawAttributeValuesMap[ibnodeVal].MixedValues[2].(sst.IBNode) != nil && i == 3 {
							representationNode.AddStatement(complexNode, cp.rawAttributeValuesMap[ibnodeVal].MixedValues[2].(sst.IBNode))
						}
					}
				}
			}
		}
	}
}

func (cp *conversionParameters) containsIBNode(slice []sst.IBNode, item sst.IBNode) bool {
	for _, sliceItem := range slice {
		if sliceItem == item {
			return true
		}
	}
	return false
}

func (cp *conversionParameters) isValid(s string) bool {
	testValues := []string{"", " ", "NONE", "none", "/NULL", "-"}
	for _, val := range testValues {
		if strings.TrimSpace(s) == val {
			return false
		}
	}
	return true
}

// ------------------------ PDM - end handle part, part version and part design -------------------------

// ------------------------- start handle complex instance type -------------------------
func (cp *conversionParameters) processOntologyOrder() {
	parentMap := make(map[sst.IBNode]sst.IBNode)
	hierarchyLevelMap := make(map[sst.IBNode]int)

	// Collecting parent and child relationships
	for _, arrayOfNode := range cp.collectComplexNodes {
		for _, node := range arrayOfNode {
			node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
					if p.Is(rdfs.SubClassOf) && o.(sst.IBNode) != node {
						if o.(sst.IBNode) != node {
							parentMap[node] = o.(sst.IBNode)
						}
					}
				}
				return nil
			})
		}
	}

	// Calculate hierarchy levels once
	for node := range parentMap {
		hierarchyLevelMap[node] = cp.getHierarchyLevel(node, parentMap)
	}

	// Sort each array in the map based on the hierarchy level
	cp.sortAndReverseNodes(hierarchyLevelMap)

	// add complexInstanceValue type to the node
	for selectedNode, arrayOfNode := range cp.collectComplexNodes {
		mainClassAdded := false
		for _, node := range arrayOfNode {
			node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
					if o.(sst.IBNode).Is(ssmeta.MainClass) && !mainClassAdded {
						mainClassAdded = true
						selectedNode.AddStatement(rdf.Type, node.InVocabulary().VocabularyElement())
					} else if o.(sst.IBNode).Is(ssmeta.OptionClass) {
						selectedNode.AddStatement(rdf.Type, node.InVocabulary().VocabularyElement())
					}
				}
				return nil
			})
		}
	}
}

func (cp *conversionParameters) sortAndReverseNodes(hierarchyLevelMap map[sst.IBNode]int) {
	for key, arrayOfNode := range cp.collectComplexNodes {
		sort.Slice(arrayOfNode, func(i, j int) bool {
			return hierarchyLevelMap[arrayOfNode[i]] < hierarchyLevelMap[arrayOfNode[j]]
		})
		for i, j := 0, len(arrayOfNode)-1; i < j; i, j = i+1, j-1 {
			arrayOfNode[i], arrayOfNode[j] = arrayOfNode[j], arrayOfNode[i]
		}
		cp.collectComplexNodes[key] = arrayOfNode
	}
}

func (cp *conversionParameters) getHierarchyLevel(node sst.IBNode, parentMap map[sst.IBNode]sst.IBNode) int {
	level := 0
	for ; parentMap[node] != nil; node = parentMap[node] {
		level++
	}
	return level
}

// ------------------------- end handle complex instance type -------------------------

// ---------------------------- remove entity instance and attribute values ----------------------------

func (cp *conversionParameters) removeEntityInstance(graph sst.NamedGraph) {
	// Collect nodes to delete separately to avoid modifying graph during ForDelete iteration
	nodesToDelete := make(map[sst.IBNode]bool)

	graph.ForIRINodes(func(node sst.IBNode) error {
		node.ForDelete(func(index int, s, p sst.IBNode, o sst.Term) bool {
			if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
				if p.Is(rdf.Type) && o.(sst.IBNode).Is(ssmeta.EntityInstance) {
					return true
				}
				if p.Is(ssmeta.EntityInstanceType) {
					return true
				}
				if p.Is(rdf.Type) && o.(sst.IBNode).Is(ssmeta.SingleEntityValue) {
					return true
				}
				if p.Is(ssmeta.SingleEntityValueType) {
					return true
				}
				if p.Is(ssmeta.ComplexInstanceValue) {
					return true
				}
				if p.Is(rdf.Type) && o.(sst.IBNode).Is(ssmeta.Entity) {
					nodesToDelete[s] = true
					return true
				}
				if p.Is(rdf.Type) && o.(sst.IBNode).Is(ssmeta.EnumerationValue) {
					nodesToDelete[s] = true
					return true
				}
			}
			return false
		})
		return nil
	})

	for nodeToDelete := range nodesToDelete {
		nodeToDelete.Delete()
	}
}

func (cp *conversionParameters) removeAttributeValues(graph sst.NamedGraph) {
	graph.ForIRINodes(func(node sst.IBNode) error {
		node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if p.Is(ssmeta.AttributeValues) {
				cp.deleteCollectionRecursively(o.(sst.IBNode))
			}
			return nil
		})
		return nil
	})
}

func (cp *conversionParameters) deleteCollectionRecursively(node sst.IBNode) {
	if collection, ok := node.AsCollection(); ok {
		collection.ForMembers(func(_ int, o sst.Term) {
			if o.TermKind() == sst.TermKindIBNode || o.TermKind() == sst.TermKindTermCollection {
				cp.deleteCollectionRecursively(o.(sst.IBNode))
			}
		})
		node.Delete()
	}
}

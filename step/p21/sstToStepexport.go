// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package p21

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/semanticstep/sst-core/sst"
	"github.com/semanticstep/sst-core/vocabularies/lci"
	"github.com/semanticstep/sst-core/vocabularies/rdf"
	"github.com/semanticstep/sst-core/vocabularies/rdfs"
	"github.com/semanticstep/sst-core/vocabularies/rep"
	"github.com/semanticstep/sst-core/vocabularies/sso"
)

const (
	productContextFormat             = "#%d=%s('%s',%s,'%s');\n"
	productDefinitionFormat          = "#%d=%s('%s','%s',%s,%s);\n"
	productDefinitionShapeFormat     = "#%d=%s('%s','%s',%s);\n"
	productDefinitionFormationFormat = "#%d=%s('%s','%s',%s);\n"
)

var shapeContent = map[string]string{
	"Direction":                                        "DIRECTION",
	"UniformCurve":                                     "UNIFORM_CURVE",
	"GeometricCurveSet":                                "GEOMETRIC_CURVE_SET",
	"CartesianPoint":                                   "CARTESIAN_POINT",
	"Axis2Placement2D":                                 "AXIS2_PLACEMENT_2D",
	"Axis2Placement3D":                                 "AXIS2_PLACEMENT_3D",
	"ShapeRepresentation":                              "SHAPE_REPRESENTATION",
	"GeometricModel":                                   "GEOMETRIC_MODEL",
	"GeometricRepresentationContext":                   "GEOMETRIC_REPRESENTATION_CONTEXT",
	"TessellatedShapeRepresentation":                   "TESSELLATED_SHAPE_REPRESENTATION",
	"ScanDataShapeRepresentation":                      "SCAN_DATA_SHAPE_REPRESENTATION",
	"AdvancedBrepShapeRepresentation":                  "ADVANCED_BREP_SHAPE_REPRESENTATION",
	"CsgShapeRepresentation":                           "CSG_SHAPE_REPRESENTATION",
	"CurveSweptSolidShapeRepresentation":               "SURFACE_CURVE_SWEPT_AREA_SOLID",
	"EdgeBasedWireframeShapeRepresentation":            "EDGE_BASED_WIREFRAME_SHAPE_REPRESENTATION",
	"ElementaryBrepShapeRepresentation":                "ELEMENTARY_BREP_SHAPE_REPRESENTATION",
	"FacetedBrepShapeRepresentation":                   "FACETED_BREP_SHAPE_REPRESENTATION",
	"GeometricallyBounded2dWireframeRepresentation":    "GEOMETRICALLY_BOUNDED_2D_WIREFRAME_REPRESENTATION",
	"GeometricallyBoundedSurfaceShapeRepresentation":   "GEOMETRICALLY_BOUNDED_SURFACE_SHAPE_REPRESENTATION",
	"GeometricallyBoundedWireframeShapeRepresentation": "GEOMETRICALLY_BOUNDED_WIREFRAME_SHAPE_REPRESENTATION",
	"ManifoldSubsurfaceShapeRepresentation":            "MANIFOLD_SUBSURFACE_SHAPE_REPRESENTATION",
	"ManifoldSurfaceShapeRepresentation":               "MANIFOLD_SURFACE_SHAPE_REPRESENTATION",
	"NonManifoldSurfaceShapeRepresentation":            "NON_MANIFOLD_SURFACE_SHAPE_REPRESENTATION",
	"ShellBasedWireframeShapeRepresentation":           "SHELL_BASED_WIREFRAME_SHAPE_REPRESENTATION",
	"NeutralSketchRepresentation":                      "NEUTRAL_SKETCH_REPRESENTATION",
	"Csg2DShapeRepresentation":                         "CSG_2D_SHAPE_REPRESENTATION",
	"individualShapeRepresentation":                    "SHAPE_DEFINITION_REPRESENTATION",
}

type FileDescription struct {
	Description         string
	ImplementationLevel string
}
type FileName struct {
	Name                string
	TimeStamp           string
	Author              []string
	Organization        string
	PreprocessorVersion string
	OriginatingSystem   string
	Authorization       string
}
type FileSchema struct {
	Schema string
}
type Header struct {
	FileDescription FileDescription
	FileName        FileName
	FileSchema      FileSchema
}
type ProductContext struct {
	Count       int
	Type        string
	Id          string
	Name        string
	Ref         string
	ContextText string
}
type Product struct {
	Count   int
	Type    string
	Id      string
	Name    string
	Comment string
	Ref     string
}
type ProductDefinitionFormation struct {
	Count   int
	Type    string
	Id      string
	Name    string
	Comment string
	Ref     string
}
type ProductDefinition struct {
	Count   int
	Type    string
	Id      string
	Name    string
	Comment string
	Ref_1   string
	Ref_2   string
}
type ProductDefinitionContext struct {
	Count   int
	Id      string
	Type    string
	Name    string
	Comment string
	Ref     string
}
type ProductDefinitionShape struct {
	Count   int
	Type    string
	Id      string
	Name    string
	Comment string
	Ref     string
}
type Direction struct {
	Count int
	Type  string
	Id    string
	Value []string
}
type CartesianPoint struct {
	Count int
	Type  string
	Id    string
	Value []string
}
type AxisPlacement struct {
	Count          int
	Type           string
	Id             string
	CartesianPoint CartesianPoint
	DirectionX     Direction
	// DirectionY Direction
}
type ShapeRepresentation struct {
	Count         int
	Type          string
	Id            string
	Ref           []string
	AxisPlacement []AxisPlacement
}
type GeometricallyBounded struct {
	Count      int
	Type       string
	Id         string
	Context    string
	CurveSet   []GeometricCurveSet
	RepContext RepresentationContext
}
type GeometricCurveSet struct {
	Count  int
	Type   string
	Id     string
	Curves []UniformCurve
}
type UniformCurve struct {
	Count         int
	Type          string
	Id            string
	Degree        int
	ControlPoints []CartesianPoint
	SelfIntersect bool
	ClosedCurve   bool
}
type RepresentationContext struct {
	Count     int
	Type      string
	Dimension int
}
type LineOrderWithIndex struct {
	line  string
	index int
}
type extractedData struct {
	graph                         sst.NamedGraph
	productMap                    map[sst.IBNode]Product
	productDefinitionFormationMap map[sst.IBNode]ProductDefinitionFormation
	productDefinitionMap          map[sst.IBNode]ProductDefinition
	shapeRepresentationMap        map[sst.IBNode]ShapeRepresentation
	geometricallyBoundedMap       map[sst.IBNode]GeometricallyBounded
	totalLength                   int
}

func newExtractedData(graph sst.NamedGraph) *extractedData {
	ex := new(extractedData)
	ex.graph = graph
	ex.productMap = make(map[sst.IBNode]Product)
	ex.productDefinitionFormationMap = make(map[sst.IBNode]ProductDefinitionFormation)
	ex.productDefinitionMap = make(map[sst.IBNode]ProductDefinition)
	ex.shapeRepresentationMap = make(map[sst.IBNode]ShapeRepresentation)
	ex.geometricallyBoundedMap = make(map[sst.IBNode]GeometricallyBounded)
	ex.totalLength = 3
	return ex
}

func SSTToStepExport(sstFileName *bufio.Reader) {
	st, err := sst.RdfRead(bufio.NewReader(sstFileName), sst.RdfFormatTurtle, sst.StrictHandler, sst.DefaultTriplexMode)
	if err != nil {
		log.Panic(err)
	}
	ToStepExporter(st.NamedGraphs()[0], "")
}

func ToStepExporter(graph sst.NamedGraph, fileLocation string) error {
	ex := newExtractedData(graph)

	// open the file for writing
	file, err := os.Create(filepath.Dir(fileLocation) + "/step-output-new.stp")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()

	// handle header
	writeHeader(file)

	// Get the outer nodes
	outerNodes, _ := getOuterNodes(graph)

	// Process the outer nodes
	for _, outerNode := range outerNodes {
		ex.processNodes(outerNode)
	}

	var builder strings.Builder

	// write processed sst data output
	applicationContext := "#1"
	applicationProtocol := "#2"
	builder.WriteString(fmt.Sprintf("%s=APPLICATION_CONTEXT('design description');\n", applicationContext))
	builder.WriteString(fmt.Sprintf("%s=APPLICATION_PROTOCOL_DEFINITION('design description', 'design type', 2023, %s);\n", applicationProtocol, applicationContext))

	for selectedNode, product := range ex.productMap {
		// create product context for each product
		productContext := ProductContext{
			Count: ex.totalLength,
			Type:  "PRODUCT_CONTEXT",
			Id:    applicationProtocol,
			Name:  "EWH",
		}
		builder.WriteString(fmt.Sprintf("#%d=%s('%s',%s,'%s');\n", productContext.Count, productContext.Type, productContext.Ref, productContext.Id, productContext.Name))
		ex.totalLength++

		// product
		product.Count = ex.totalLength
		product.Ref = "#" + strconv.Itoa(productContext.Count)
		builder.WriteString(fmt.Sprintf("#%d=%s('%s','%s','%s',(%s));\n", product.Count, product.Type, product.Id, product.Name, product.Comment, product.Ref))
		ex.totalLength++

		builder.WriteString(fmt.Sprintf("#%d=PRODUCT_RELATED_PRODUCT_CATEGORY('part',$,(#%d));\n", ex.totalLength, product.Count))
		ex.totalLength++

		// productDefinitionFormation
		productDefinitionFormation := ex.productDefinitionFormationMap[selectedNode]
		productDefinitionFormation.Type = "PRODUCT_DEFINITION_FORMATION"
		productDefinitionFormation.Count = ex.totalLength
		productDefinitionFormation.Ref = "#" + strconv.Itoa(product.Count)
		builder.WriteString(fmt.Sprintf("#%d=%s('%s','%s',%s);\n", productDefinitionFormation.Count, productDefinitionFormation.Type, productDefinitionFormation.Id, productDefinitionFormation.Name, productDefinitionFormation.Ref))

		ex.totalLength++

		// ProductDefinitionContext
		defContext := ProductDefinitionContext{
			Count: ex.totalLength,
			Type:  "PRODUCT_DEFINITION_CONTEXT",
			Id:    "part definition",
			Name:  "design",
			Ref:   applicationContext,
		}

		builder.WriteString(fmt.Sprintf("#%d=%s('%s',(%s),'%s');\n", defContext.Count, defContext.Type, defContext.Id, applicationContext, defContext.Name))
		ex.totalLength++

		// ProductDefinition
		productDefinition := ProductDefinition{
			Count: ex.totalLength,
			Type:  "PRODUCT_DEFINITION",
			Id:    "design",
			Name:  "",
			Ref_1: "#" + strconv.Itoa(productDefinitionFormation.Count),
			Ref_2: "#" + strconv.Itoa(defContext.Count),
		}
		builder.WriteString(fmt.Sprintf(productDefinitionFormat, productDefinition.Count, productDefinition.Type, productDefinition.Id, productDefinition.Name, productDefinition.Ref_1, productDefinition.Ref_2))
		ex.totalLength++

		// ProductDefinitionShape
		definitionShape := ProductDefinitionShape{
			Count: ex.totalLength,
			Type:  "PRODUCT_DEFINITION_SHAPE",
			Id:    "",
			Name:  "",
			Ref:   "#" + strconv.Itoa(productDefinition.Count),
		}
		builder.WriteString(fmt.Sprintf(productDefinitionShapeFormat, definitionShape.Count, definitionShape.Type, definitionShape.Id, definitionShape.Name, definitionShape.Ref))
		ex.totalLength++

		// Geometrically bounded
		geometricallyBoundedID := ""     // Get Geometrically bounded ID
		shapeRepresentationID := ""      // Get Shape Representation ID
		goeRepresentationContextID := "" // Get Shape Representation ID
		for _, geometry := range ex.geometricallyBoundedMap {
			geometry.Count = ex.totalLength
			geometricallyBoundedID = "#" + strconv.Itoa(geometry.Count)
			geometry.Id = ""
			collectGeometryData := []string{}
			ex.totalLength++

			for _, curve := range geometry.CurveSet {
				curve.Count = ex.totalLength
				curve.Id = ""
				collectGeometryData = append(collectGeometryData, "#"+strconv.Itoa(curve.Count))
				collectUniformCurveData := []string{}
				ex.totalLength++

				for _, uniformCurve := range curve.Curves {
					uniformCurve.Count = ex.totalLength
					collectUniformCurveData = append(collectUniformCurveData, "#"+strconv.Itoa(uniformCurve.Count))
					uniformCurve.Id = ""
					collectCartesianData := []string{}
					ex.totalLength++

					for _, controlPoint := range uniformCurve.ControlPoints {
						controlPoint.Count = ex.totalLength
						collectCartesianData = append(collectCartesianData, "#"+strconv.Itoa(controlPoint.Count))
						cartesianData := strings.Join(controlPoint.Value, ",")
						builder.WriteString(fmt.Sprintf("#%d=%s('%s',(%s));\n", controlPoint.Count, controlPoint.Type, controlPoint.Id, cartesianData))
						ex.totalLength++
					}
					collectIDS := strings.Join(collectCartesianData, ",")
					builder.WriteString(fmt.Sprintf("#%d=%s('%s',%d,(%s),%s,%s);\n", uniformCurve.Count, uniformCurve.Type, uniformCurve.Id, uniformCurve.Degree, collectIDS, ex.convertBool(uniformCurve.SelfIntersect), ex.convertBool(uniformCurve.ClosedCurve)))
				}
				collectIDS := strings.Join(collectUniformCurveData, ",")
				builder.WriteString(fmt.Sprintf("#%d=%s('%s',(%s));\n", curve.Count, curve.Type, curve.Id, collectIDS))
			}

			if geometry.RepContext != (RepresentationContext{}) {
				geometry.RepContext.Count = ex.totalLength
				goeRepresentationContextID = "#" + strconv.Itoa(geometry.RepContext.Count)
				builder.WriteString(fmt.Sprintf("#%d=%s(%d);\n", geometry.RepContext.Count, geometry.RepContext.Type, geometry.RepContext.Dimension))
			}
			collectIDS := strings.Join(collectGeometryData, ",")
			builder.WriteString(fmt.Sprintf("#%d=%s('',(%s),#%d);\n", geometry.Count, geometry.Type, collectIDS, geometry.RepContext.Count))
			ex.totalLength++
		}

		// shapeRepresentation
		for _, shapeRepresentation := range ex.shapeRepresentationMap {
			shapeRepresentation.Count = ex.totalLength
			shapeRepresentationID = "#" + strconv.Itoa(shapeRepresentation.Count)
			shapeRepresentation.Id = ""
			axisData := strings.Join(shapeRepresentation.Ref, ",")
			builder.WriteString(fmt.Sprintf("#%d=%s('%s',(%s),%s,%s);\n", shapeRepresentation.Count, shapeRepresentation.Type, shapeRepresentation.Id, axisData, geometricallyBoundedID, goeRepresentationContextID))
			ex.totalLength++

			builder.WriteString(fmt.Sprintf("#%d=SHAPE_DEFINITION_REPRESENTATION(#%d,#%d);\n", ex.totalLength, definitionShape.Count, shapeRepresentation.Count))
			ex.totalLength++

			for _, axisPlacement := range shapeRepresentation.AxisPlacement {
				cartesianData := strings.Join(axisPlacement.CartesianPoint.Value, ",")
				axisPlacement.CartesianPoint.Count = ex.totalLength
				ex.totalLength++

				directionXData := strings.Join(axisPlacement.DirectionX.Value, ",")
				axisPlacement.DirectionX.Count = ex.totalLength
				ex.totalLength++

				builder.WriteString(fmt.Sprintf("#%d=%s('%s',#%d,#%d);\n", axisPlacement.Count, axisPlacement.Type, axisPlacement.Id, axisPlacement.CartesianPoint.Count, axisPlacement.DirectionX.Count))
				builder.WriteString(fmt.Sprintf("#%d=%s('%s',(%s));\n", axisPlacement.CartesianPoint.Count, axisPlacement.CartesianPoint.Type, axisPlacement.CartesianPoint.Id, cartesianData))
				builder.WriteString(fmt.Sprintf("#%d=%s('%s',(%s));\n", axisPlacement.DirectionX.Count, axisPlacement.DirectionX.Type, axisPlacement.DirectionX.Id, directionXData))
			}
		}

		if shapeRepresentationID != "" && geometricallyBoundedID != "" {
			builder.WriteString(fmt.Sprintf("#%d=REPRESENTATION_RELATIONSHIP('','',%s,%s);\n", ex.totalLength, shapeRepresentationID, geometricallyBoundedID))
			ex.totalLength++
		}

		// Sort the lines based on the index

		lines := strings.Split(builder.String(), "\n")
		// Create a slice of LineOrderWithIndex structs
		lineSlice := make([]LineOrderWithIndex, len(lines))
		for i, line := range lines {
			if strings.Contains(line, "=") {
				num, err := strconv.Atoi(strings.Split(line, "=")[0][1:])
				if err != nil {
					// Handle error
				}
				lineSlice[i] = LineOrderWithIndex{line, num}
			} else {
				lineSlice[i] = LineOrderWithIndex{line, -1}
			}
		}

		// Sort the slice of LineOrderWithIndex structs based on the index field
		sort.Slice(lineSlice, func(i, j int) bool {
			return lineSlice[i].index < lineSlice[j].index
		})

		// Extract the sorted lines
		sortedLines := make([]string, len(lineSlice))
		for i, lineWithIndex := range lineSlice {
			sortedLines[i] = lineWithIndex.line
		}

		// Join the sorted lines back into a single string
		sortedString := strings.Join(sortedLines, "\n")

		file.WriteString(sortedString)
	}

	// write the footer to the file
	_, err = file.WriteString("\nENDSEC;\n")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	_, err = file.WriteString("END-ISO-10303-21;\n")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}

	return nil
}

func getOuterNodes(graph sst.NamedGraph) (map[string]sst.IBNode, error) {
	nodeMap := make(map[string]sst.IBNode)
	err := graph.ForIRINodes(func(t sst.IBNode) error {
		if t.TypeOf() != nil {
			switch t.TypeOf().InVocabulary().(type) {
			case sso.KindPart:
				nodeMap[string(t.Fragment())] = t
			case lci.KindOrganization:
				nodeMap[string(t.Fragment())] = t
			case sso.KindShapeFeatureDefinition:
				nodeMap[string(t.Fragment())] = t
			case rep.KindRepresentationContext:
				nodeMap[string(t.Fragment())] = t
			case lci.KindPropertyDefinition:
				nodeMap[string(t.Fragment())] = t
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nodeMap, nil
}

func (ex *extractedData) convertBool(value bool) string {
	if value {
		return ".T."
	} else {
		return ".F."
	}
}

func (ex *extractedData) processNodes(node sst.IBNode) {
	switch node.TypeOf().InVocabulary().(type) {
	case sso.IsPart:
		var wiringHarnessNode sst.IBNode
		node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
			if o.TermKind() == sst.TermKindIBNode {
				if o.(sst.IBNode).Is(sso.PartCategory_wiring_harness) {
					wiringHarnessNode = s
				}
			}
			return nil
		})
		if wiringHarnessNode != nil {
			ex.processStepNodes(wiringHarnessNode)
		}
	default:
	}
}

func (ex *extractedData) processStepNodes(node sst.IBNode) {
	product := Product{}
	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		product.Type = "PRODUCT"
		if o.TermKind() == sst.TermKindLiteral {
			extractTextContent(p, o.(sst.Literal), &product)
		}

		if p.Is(sso.HasPartVersion) {
			productDef := o.(sst.IBNode).GetObjects(sso.HasProductDefinition)
			for _, def := range productDef {
				productDefinitionFormation := ProductDefinitionFormation{}
				def.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					if o.TermKind() == sst.TermKindLiteral {
						extractTextContent(p, o.(sst.Literal), &productDefinitionFormation)
					}
					if p.Is(sso.DefiningGeometry) {
						shapeRepresentation := ShapeRepresentation{}
						shapeRepresentation.Count = ex.totalLength
						shapeRepresentation.Type = shapeContent[o.(sst.IBNode).TypeOf().Fragment()]
						o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
							if o.TermKind() == sst.TermKindIBNode {
								if p.Is(rep.Item) {
									ex.processShapeRepresentation(o.(sst.IBNode), &shapeRepresentation)
									ex.shapeRepresentationMap[node] = shapeRepresentation
									ex.totalLength++
								}
								if p.Is(rep.DefinitionalRepresentationRelationshipWithSameContext) {
									ex.processShapeGeometry(o.(sst.IBNode))
								}
							}
							return nil
						})

					}
					return nil
				})
				ex.productDefinitionFormationMap[node] = productDefinitionFormation
			}
		}
		return nil
	})
	ex.productMap[node] = product
}

func (ex *extractedData) processShapeRepresentation(node sst.IBNode, shapeRepresentation *ShapeRepresentation) {
	direction := Direction{}
	axisPlacement := AxisPlacement{}
	cartesianPoint := CartesianPoint{}
	axisPlacement.Count = ex.totalLength
	axisPlacement.Type = shapeContent[node.TypeOf().Fragment()]

	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.Location) {
			cartesianPoint = ex.processCartesianPoint(o.(sst.IBNode))
		}
		if p.Is(rep.RefDirection) || p.Is(rep.Axis) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				direction.Type = "DIRECTION"
				if p.Is(rep.DirectionRatios) {
					var coordinateParts []string
					for _, v := range o.(sst.LiteralCollection).Values() {
						coordinateParts = append(coordinateParts, fmt.Sprintf("%v", v))
					}
					direction.Value = coordinateParts
				}
				return nil
			})
		}
		return nil
	})
	axisPlacement.CartesianPoint = cartesianPoint
	axisPlacement.DirectionX = direction
	shapeRepresentation.AxisPlacement = append(shapeRepresentation.AxisPlacement, axisPlacement)
	shapeRepresentation.Ref = append(shapeRepresentation.Ref, "#"+strconv.Itoa(axisPlacement.Count))
}

func (ex *extractedData) processCartesianPoint(node sst.IBNode) CartesianPoint {
	cartesianPoint := CartesianPoint{}
	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		cartesianPoint.Type = "CARTESIAN_POINT"
		if p.Is(rep.Coordinates) {
			var coordinateParts []string
			for _, v := range o.(sst.LiteralCollection).Values() {
				coordinateParts = append(coordinateParts, fmt.Sprintf("%v", v))
			}
			cartesianPoint.Value = coordinateParts
		}
		if o.TermKind() == sst.TermKindLiteral {
			extractTextContent(p, o.(sst.Literal), &cartesianPoint)
		}
		return nil
	})
	return cartesianPoint
}

func (ex *extractedData) processShapeGeometry(node sst.IBNode) {
	geometricallyBounded := GeometricallyBounded{}
	geometricCurveSet := GeometricCurveSet{}
	representationContext := RepresentationContext{}
	node.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rdf.Type) {
			geometricallyBounded.Type = shapeContent[o.(sst.IBNode).Fragment()]
		}
		if p.Is(rep.Item) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				uniformCurve := UniformCurve{}
				if p.Is(rdf.Type) {
					geometricCurveSet.Type = shapeContent[o.(sst.IBNode).Fragment()]
				}
				if p.Is(rep.Element) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rdf.Type) {
							uniformCurve.Type = shapeContent[o.(sst.IBNode).Fragment()]
						}
						if p.Is(rep.ControlPointsList) {
							if itemList, ok := o.(sst.IBNode).AsCollection(); ok {
								itemList.ForMembers(func(_ int, o sst.Term) {
									if o.TermKind() == sst.TermKindIBNode {
										cartesianPoint := ex.processCartesianPoint(o.(sst.IBNode))
										uniformCurve.ControlPoints = append(uniformCurve.ControlPoints, cartesianPoint)
									}
								})
							}
						}
						if o.TermKind() == sst.TermKindLiteral {
							if p.Is(rep.ClosedCurve) {
								uniformCurve.ClosedCurve = bool(o.(sst.Boolean))
							}
							if p.Is(rep.Degree) {
								uniformCurve.Degree = int(o.(sst.Integer))
							}
							if p.Is(rep.SelfIntersect) {
								uniformCurve.SelfIntersect = bool(o.(sst.Boolean))
							}
						}
						return nil
					})
				}
				if uniformCurve.Type != "" {
					geometricCurveSet.Curves = append(geometricCurveSet.Curves, uniformCurve)
				}
				return nil
			})
		}
		if p.Is(rep.ContextOfItems) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rdf.Type) {
					representationContext.Type = shapeContent[o.(sst.IBNode).Fragment()]
				}
				if o.TermKind() == sst.TermKindLiteral {
					if p.Is(rep.CoordinateSpaceDimension) {
						representationContext.Dimension = int(o.(sst.Integer))
					}
				}
				return nil
			})
		}
		return nil
	})
	geometricallyBounded.RepContext = representationContext
	geometricallyBounded.CurveSet = append(geometricallyBounded.CurveSet, geometricCurveSet)
	ex.geometricallyBoundedMap[node] = geometricallyBounded
}

func extractTextContent[T any](p sst.IBNode, data sst.Literal, part T) {
	val := reflect.ValueOf(part).Elem()
	switch p.InVocabulary().(type) {
	case sso.IsID, sso.IsVersionID, sso.IsViewID:
		idVal := val.FieldByName("Id")
		if idVal != (reflect.Value{}) {
			idVal.SetString(string(data.(sst.String)))
		}
	case rdfs.IsLabel:
		nameVal := val.FieldByName("Name")
		if nameVal != (reflect.Value{}) {
			nameVal.SetString(string(data.(sst.String)))
		}
	case rdfs.IsComment:
		commentVal := val.FieldByName("Comment")
		if commentVal != (reflect.Value{}) {
			commentVal.SetString(string(data.(sst.String)))
		}
	}
}

func writeHeader(file *os.File) error {
	header := &Header{
		FileDescription: FileDescription{
			Description:         "EWH test case with basic topological structure",
			ImplementationLevel: "1",
		},
		FileName: FileName{
			Name:                "EWH-Topology1",
			TimeStamp:           time.Now().UTC().Format(time.RFC3339),
			Author:              []string{""},
			Organization:        "LKSoft.com",
			PreprocessorVersion: "",
			OriginatingSystem:   "SST Semantic STEP Technology API",
			Authorization:       "",
		},
		FileSchema: FileSchema{
			Schema: "AP242_MANAGED_MODEL_BASED_3D_ENGINEERING_MIM_LF",
		},
	}

	_, err := fmt.Fprintln(file, "ISO-10303-21;")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(file, "HEADER;")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "\tFILE_DESCRIPTION(\n\t/* description */\t('%s'),\n\t/* implementation_level */\t'%s');\n", header.FileDescription.Description, header.FileDescription.ImplementationLevel)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "\tFILE_NAME(\n\t/* name */\t'%s',\n\t/* time_stamp */\t'%s',\n\t/* author */\t('%s'),\n\t/* organization */\t('%s'),\n\t/* preprocessor_version */\t'%s',\n\t/* originating_system */\t'%s',\n\t/* authorization */\t'%s');\n", header.FileName.Name, header.FileName.TimeStamp, strings.Join(header.FileName.Author, ", "), header.FileName.Organization, header.FileName.PreprocessorVersion, header.FileName.OriginatingSystem, header.FileName.Authorization)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(file, "\tFILE_SCHEMA(\n\t('%s'));\n", header.FileSchema.Schema)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(file, "ENDSEC;\\N\\")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(file, "DATA;")
	if err != nil {
		return err
	}

	return err
}

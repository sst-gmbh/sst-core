// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// This package converts a SVG file into a coressponding SST 2D geometry representation.
package svgtosst

import (
	"encoding/xml"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"git.semanticstep.net/x/sst/sst"
	_ "git.semanticstep.net/x/sst/vocabularies/dict"
	_ "git.semanticstep.net/x/sst/vocabularies/lci"
	"git.semanticstep.net/x/sst/vocabularies/rdf"
	"git.semanticstep.net/x/sst/vocabularies/rdfs"
	"git.semanticstep.net/x/sst/vocabularies/rep"
	"git.semanticstep.net/x/sst/vocabularies/sso"
	"github.com/google/uuid"
)

type SVG struct {
	XMLName   xml.Name   `xml:"svg"`
	Width     string     `xml:"width,attr"`
	Height    string     `xml:"height,attr"`
	ViewBox   string     `xml:"viewBox,attr"`
	XMLNS     string     `xml:"xmlns,attr"`
	Version   string     `xml:"version,attr"`
	Title     string     `xml:"title"`
	Desc      string     `xml:"desc"`
	Rects     []Rect     `xml:"rect"`
	Texts     []Text     `xml:"text"`
	Circles   []Circle   `xml:"circle"`
	Ellipses  []Ellipse  `xml:"ellipse"`
	Lines     []Line     `xml:"line"`
	Paths     []Path     `xml:"path"`
	Polygons  []Polygon  `xml:"polygon"`
	Polylines []Polyline `xml:"polyline"`
	Groups    []Group    `xml:"g"`
}

type Group struct {
	XMLName xml.Name `xml:"g"`
	Style   string   `xml:"style,attr,omitempty"`
	ShapeStyle
	Transform string     `xml:"transform,attr,omitempty"`
	Rects     []Rect     `xml:"rect"`
	Texts     []Text     `xml:"text"`
	Circles   []Circle   `xml:"circle"`
	Ellipses  []Ellipse  `xml:"ellipse"`
	Lines     []Line     `xml:"line"`
	Paths     []Path     `xml:"path"`
	Polygons  []Polygon  `xml:"polygon"`
	Polylines []Polyline `xml:"polyline"`
	Groups    []Group    `xml:"g"`
}

type ShapeStyle struct {
	Fill        string `xml:"fill,attr"`
	Stroke      string `xml:"stroke,attr"`
	StrokeWidth string `xml:"stroke-width,attr"`
}

type Shape interface {
	GetStyle() ShapeStyle
	GetType() string
	GetDirection() float64
}

type StyledShape interface {
	Shape
}

type Rect struct {
	XMLName xml.Name `xml:"rect"`
	Style   string   `xml:"style,attr"`
	XLength float64  `xml:"width,attr"`
	YLength float64  `xml:"height,attr"`
	X       float64  `xml:"x,attr"`
	Y       float64  `xml:"y,attr"`
	RX      float64  `xml:"rx,attr"`
	RY      float64  `xml:"ry,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (r Rect) GetStyle() ShapeStyle {
	return r.ShapeStyle
}

func (r Rect) GetType() string {
	return "Rect"
}

func (r Rect) GetDirection() float64 {
	return r.Direction
}

type Text struct {
	XMLName        xml.Name `xml:"text"`
	Style          string   `xml:"style,attr"`
	X              float64  `xml:"x,attr"`
	Y              float64  `xml:"y,attr"`
	Content        string   `xml:",chardata"`
	FontFamily     string   `xml:"font-family,attr,omitempty"`
	FontSize       float64  `xml:"font-size,attr,omitempty"`
	FontStyle      string   `xml:"font-style,attr,omitempty"`
	FontWeight     string   `xml:"font-weight,attr,omitempty"`
	TextDecoration string   `xml:"text-decoration,attr,omitempty"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (t Text) GetStyle() ShapeStyle {
	return t.ShapeStyle
}

func (t Text) GetType() string {
	return "Text"
}

func (t Text) GetDirection() float64 {
	return t.Direction
}

type Circle struct {
	CX     float64 `xml:"cx,attr"`
	CY     float64 `xml:"cy,attr"`
	Radius float64 `xml:"r,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (c Circle) GetStyle() ShapeStyle {
	return c.ShapeStyle
}

func (c Circle) GetType() string {
	return "Circle"
}

func (c Circle) GetDirection() float64 {
	return c.Direction
}

type Ellipse struct {
	CX float64 `xml:"cx,attr"`
	CY float64 `xml:"cy,attr"`
	RX float64 `xml:"rx,attr"`
	RY float64 `xml:"ry,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (e Ellipse) GetStyle() ShapeStyle {
	return e.ShapeStyle
}

func (e Ellipse) GetType() string {
	return "Ellipse"
}

func (e Ellipse) GetDirection() float64 {
	return e.Direction
}

type Line struct {
	XMLName xml.Name `xml:"line"`
	X1      float64  `xml:"x1,attr"`
	Y1      float64  `xml:"y1,attr"`
	X2      float64  `xml:"x2,attr"`
	Y2      float64  `xml:"y2,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (l Line) GetStyle() ShapeStyle {
	return l.ShapeStyle
}

func (l Line) GetType() string {
	return "Line"
}

func (l Line) GetDirection() float64 {
	return l.Direction
}

type Polygon struct {
	XMLName xml.Name `xml:"polygon"`
	Points  string   `xml:"points,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (p Polygon) GetStyle() ShapeStyle {
	return p.ShapeStyle
}

func (p Polygon) GetType() string {
	return "Polygon"
}

func (p Polygon) GetDirection() float64 {
	return p.Direction
}

type Polyline struct {
	XMLName xml.Name `xml:"polyline"`
	Points  string   `xml:"points,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (p Polyline) GetStyle() ShapeStyle {
	return p.ShapeStyle
}

func (p Polyline) GetType() string {
	return "Polyline"
}

func (p Polyline) GetDirection() float64 {
	return p.Direction
}

type Point struct {
	X float64
	Y float64
}

type Path struct {
	XMLName xml.Name `xml:"path"`
	D       string   `xml:"d,attr"`
	ShapeStyle
	Transform string `xml:"transform,attr,omitempty"`
	Direction float64
}

func (p Path) GetStyle() ShapeStyle {
	return p.ShapeStyle
}

func (p Path) GetType() string {
	return "Path"
}

func (p Path) GetDirection() float64 {
	return p.Direction
}

type PathCommand struct {
	Command string
	Params  []float64
}

type EllipticalArc struct {
	RX            float64
	RY            float64
	StartX        float64
	StartY        float64
	EndX          float64
	EndY          float64
	XAxisRotation float64
	LargeArcFlag  bool
	SweepFlag     bool
}

type QuadraticBezier struct {
	StartX   float64
	StartY   float64
	ControlX float64
	ControlY float64
	EndX     float64
	EndY     float64
}

type CubicBezier struct {
	StartX    float64
	StartY    float64
	ControlX1 float64
	ControlY1 float64
	ControlX2 float64
	ControlY2 float64
	EndX      float64
	EndY      float64
}

// func main() {
// 	// Transform()
// 	dir := "./svg"
// 	// dir := "./svgTransformTest"

// 	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			return err
// 		}

// 		if !info.IsDir() && filepath.Ext(path) == ".svg" {
// 			fmt.Println("Found SVG file:", path)
// 			processFile(path)
// 		}

// 		return nil
// 	})

// 	if err != nil {
// 		fmt.Printf("Error walking the path %q: %v\n", dir, err)
// 	}
// }

// func main() {
// 	svgContent := `
// 	<svg xmlns="http://www.w3.org/2000/svg" version="1.1">
// 	<title>Example SVG</title>
// 	<desc>A simple example</desc>
// 	<rect x="10" y="10" width="100" height="100" fill="blue"/>
// 	</svg>`

// 	err := ConvertSVGFromText(svgContent, "example_svg", "")
// 	if err != nil {
// 		log.Fatalf("Conversion failed: %v", err)
// 	}

// 	err = ConvertSVGFromText(svgContent, "example_svg", "./outputDirectory")
// 	if err != nil {
// 		log.Fatalf("Conversion failed: %v", err)
// 	}
// }

// func processFile(fileName string) {
// 	// Open SVG file
// 	// fileName := "svg/circle.svg"
// 	file, err := os.Open(fileName)
// 	if err != nil {
// 		fmt.Println("Error opening file:", err)
// 		return
// 	}
// 	defer file.Close()

// 	// Read file content
// 	data, err := io.ReadAll(file)
// 	if err != nil {
// 		fmt.Println("Error reading file:", err)
// 		return
// 	}

// 	// Unmarshal XML into struct
// 	var svg SVG
// 	err = xml.Unmarshal(data, &svg)
// 	if err != nil {
// 		fmt.Println("Error unmarshalling SVG:", err)
// 		return
// 	}

// 	// Create a new SST stage
// 	stage := sst.OpenStage(sst.DefaultTriplexMode)
// 	graph, err := stage.CreateNamedGraph("")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Create a new itemUUIDs slice for each file
// 	var itemUUIDs []sst.Object

// 	// Create nodes for each SVG element
// 	createNodesForGroups(graph, svg.Groups, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForTexts(graph, svg.Texts, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForRects(graph, svg.Rects, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForCircles(graph, svg.Circles, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForEllipses(graph, svg.Ellipses, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForPolygons(graph, svg.Polygons, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForPolylines(graph, svg.Polylines, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForLines(graph, svg.Lines, &itemUUIDs, "", ShapeStyle{})
// 	createNodesForPaths(graph, svg.Paths, &itemUUIDs, "", ShapeStyle{})

// 	// Create GeometricRepresentationContext node
// 	contextNode, err := createGeometricRepresentationContext(graph)
// 	if err != nil {
// 		log.Panic(err)
// 	}

// 	// Create SymbolRepresentation node
// 	createSymbolRepresentation(graph, contextNode, itemUUIDs, svg.Title, svg.Desc)

// 	// Ensure output directories exist
// 	err = ensureDirectoriesExist("ttlOutput", "sstOutput")
// 	if err != nil {
// 		log.Panic(err)
// 	}

// 	// Export graph to Turtle file
// 	baseName := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))

// 	turtleFileName := "ttlOutput/" + baseName + ".ttl"
// 	f, err := os.Create(turtleFileName)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer f.Close()

// 	err = graph.RdfWrite(f, sst.RdfFormatTurtle)
// 	if err != nil {
// 		log.Panic(err)
// 	}

// 	// Export graph to binary SST file
// 	sstFileName := "sstOutput/" + baseName + ".sst"
// 	out, err := os.Create(sstFileName)
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	defer func() {
// 		_ = out.Close()
// 	}()
// 	err = graph.SstWrite(out)
// 	if err != nil {
// 		log.Panic(err)
// 	}

// 	fmt.Println("Done")
// }

// ConvertSVGFromText converts an SVG content string into RDF/Turtle and SST formats.
// It processes the provided SVG content, creates a graph representation using the SST library,
// and writes the result to files in the specified output directory.
// The output consists of two files: one in Turtle (.ttl) format and one in binary SST (.sst) format.
//
// Parameters:
// - svgContent: A string representing the SVG content in XML format.
// - baseName: The base name used for generating the output file names. The output files will be named baseName.ttl and baseName.sst.
// - outputDir: The directory where the output files will be saved. If this is an empty string, the current directory is used.
//
// Returns:
// - error: An error is returned if any part of the conversion or file writing process fails, otherwise nil.
//
// Example:
//
//	svgContent := `<svg xmlns="http://www.w3.org/2000/svg" version="1.1"><title>Example SVG</title></svg>`
//	err := ConvertSVGFromText(svgContent, "example_svg", "./output")
//	if err != nil {
//	    log.Fatalf("Conversion failed: %v", err)
//	}

func ConverSvgToGraph(svgContent string, id uuid.UUID) (sst.NamedGraph, error) {
	var svg SVG
	err := xml.Unmarshal([]byte(svgContent), &svg)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling SVG: %v", err)
	}

	// Create a new SST stage
	stage := sst.OpenStage(sst.DefaultTriplexMode)
	graph := stage.CreateNamedGraph(sst.IRI(id.URN()))
	// if err != nil {
	// 	return nil, fmt.Errorf("error creating named graph: %v", err)
	// }

	// Create a new itemUUIDs slice for each file
	var itemUUIDs []sst.Term

	// Create nodes for each SVG element
	createNodesForGroups(graph, svg.Groups, &itemUUIDs, "", ShapeStyle{})
	createNodesForTexts(graph, svg.Texts, &itemUUIDs, "", ShapeStyle{})
	createNodesForRects(graph, svg.Rects, &itemUUIDs, "", ShapeStyle{})
	createNodesForCircles(graph, svg.Circles, &itemUUIDs, "", ShapeStyle{})
	createNodesForEllipses(graph, svg.Ellipses, &itemUUIDs, "", ShapeStyle{})
	createNodesForPolygons(graph, svg.Polygons, &itemUUIDs, "", ShapeStyle{})
	createNodesForPolylines(graph, svg.Polylines, &itemUUIDs, "", ShapeStyle{})
	createNodesForLines(graph, svg.Lines, &itemUUIDs, "", ShapeStyle{})
	createNodesForPaths(graph, svg.Paths, &itemUUIDs, "", ShapeStyle{})

	// Create GeometricRepresentationContext node
	contextNode, err := createGeometricRepresentationContext(graph)
	if err != nil {
		return nil, fmt.Errorf("error creating geometric representation context: %v", err)
	}

	createSymbolRepresentation(graph, contextNode, itemUUIDs, svg.Title, svg.Desc)
	return graph, nil
}

func ConvertSVGFromText(svgContent, baseName, outputDir string) error {
	if outputDir == "" {
		outputDir = "."
	}

	err := ensureDirectoriesExist(outputDir)
	if err != nil {
		return fmt.Errorf("error ensuring output directory exists: %v", err)
	}

	// Unmarshal XML into struct
	var svg SVG
	err = xml.Unmarshal([]byte(svgContent), &svg)
	if err != nil {
		return fmt.Errorf("error unmarshalling SVG: %v", err)
	}

	// Create a new SST stage
	stage := sst.OpenStage(sst.DefaultTriplexMode)
	graph := stage.CreateNamedGraph("")
	// if err != nil {
	// 	return fmt.Errorf("error creating named graph: %v", err)
	// }

	// Create a new itemUUIDs slice for each file
	var itemUUIDs []sst.Term

	// Create nodes for each SVG element
	createNodesForGroups(graph, svg.Groups, &itemUUIDs, "", ShapeStyle{})
	createNodesForTexts(graph, svg.Texts, &itemUUIDs, "", ShapeStyle{})
	createNodesForRects(graph, svg.Rects, &itemUUIDs, "", ShapeStyle{})
	createNodesForCircles(graph, svg.Circles, &itemUUIDs, "", ShapeStyle{})
	createNodesForEllipses(graph, svg.Ellipses, &itemUUIDs, "", ShapeStyle{})
	createNodesForPolygons(graph, svg.Polygons, &itemUUIDs, "", ShapeStyle{})
	createNodesForPolylines(graph, svg.Polylines, &itemUUIDs, "", ShapeStyle{})
	createNodesForLines(graph, svg.Lines, &itemUUIDs, "", ShapeStyle{})
	createNodesForPaths(graph, svg.Paths, &itemUUIDs, "", ShapeStyle{})

	// Create GeometricRepresentationContext node
	contextNode, err := createGeometricRepresentationContext(graph)
	if err != nil {
		return fmt.Errorf("error creating geometric representation context: %v", err)
	}

	createSymbolRepresentation(graph, contextNode, itemUUIDs, svg.Title, svg.Desc)

	turtleFileName := filepath.Join(outputDir, baseName+".ttl")
	f, err := os.Create(turtleFileName)
	if err != nil {
		return fmt.Errorf("error creating Turtle file: %v", err)
	}
	defer f.Close()

	err = graph.RdfWrite(f, sst.RdfFormatTurtle)
	if err != nil {
		return fmt.Errorf("error writing Turtle file: %v", err)
	}

	sstFileName := filepath.Join(outputDir, baseName+".sst")
	out, err := os.Create(sstFileName)
	if err != nil {
		return fmt.Errorf("error creating SST file: %v", err)
	}
	defer out.Close()

	err = graph.SstWrite(out)
	if err != nil {
		return fmt.Errorf("error writing SST file: %v", err)
	}

	fmt.Println("Conversion complete for base name:", baseName)
	return nil
}

func createNodesForGroups(graph sst.NamedGraph, groups []Group, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, group := range groups {
		combinedStyle := CombineShapeStyles(parentStyle, group.ShapeStyle)
		combinedTransform := CombineTransforms(parentTransform, group.Transform)

		createNodesForTexts(graph, group.Texts, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForRects(graph, group.Rects, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForCircles(graph, group.Circles, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForEllipses(graph, group.Ellipses, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForPolygons(graph, group.Polygons, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForPolylines(graph, group.Polylines, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForLines(graph, group.Lines, itemUUIDs, combinedTransform, combinedStyle)
		createNodesForPaths(graph, group.Paths, itemUUIDs, combinedTransform, combinedStyle)

		if len(group.Groups) > 0 {
			createNodesForGroups(graph, group.Groups, itemUUIDs, combinedTransform, combinedStyle)
		}
	}
}

func createNodeForShape(graph sst.NamedGraph, shape Shape) (sst.IBNode, error) {
	var node sst.IBNode
	var err error

	switch s := shape.(type) {
	case Text:
		node, err = createNodeForText(graph, s)
	case Rect:
		node, err = createNodeForRect(graph, s)
	case Circle:
		node, err = createNodeForCircle(graph, s)
	case Ellipse:
		node, err = createNodeForEllipse(graph, s)
	case Polygon:
		node, err = createNodeForPolygon(graph, s)
	case Polyline:
		node, err = createNodeForPolyline(graph, s)
	case Line:
		node, err = createNodeForLine(graph, s)
	case Path:
		node, err = createNodeForPath(graph, s)
	default:
		err = fmt.Errorf("unsupported shape type: %T", s)
	}

	return node, err
}

func createNodesForTexts(graph sst.NamedGraph, texts []Text, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, text := range texts {
		combinedTransform := CombineTransforms(parentTransform, text.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, text.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(text, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}

		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForText(graph sst.NamedGraph, text Text) (sst.IBNode, error) {
	// Create position
	position := createAxis2Placement2D(graph, text.X, text.Y, text.Direction)

	// Create text node
	textNode := graph.CreateIRINode(uuid.New().String())

	// Add statements to the text node
	textNode.AddStatement(rdf.Type, rep.TextLiteral)
	textNode.AddStatement(rep.Alignment, rep.TextAlignment_baseline_left)
	textNode.AddStatement(rep.Position, position)
	textNode.AddStatement(rep.TextDirection, rep.TextPath_right)
	textNode.AddStatement(rep.Literal, sst.String(text.Content))

	// Use createTextFont to create the font node
	fontNode, err := createTextFont(graph, text)
	if err != nil {
		log.Panic(err)
	}

	// Associate the font node with the text node
	textNode.AddStatement(rep.Font, fontNode)

	return textNode, nil
}

func createTextFont(graph sst.NamedGraph, text Text) (sst.IBNode, error) {

	// Create a separate node for the font
	fontNode := graph.CreateIRINode(uuid.New().String())

	fontNode.AddStatement(rdf.Type, rep.TextFont)
	if text.FontFamily != "" {
		fontNode.AddStatement(sso.ID, sst.String(text.FontFamily))
	}

	if text.FontSize != 0 {
		fontNode.AddStatement(rep.FontSize, sst.Double(text.FontSize))
	}

	if text.FontStyle == "italic" {
		fontNode.AddStatement(rep.FontModifier, rep.TextModifer_italic)
	}

	if text.FontWeight == "bold" {
		fontNode.AddStatement(rep.FontModifier, rep.TextModifer_bold)
	}

	if text.TextDecoration == "underline" {
		fontNode.AddStatement(rep.FontModifier, rep.TextModifer_underscore)
	} else if text.TextDecoration == "line-through" {
		fontNode.AddStatement(rep.FontModifier, rep.TextModifer_strikethrough)
	}

	return fontNode, nil
}

func createNodesForRects(graph sst.NamedGraph, rects []Rect, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, rect := range rects {
		combinedTransform := CombineTransforms(parentTransform, rect.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, rect.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(rect, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}
		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForRect(graph sst.NamedGraph, rect Rect) (sst.IBNode, error) {
	// Create Axis2Placement2D (Position) for the rectangle
	position := createAxis2Placement2D(graph, rect.X, rect.Y, rect.Direction)

	// Create a new node for the rectangle
	node := graph.CreateIRINode(uuid.New().String())

	// Check if the rectangle has rounded corners
	if rect.RX > 0 || rect.RY > 0 {
		// Rounded rectangle node creation
		node.AddStatement(rdf.Type, rep.RoundedRectangle)

		// Add RX and RY (radii for rounded corners)
		if rect.RX > 0 {
			node.AddStatement(rep.RadiusX, sst.Double(rect.RX))
		}
		if rect.RY > 0 {
			node.AddStatement(rep.RadiusY, sst.Double(rect.RY))
		}
	} else {
		// Regular rectangle node creation
		node.AddStatement(rdf.Type, rep.Rectangle)
	}

	// Add position and dimensions (xLength, yLength)
	node.AddStatement(rep.Position, position)

	node.AddStatement(rep.XLength, sst.Double(rect.XLength))

	node.AddStatement(rep.YLength, sst.Double(rect.YLength))
	return node, nil
}

func createNodesForCircles(graph sst.NamedGraph, circles []Circle, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, circle := range circles {
		combinedTransform := CombineTransforms(parentTransform, circle.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, circle.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(circle, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}
		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForCircle(graph sst.NamedGraph, circle Circle) (sst.IBNode, error) {
	position := createAxis2Placement2D(graph, circle.CX, circle.CY, circle.GetDirection())

	// Create Circle node
	node := graph.CreateIRINode(uuid.New().String())
	node.AddStatement(rdf.Type, rep.Circle)
	node.AddStatement(rep.Position, position)
	node.AddStatement(rep.Radius, sst.Double(circle.Radius))
	return node, nil
}

func createNodesForEllipses(graph sst.NamedGraph, ellipses []Ellipse, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, ellipse := range ellipses {
		combinedTransform := CombineTransforms(parentTransform, ellipse.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, ellipse.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(ellipse, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}
		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForEllipse(graph sst.NamedGraph, ellipse Ellipse) (sst.IBNode, error) {
	position := createAxis2Placement2D(graph, ellipse.CX, ellipse.CY, ellipse.GetDirection())

	// Create Ellipse node
	node := graph.CreateIRINode(uuid.New().String())
	node.AddStatement(rdf.Type, rep.Ellipse)
	node.AddStatement(rep.Position, position)
	node.AddStatement(rep.SemiAxis1, sst.Double(ellipse.RX))
	node.AddStatement(rep.SemiAxis2, sst.Double(ellipse.RY))
	return node, nil
}

func createNodesForPolygons(graph sst.NamedGraph, polygons []Polygon, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, polygon := range polygons {
		combinedTransform := CombineTransforms(parentTransform, polygon.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, polygon.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(polygon, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}
		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForPolygon(graph sst.NamedGraph, polygon Polygon) (sst.IBNode, error) {
	node := graph.CreateIRINode(uuid.New().String())
	node.AddStatement(rdf.Type, rep.Polygon)
	points, err := parsePoints(string(polygon.Points))
	if err != nil {
		log.Panic(err)
	}
	cartesianPoints, err := createCartesianPoints(graph, points...)
	if err != nil {
		log.Panic(err)
	}
	objectPoints := make([]sst.Term, len(cartesianPoints))
	for i, point := range cartesianPoints {
		objectPoints[i] = point
	}
	collection := graph.CreateCollection(objectPoints...)
	node.AddStatement(rep.Points, collection)
	return node, nil
}

func createNodesForPolylines(graph sst.NamedGraph, polylines []Polyline, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, polyline := range polylines {
		combinedTransform := CombineTransforms(parentTransform, polyline.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, polyline.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(polyline, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}
		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForPolyline(graph sst.NamedGraph, polyline Polyline) (sst.IBNode, error) {
	node := graph.CreateIRINode(uuid.New().String())
	node.AddStatement(rdf.Type, rep.Polyline)
	points, err := parsePoints(string(polyline.Points))
	if err != nil {
		log.Panic(err)
	}
	cartesianPoints, err := createCartesianPoints(graph, points...)
	if err != nil {
		log.Panic(err)
	}
	objectPoints := make([]sst.Term, len(cartesianPoints))
	for i, point := range cartesianPoints {
		objectPoints[i] = point
	}
	collection := graph.CreateCollection(objectPoints...)
	node.AddStatement(rep.Points, collection)
	return node, nil
}

func createNodesForLines(graph sst.NamedGraph, lines []Line, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, line := range lines {
		combinedTransform := CombineTransforms(parentTransform, line.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, line.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(line, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}
		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

func createNodeForLine(graph sst.NamedGraph, line Line) (sst.IBNode, error) {
	node := graph.CreateIRINode(uuid.New().String())
	node.AddStatement(rdf.Type, rep.Polyline)

	points := []Point{
		{X: line.X1, Y: line.Y1},
		{X: line.X2, Y: line.Y2},
	}
	cartesianPoints, err := createCartesianPoints(graph, points...)
	if err != nil {
		log.Panic(err)
	}
	objectPoints := make([]sst.Term, len(cartesianPoints))
	for i, point := range cartesianPoints {
		objectPoints[i] = point
	}
	collection := graph.CreateCollection(objectPoints...)
	if err != nil {
		log.Panic(err)
	}

	node.AddStatement(rep.Points, collection)

	return node, nil
}

func createNodesForPaths(graph sst.NamedGraph, paths []Path, itemUUIDs *[]sst.Term, parentTransform string, parentStyle ShapeStyle) {
	for _, path := range paths {
		combinedTransform := CombineTransforms(parentTransform, path.Transform)

		combinedStyle := CombineShapeStyles(parentStyle, path.ShapeStyle)

		// Check if the combined style is white. If so, skip this iteration.
		if combinedStyle.Fill == "#ffffff" || combinedStyle.Fill == "#FFFFFF" {
			continue
		}

		shape := TransformShape(path, combinedTransform)
		node, err := createNodeForShape(graph, shape)
		if err != nil {
			log.Panic(err)
		}

		styledNode, err := createStyledItemForShape(graph, shape, node, combinedStyle)
		if err != nil {
			log.Panic(err)
		}
		*itemUUIDs = append(*itemUUIDs, styledNode)
	}
}

// createNodeForPath processes a single path and creates a node for it.
func createNodeForPath(graph sst.NamedGraph, path Path) (sst.IBNode, error) {
	commands, err := ParsePathData(string(path.D))
	if err != nil {
		log.Panic(err)
	}

	// Determine if the path represents a single shape or a composite curve
	isSingle, shapeType := isSingleShape(commands)
	var node sst.IBNode

	if isSingle {
		fmt.Printf("Single shape detected: %s\n", shapeType)
		// Handle single shape and return the node
		node, err = handleSingleShape(graph, commands, shapeType)
	} else {
		fmt.Println("Composite curve detected")
		// Handle composite curve and return the node
		node, err = handleCompositeCurve(graph, commands)
	}

	if err != nil {
		log.Panic(err)
	}

	return node, nil
}

func handleSingleShape(graph sst.NamedGraph, commands []PathCommand, shapeType string) (sst.IBNode, error) {
	currentPoint := Point{0, 0}
	var points []Point
	var node sst.IBNode
	var err error

	for _, command := range commands {
		switch command.Command {
		case "M":
			currentPoint.X = command.Params[0]
			currentPoint.Y = command.Params[1]
			points = append(points, currentPoint)
		case "m":
			currentPoint.X += command.Params[0]
			currentPoint.Y += command.Params[1]
			points = append(points, currentPoint)
		case "L":
			currentPoint.X = command.Params[0]
			currentPoint.Y = command.Params[1]
			points = append(points, currentPoint)
		case "l":
			currentPoint.X += command.Params[0]
			currentPoint.Y += command.Params[1]
			points = append(points, currentPoint)
		case "H":
			currentPoint.X = command.Params[0]
			points = append(points, currentPoint)
		case "h":
			currentPoint.X += command.Params[0]
			points = append(points, currentPoint)
		case "V":
			currentPoint.Y = command.Params[0]
			points = append(points, currentPoint)
		case "v":
			currentPoint.Y += command.Params[0]
			points = append(points, currentPoint)
		case "Z", "z":
			if len(points) > 2 {
				polygon := Polygon{Points: pointsToString(points)}
				node, err = createNodeForPolygon(graph, polygon)
				if err != nil {
					return nil, err
				}
			}
		case "A":
			arc := EllipticalArc{
				RX:            command.Params[0],
				RY:            command.Params[1],
				StartX:        currentPoint.X,
				StartY:        currentPoint.Y,
				EndX:          command.Params[5],
				EndY:          command.Params[6],
				XAxisRotation: command.Params[2],
				LargeArcFlag:  command.Params[3] != 0,
				SweepFlag:     command.Params[4] != 0,
			}
			node, err = createNodesForTrimmedCurve(graph, arc)
			if err != nil {
				return nil, err
			}
			currentPoint.X = command.Params[5]
			currentPoint.Y = command.Params[6]
		case "a":
			arc := EllipticalArc{
				RX:            command.Params[0],
				RY:            command.Params[1],
				StartX:        currentPoint.X,
				StartY:        currentPoint.Y,
				EndX:          currentPoint.X + command.Params[5],
				EndY:          currentPoint.Y + command.Params[6],
				XAxisRotation: command.Params[2],
				LargeArcFlag:  command.Params[3] != 0,
				SweepFlag:     command.Params[4] != 0,
			}
			node, err = createNodesForTrimmedCurve(graph, arc)
			if err != nil {
				return nil, err
			}
			currentPoint.X += command.Params[5]
			currentPoint.Y += command.Params[6]
		case "C":
			bezier := CubicBezier{
				StartX:    currentPoint.X,
				StartY:    currentPoint.Y,
				ControlX1: command.Params[0],
				ControlY1: command.Params[1],
				ControlX2: command.Params[2],
				ControlY2: command.Params[3],
				EndX:      command.Params[4],
				EndY:      command.Params[5],
			}
			node, err = createNodeForCubicBezier(graph, bezier)
			if err != nil {
				return nil, err
			}
			currentPoint.X = command.Params[4]
			currentPoint.Y = command.Params[5]
		case "c":
			bezier := CubicBezier{
				StartX:    currentPoint.X,
				StartY:    currentPoint.Y,
				ControlX1: currentPoint.X + command.Params[0],
				ControlY1: currentPoint.Y + command.Params[1],
				ControlX2: currentPoint.X + command.Params[2],
				ControlY2: currentPoint.Y + command.Params[3],
				EndX:      currentPoint.X + command.Params[4],
				EndY:      currentPoint.Y + command.Params[5],
			}
			node, err = createNodeForCubicBezier(graph, bezier)
			if err != nil {
				return nil, err
			}
			currentPoint.X += command.Params[4]
			currentPoint.Y += command.Params[5]
		case "Q":
			bezier := QuadraticBezier{
				StartX:   currentPoint.X,
				StartY:   currentPoint.Y,
				ControlX: command.Params[0],
				ControlY: command.Params[1],
				EndX:     command.Params[2],
				EndY:     command.Params[3],
			}
			node, err = createNodeForQuadraticBezier(graph, bezier)
			if err != nil {
				return nil, err
			}
			currentPoint.X = command.Params[2]
			currentPoint.Y = command.Params[3]
		case "q":
			bezier := QuadraticBezier{
				StartX:   currentPoint.X,
				StartY:   currentPoint.Y,
				ControlX: currentPoint.X + command.Params[0],
				ControlY: currentPoint.Y + command.Params[1],
				EndX:     currentPoint.X + command.Params[2],
				EndY:     currentPoint.Y + command.Params[3],
			}
			node, err = createNodeForQuadraticBezier(graph, bezier)
			if err != nil {
				return nil, err
			}
			currentPoint.X += command.Params[2]
			currentPoint.Y += command.Params[3]
		}
	}

	if shapeType == "polyline" && len(points) > 1 {
		polyline := Polyline{Points: pointsToString(points)}
		node, err = createNodeForPolyline(graph, polyline)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func handleCompositeCurve(graph sst.NamedGraph, commands []PathCommand) (sst.IBNode, error) {
	compositeCurveNode := graph.CreateIRINode(uuid.New().String())

	compositeCurveNode.AddStatement(rdf.Type, rep.CompositeCurve)

	var segmentNodes []sst.Term
	initialPoint := Point{0, 0}
	currentPoint := Point{0, 0}
	lastControlPoint := Point{0, 0}
	var points []Point
	var previousCommand string // Track the previous command
	var lastSegmentNode sst.Term

	for i, command := range commands {
		fmt.Printf("i: %v Command: %s, Points: %v\n", i, command.Command, command.Params)

		switch command.Command {
		case "M", "m":
			if len(points) > 1 {
				segmentNode, err := createNodesForPolyOrLine(graph, points)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
			}
			if lastSegmentNode != nil {
				compositeCurveSegmentNode := createCompositeCurveSegment(graph, lastSegmentNode, false)
				segmentNodes = append(segmentNodes, compositeCurveSegmentNode)
				lastSegmentNode = nil
			}
			points = []Point{}
			if command.Command == "M" {
				currentPoint.X = command.Params[0]
				currentPoint.Y = command.Params[1]
			} else {
				currentPoint.X += command.Params[0]
				currentPoint.Y += command.Params[1]
			}
			initialPoint = currentPoint
			points = append(points, currentPoint)
		case "L", "l", "H", "h", "V", "v":
			switch command.Command {
			case "L":
				currentPoint.X = command.Params[0]
				currentPoint.Y = command.Params[1]
			case "l":
				currentPoint.X += command.Params[0]
				currentPoint.Y += command.Params[1]
			case "H":
				currentPoint.X = command.Params[0]
			case "h":
				currentPoint.X += command.Params[0]
			case "V":
				currentPoint.Y = command.Params[0]
			case "v":
				currentPoint.Y += command.Params[0]
			}
			points = append(points, currentPoint)
		case "A", "a":
			if len(points) > 1 {
				segmentNode, err := createNodesForPolyOrLine(graph, points)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
			}
			if lastSegmentNode != nil {
				compositeCurveSegmentNode := createCompositeCurveSegment(graph, lastSegmentNode, true)
				segmentNodes = append(segmentNodes, compositeCurveSegmentNode)
				lastSegmentNode = nil
			}
			var arc EllipticalArc
			if command.Command == "A" {
				arc = EllipticalArc{
					RX:            command.Params[0],
					RY:            command.Params[1],
					StartX:        currentPoint.X,
					StartY:        currentPoint.Y,
					EndX:          command.Params[5],
					EndY:          command.Params[6],
					XAxisRotation: command.Params[2],
					LargeArcFlag:  command.Params[3] != 0,
					SweepFlag:     command.Params[4] != 0,
				}
				currentPoint.X = command.Params[5]
				currentPoint.Y = command.Params[6]
			} else { // command.Command == "a"
				arc = EllipticalArc{
					RX:            command.Params[0],
					RY:            command.Params[1],
					StartX:        currentPoint.X,
					StartY:        currentPoint.Y,
					EndX:          currentPoint.X + command.Params[5],
					EndY:          currentPoint.Y + command.Params[6],
					XAxisRotation: command.Params[2],
					LargeArcFlag:  command.Params[3] != 0,
					SweepFlag:     command.Params[4] != 0,
				}
				currentPoint.X += command.Params[5]
				currentPoint.Y += command.Params[6]
			}

			points = []Point{}
			points = append(points, currentPoint)

			segmentNode, err := createNodesForTrimmedCurve(graph, arc)
			if err != nil {
				log.Panic(err)
			}
			lastSegmentNode = segmentNode
			// isContinuous := !isLastCommand
			// compositeCurveSegmentNode := createCompositeCurveSegment(graph, segmentNode, isContinuous)
			// segmentNodes = append(segmentNodes, compositeCurveSegmentNode)

		case "C", "c", "S", "s", "Q", "q", "T", "t":
			if len(points) > 1 {
				segmentNode, err := createNodesForPolyOrLine(graph, points)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
			}
			if lastSegmentNode != nil {
				compositeCurveSegmentNode := createCompositeCurveSegment(graph, lastSegmentNode, true)
				segmentNodes = append(segmentNodes, compositeCurveSegmentNode)
				lastSegmentNode = nil
			}
			points = []Point{}

			var segmentNode sst.Term
			var err error

			switch command.Command {
			case "C", "c":
				var bezier CubicBezier
				if command.Command == "C" {
					bezier = CubicBezier{
						StartX:    currentPoint.X,
						StartY:    currentPoint.Y,
						ControlX1: command.Params[0],
						ControlY1: command.Params[1],
						ControlX2: command.Params[2],
						ControlY2: command.Params[3],
						EndX:      command.Params[4],
						EndY:      command.Params[5],
					}
					lastControlPoint.X = command.Params[2]
					lastControlPoint.Y = command.Params[3]
					currentPoint.X = command.Params[4]
					currentPoint.Y = command.Params[5]
				} else {
					bezier = CubicBezier{
						StartX:    currentPoint.X,
						StartY:    currentPoint.Y,
						ControlX1: currentPoint.X + command.Params[0],
						ControlY1: currentPoint.Y + command.Params[1],
						ControlX2: currentPoint.X + command.Params[2],
						ControlY2: currentPoint.Y + command.Params[3],
						EndX:      currentPoint.X + command.Params[4],
						EndY:      currentPoint.Y + command.Params[5],
					}
					lastControlPoint.X = currentPoint.X + command.Params[2]
					lastControlPoint.Y = currentPoint.Y + command.Params[3]
					currentPoint.X += command.Params[4]
					currentPoint.Y += command.Params[5]
				}

				segmentNode, err = createNodeForCubicBezier(graph, bezier)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
				// isContinuous := !isLastCommand
				// compositeCurveSegmentNode := createCompositeCurveSegment(graph, segmentNode, isContinuous)
				// segmentNodes = append(segmentNodes, compositeCurveSegmentNode)

				points = append(points, currentPoint)

			case "S", "s":
				var controlX1, controlY1 float64
				if lastCommandIsCurve(previousCommand) {
					controlX1 = 2*currentPoint.X - lastControlPoint.X
					controlY1 = 2*currentPoint.Y - lastControlPoint.Y
				} else {
					controlX1 = currentPoint.X
					controlY1 = currentPoint.Y
				}
				var bezier CubicBezier
				if command.Command == "S" {
					bezier = CubicBezier{
						StartX:    currentPoint.X,
						StartY:    currentPoint.Y,
						ControlX1: controlX1,
						ControlY1: controlY1,
						ControlX2: command.Params[2],
						ControlY2: command.Params[3],
						EndX:      command.Params[4],
						EndY:      command.Params[5],
					}
					lastControlPoint.X = command.Params[0]
					lastControlPoint.Y = command.Params[1]
					currentPoint.X = command.Params[2]
					currentPoint.Y = command.Params[3]
				} else {
					bezier = CubicBezier{
						StartX:    currentPoint.X,
						StartY:    currentPoint.Y,
						ControlX1: controlX1,
						ControlY1: controlY1,
						ControlX2: currentPoint.X + command.Params[2],
						ControlY2: currentPoint.Y + command.Params[3],
						EndX:      currentPoint.X + command.Params[4],
						EndY:      currentPoint.Y + command.Params[5],
					}
					lastControlPoint.X = currentPoint.X + command.Params[0]
					lastControlPoint.Y = currentPoint.Y + command.Params[1]
					currentPoint.X += command.Params[2]
					currentPoint.Y += command.Params[3]
				}
				segmentNode, err = createNodeForCubicBezier(graph, bezier)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
				// isContinuous := !isLastCommand
				// compositeCurveSegmentNode := createCompositeCurveSegment(graph, segmentNode, isContinuous)
				// segmentNodes = append(segmentNodes, compositeCurveSegmentNode)

				points = append(points, currentPoint)

			case "Q", "q":
				// Quadratic Bezier curve
				var bezier QuadraticBezier
				if command.Command == "Q" {
					bezier = QuadraticBezier{
						StartX:   currentPoint.X,
						StartY:   currentPoint.Y,
						ControlX: command.Params[0],
						ControlY: command.Params[1],
						EndX:     command.Params[2],
						EndY:     command.Params[3],
					}
					lastControlPoint.X = command.Params[0]
					lastControlPoint.Y = command.Params[1]
					currentPoint.X = command.Params[2]
					currentPoint.Y = command.Params[3]
				} else {
					bezier = QuadraticBezier{
						StartX:   currentPoint.X,
						StartY:   currentPoint.Y,
						ControlX: currentPoint.X + command.Params[0],
						ControlY: currentPoint.Y + command.Params[1],
						EndX:     currentPoint.X + command.Params[2],
						EndY:     currentPoint.Y + command.Params[3],
					}
					lastControlPoint.X = currentPoint.X + command.Params[0]
					lastControlPoint.Y = currentPoint.Y + command.Params[1]
					currentPoint.X += command.Params[2]
					currentPoint.Y += command.Params[3]
				}

				segmentNode, err = createNodeForQuadraticBezier(graph, bezier)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
				// isContinuous := !isLastCommand
				// compositeCurveSegmentNode := createCompositeCurveSegment(graph, segmentNode, isContinuous)
				// segmentNodes = append(segmentNodes, compositeCurveSegmentNode)

				points = append(points, currentPoint)

			case "T", "t":
				// Smooth Quadratic Bezier curve
				var controlX, controlY float64
				if lastCommandIsCurve(previousCommand) {
					controlX = 2*currentPoint.X - lastControlPoint.X
					controlY = 2*currentPoint.Y - lastControlPoint.Y
				} else {
					controlX = currentPoint.X
					controlY = currentPoint.Y
				}

				if command.Command == "T" {
					bezier := QuadraticBezier{
						StartX:   currentPoint.X,
						StartY:   currentPoint.Y,
						ControlX: controlX,
						ControlY: controlY,
						EndX:     command.Params[0],
						EndY:     command.Params[1],
					}
					segmentNode, err = createNodeForQuadraticBezier(graph, bezier)
					if err != nil {
						log.Panic(err)
					}
					lastControlPoint.X = controlX
					lastControlPoint.Y = controlY
					currentPoint.X = command.Params[0]
					currentPoint.Y = command.Params[1]
				} else {
					bezier := QuadraticBezier{
						StartX:   currentPoint.X,
						StartY:   currentPoint.Y,
						ControlX: controlX,
						ControlY: controlY,
						EndX:     currentPoint.X + command.Params[0],
						EndY:     currentPoint.Y + command.Params[1],
					}
					segmentNode, err = createNodeForQuadraticBezier(graph, bezier)
					if err != nil {
						log.Panic(err)
					}
					lastControlPoint.X = controlX
					lastControlPoint.Y = controlY
					currentPoint.X += command.Params[0]
					currentPoint.Y += command.Params[1]
				}
				lastSegmentNode = segmentNode
				// isContinuous := !isLastCommand
				// compositeCurveSegmentNode := createCompositeCurveSegment(graph, segmentNode, isContinuous)
				// segmentNodes = append(segmentNodes, compositeCurveSegmentNode)

				points = append(points, currentPoint)
			}

		case "Z", "z":
			points = append(points, initialPoint)
			if len(points) > 1 {
				segmentNode, err := createNodesForPolyOrLine(graph, points)
				if err != nil {
					log.Panic(err)
				}
				lastSegmentNode = segmentNode
			}
			if lastSegmentNode != nil {
				compositeCurveSegmentNode := createCompositeCurveSegment(graph, lastSegmentNode, false)
				segmentNodes = append(segmentNodes, compositeCurveSegmentNode)
				lastSegmentNode = nil
			}
			points = []Point{}
		}

		previousCommand = command.Command // Update previous command
		fmt.Printf("Current Point: %v\n", currentPoint)
	}

	if len(points) > 1 {
		segmentNode, err := createNodesForPolyOrLine(graph, points)
		if err != nil {
			log.Panic(err)
		}

		lastSegmentNode = segmentNode
	}
	if lastSegmentNode != nil {
		compositeCurveSegmentNode := createCompositeCurveSegment(graph, lastSegmentNode, false)
		segmentNodes = append(segmentNodes, compositeCurveSegmentNode)
		lastSegmentNode = nil
	}

	collection := graph.CreateCollection(segmentNodes...)

	compositeCurveNode.AddStatement(rep.Segments, collection)

	fmt.Println()
	return compositeCurveNode, nil
}

func createCompositeCurveSegment(graph sst.NamedGraph, segmentNode sst.Term, isContinuous bool) sst.Term {
	compositeCurveSegmentNode := graph.CreateIRINode(uuid.New().String())

	compositeCurveSegmentNode.AddStatement(rdf.Type, rep.CompositeCurveSegment)

	compositeCurveSegmentNode.AddStatement(rep.ParentCurve, segmentNode)

	if isContinuous {
		compositeCurveSegmentNode.AddStatement(rep.Transition, rep.TransitionCode_Continuous)
	} else {
		compositeCurveSegmentNode.AddStatement(rep.Transition, rep.TransitionCode_Discontinuous)
	}

	compositeCurveSegmentNode.AddStatement(rep.SameSense, sst.Boolean(true))

	return compositeCurveSegmentNode
}

func lastCommandIsCurve(command string) bool {
	return command == "C" || command == "c" || command == "S" || command == "s" || command == "Q" || command == "q" || command == "T" || command == "t"
}

func createNodesForPolyOrLine(graph sst.NamedGraph, points []Point) (sst.IBNode, error) {
	var node sst.IBNode
	var err error

	if len(points) == 2 {
		line := Line{
			X1: points[0].X,
			Y1: points[0].Y,
			X2: points[1].X,
			Y2: points[1].Y,
		}
		node, err = createNodeForLine(graph, line)
		if err != nil {
			return nil, err
		}
	} else if len(points) > 2 {
		polyline := Polyline{Points: pointsToString(points)}
		node, err = createNodeForPolyline(graph, polyline)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

func isSingleShape(commands []PathCommand) (bool, string) {
	if len(commands) == 0 {
		return false, "empty"
	}

	var hasLine, hasPoly, hasCubic, hasQuadratic, hasArc bool
	var shapeType string

	for i, command := range commands {
		switch command.Command {
		case "M", "m":
			// M (move) is fine as long as it's the first command
			if i > 0 {
				return false, "compositeCurve"
			}
		case "L", "l", "H", "h", "V", "v":
			if hasCubic || hasQuadratic || hasArc || hasPoly {
				return false, "compositeCurve"
			}
			hasLine = true
		case "Z", "z":
			// Z can close a path, should work with polylines or polygons
			if !hasLine && !hasPoly {
				return false, "compositeCurve"
			}
			hasPoly = true
		case "A", "a":
			if hasCubic || hasQuadratic || hasArc || hasLine || hasPoly {
				return false, "compositeCurve"
			}
			hasArc = true
		case "C", "c", "S", "s":
			if hasCubic || hasQuadratic || hasArc || hasLine || hasPoly {
				return false, "compositeCurve"
			}
			hasCubic = true
		case "Q", "q", "T", "t":
			if hasCubic || hasQuadratic || hasArc || hasLine || hasPoly {
				return false, "compositeCurve"
			}
			hasQuadratic = true
		default:
			// Any unrecognized command results in a composite curve
			return false, "compositeCurve"
		}
	}

	// After the loop, determine the shape type
	if hasCubic {
		shapeType = "cubicBezierCurve"
	} else if hasQuadratic {
		shapeType = "quadraticBezierCurve"
	} else if hasArc {
		shapeType = "trimmedCurve"
	} else if hasPoly {
		shapeType = "polygon"
	} else if hasLine {
		shapeType = "polyline"
	}

	return true, shapeType
}

func createNodesForTrimmedCurve(graph sst.NamedGraph, arc EllipticalArc) (sst.IBNode, error) {
	cx, cy := GetArcCenterPoint(arc)
	node := graph.CreateIRINode(uuid.New().String())

	node.AddStatement(rdf.Type, rep.TrimmedCurve)

	if arc.RX == arc.RY {
		circle := Circle{
			CX:        cx,
			CY:        cy,
			Radius:    arc.RX,
			Direction: float64(arc.XAxisRotation),
		}
		log.Printf("Detected Circle: CenterX=%f, CenterY=%f, Radius=%f, Direction=%f", circle.CX, circle.CY, circle.Radius, circle.Direction)

		basisCurve, err := createNodeForCircle(graph, circle)
		if err != nil {
			log.Panic(err)
		}
		node.AddStatement(rep.BasisCurve, basisCurve)
	} else {
		ellipse := Ellipse{
			CX:        cx,
			CY:        cy,
			RX:        arc.RX,
			RY:        arc.RY,
			Direction: float64(arc.XAxisRotation),
		}
		log.Printf("Detected Ellipse: CenterX=%f, CenterY=%f, RX=%f, RY=%f, Direction=%f", ellipse.CX, ellipse.CY, ellipse.RX, ellipse.RY, ellipse.Direction)

		basisCurve, err := createNodeForEllipse(graph, ellipse)
		if err != nil {
			log.Panic(err)
		}
		node.AddStatement(rep.BasisCurve, basisCurve)
	}

	trim1 := createCartesianPoint(graph, arc.StartX, arc.StartY)
	trim2 := createCartesianPoint(graph, arc.EndX, arc.EndY)
	node.AddStatement(rep.Trim1, trim1)
	node.AddStatement(rep.Trim2, trim2)

	startAngle := math.Atan2(float64(arc.StartY)-cy, float64(arc.StartX)-cx)
	endAngle := math.Atan2(float64(arc.EndY)-cy, float64(arc.EndX)-cx)
	log.Printf("Debug: startAngle=%f, endAngle=%f", startAngle, endAngle)

	deltaAngle := endAngle - startAngle
	log.Printf("Debug: Initial deltaAngle (before SweepFlag adjustment)=%f", deltaAngle)

	if arc.SweepFlag {
		if deltaAngle < 0 {
			deltaAngle += 2 * math.Pi
		}
		log.Printf("Debug: Adjusted deltaAngle for SweepFlag (Clockwise)=%f", deltaAngle)
	} else {
		if deltaAngle > 0 {
			deltaAngle -= 2 * math.Pi
		}
		log.Printf("Debug: Adjusted deltaAngle for SweepFlag (Counter-Clockwise)=%f", deltaAngle)
	}

	if arc.LargeArcFlag && math.Abs(deltaAngle) < math.Pi {
		if deltaAngle > 0 {
			deltaAngle -= 2 * math.Pi
		} else {
			deltaAngle += 2 * math.Pi
		}
		log.Printf("Debug: Adjusted deltaAngle for LargeArcFlag=%f", deltaAngle)
	}

	senseAgreement := deltaAngle <= 0
	log.Printf("Debug: Final senseAgreement=%v (Clockwise=%v)", senseAgreement, arc.SweepFlag)

	node.AddStatement(rep.SenseAgreement, sst.Boolean(senseAgreement))

	return node, nil
}

func GetArcCenterPoint(arc EllipticalArc) (float64, float64) {
	x1, y1 := float64(arc.StartX), float64(arc.StartY)
	x2, y2 := float64(arc.EndX), float64(arc.EndY)
	rx, ry := float64(arc.RX), float64(arc.RY)
	phi := float64(arc.XAxisRotation) * math.Pi / 180.0
	fA := bool(arc.LargeArcFlag)
	fs := bool(arc.SweepFlag)

	log.Printf("StartX: %f, StartY: %f\n", x1, y1)
	log.Printf("EndX: %f, EndY: %f\n", x2, y2)
	log.Printf("RX: %f, RY: %f\n", rx, ry)
	log.Printf("XAxisRotation: %f\n", phi)
	log.Printf("LargeArcFlag: %t, SweepFlag: %t\n", fA, fs)

	// Step 1: Calculate x1', y1'
	x1_ := math.Cos(phi)*(x1-x2)/2 + math.Sin(phi)*(y1-y2)/2
	y1_ := -math.Sin(phi)*(x1-x2)/2 + math.Cos(phi)*(y1-y2)/2

	// Step 2: Ensure radii are valid
	radiusCheck := math.Pow(x1_/rx, 2) + math.Pow(y1_/ry, 2)
	if radiusCheck > 1 {
		scale := math.Sqrt(radiusCheck)
		rx *= scale
		ry *= scale
	}

	// Step 3: Calculate center offset
	a := math.Pow(rx, 2)*math.Pow(ry, 2) - math.Pow(rx, 2)*math.Pow(y1_, 2) - math.Pow(ry, 2)*math.Pow(x1_, 2)
	if a < 0 {
		a = 0 // Avoid negative sqrt
	}
	b := math.Pow(rx, 2)*math.Pow(y1_, 2) + math.Pow(ry, 2)*math.Pow(x1_, 2)
	c := math.Sqrt(a / b)
	if fA == fs {
		c = -c
	}

	// Step 4: Compute cx', cy' in local space
	cx_ := c * (rx * y1_ / ry)
	cy_ := c * (-ry * x1_ / rx)

	// Step 5: Transform back to global space
	cx := math.Cos(phi)*cx_ - math.Sin(phi)*cy_ + (x1+x2)/2
	cy := math.Sin(phi)*cx_ + math.Cos(phi)*cy_ + (y1+y2)/2

	return cx, cy
}

func createNodeForQuadraticBezier(graph sst.NamedGraph, bezier QuadraticBezier) (sst.IBNode, error) {
	// Create start point node
	startPointNode := createCartesianPoint(graph, bezier.StartX, bezier.StartY)

	// Create end point node
	endPointNode := createCartesianPoint(graph, bezier.EndX, bezier.EndY)

	// Create control point node
	controlPointNode := createCartesianPoint(graph, bezier.ControlX, bezier.ControlY)

	// Create cubic Bézier curve node
	bezierNodeUUID := uuid.New().String()
	bezierNode := graph.CreateIRINode(bezierNodeUUID)

	bezierNode.AddStatement(rdf.Type, rep.BezierCurve)
	collection := graph.CreateCollection(startPointNode, controlPointNode, endPointNode)
	bezierNode.AddStatement(rep.Degree, sst.Integer(2))
	bezierNode.AddStatement(rep.ControlPointsList, collection)

	return bezierNode, nil
}

func createNodeForCubicBezier(graph sst.NamedGraph, bezier CubicBezier) (sst.IBNode, error) {

	// Create start point node
	startPointNode := createCartesianPoint(graph, bezier.StartX, bezier.StartY)
	if startPointNode == nil {
		return nil, fmt.Errorf("failed to create start point node")
	}

	// Create end point node
	endPointNode := createCartesianPoint(graph, bezier.EndX, bezier.EndY)
	if endPointNode == nil {
		return nil, fmt.Errorf("failed to create end point node")
	}

	// Create control point 1 node
	controlPoint1Node := createCartesianPoint(graph, bezier.ControlX1, bezier.ControlY1)
	if controlPoint1Node == nil {
		return nil, fmt.Errorf("failed to create control point 1 node")
	}

	// Create control point 2 node
	controlPoint2Node := createCartesianPoint(graph, bezier.ControlX2, bezier.ControlY2)
	if controlPoint2Node == nil {
		return nil, fmt.Errorf("failed to create control point 2 node")
	}

	// Create cubic Bézier curve node
	bezierNodeUUID := uuid.New().String()
	bezierNode := graph.CreateIRINode(bezierNodeUUID)

	bezierNode.AddStatement(rdf.Type, rep.BezierCurve)

	collection := graph.CreateCollection(startPointNode, controlPoint1Node, controlPoint2Node, endPointNode)

	bezierNode.AddStatement(rep.Degree, sst.Integer(3))
	bezierNode.AddStatement(rep.ControlPointsList, collection)

	return bezierNode, nil
}

func createStyledItemForShape(graph sst.NamedGraph, shape StyledShape, shapeNode sst.IBNode, combinedStyle ShapeStyle) (sst.IBNode, error) {
	var style ShapeStyle

	if combinedStyle != (ShapeStyle{}) {
		style = combinedStyle
	} else {
		style = shape.GetStyle()
	}

	if style.Stroke == "" && style.StrokeWidth == "" && style.Fill == "" {
		return shapeNode, nil
	}

	styledItemNode := graph.CreateIRINode(uuid.New().String())

	styledItemNode.AddStatement(rdf.Type, rep.StyledItem)

	var styleNodes []sst.Term

	// stroke & stroke-width
	if style.Stroke != "" || style.StrokeWidth != "" {
		curveStyleNode, err := createCurveStyle(graph, style.Stroke, style.StrokeWidth)
		if err != nil {
			return nil, err
		}
		if curveStyleNode != nil {
			styleNodes = append(styleNodes, curveStyleNode)
		}
	}

	// fill
	if style.Fill != "" {
		fillStyleNode, err := createFillAreaStyle(graph, style.Fill)
		if err != nil {
			return nil, err
		}
		if fillStyleNode != nil {
			styleNodes = append(styleNodes, fillStyleNode)
		}
	}

	for _, styleNode := range styleNodes {
		styledItemNode.AddStatement(rep.Style, styleNode)
	}

	// itemToStyle
	styledItemNode.AddStatement(rep.ItemToStyle, shapeNode)

	return styledItemNode, nil
}

func createCurveStyle(graph sst.NamedGraph, color string, width string) (sst.IBNode, error) {
	curveStyleNode := graph.CreateIRINode(uuid.New().String())

	curveStyleNode.AddStatement(rdf.Type, rep.CurveStyle)

	if width != "" {
		strokeWidth, _ := strconv.ParseFloat(width, 64)
		curveStyleNode.AddStatement(rep.CurveWidth, sst.Double(strokeWidth))
	}

	if color != "" {
		curveStyleNode.AddStatement(rep.CurveColour, sst.String("color:"+color))
	}

	return curveStyleNode, nil
}

func createFillAreaStyle(graph sst.NamedGraph, color string) (sst.IBNode, error) {
	if color == "" {
		return nil, nil
	}

	fillStyleNode := graph.CreateIRINode(uuid.New().String())

	fillStyleNode.AddStatement(rdf.Type, rep.FillAreaStyle)

	fillStyleNode.AddStatement(rep.FillColour, sst.String("color:"+color))

	return fillStyleNode, nil
}

func createGeometricRepresentationContext(graph sst.NamedGraph) (sst.IBNode, error) {
	contextNode := graph.CreateIRINode(uuid.New().String())
	contextNode.AddStatement(rdf.Type, rep.GeometricRepresentationContext)
	contextNode.AddStatement(rep.CoordinateSpaceDimension, sst.Integer(2))
	return contextNode, nil
}

func createSymbolRepresentation(graph sst.NamedGraph, contextNode sst.IBNode, itemUUIDs []sst.Term, label string, comment string) (sst.IBNode, error) {
	symbolNode := graph.CreateIRINode(uuid.New().String())
	symbolNode.AddStatement(rdf.Type, rep.SymbolRepresentation)
	if label != "" {
		symbolNode.AddStatement(rdfs.Label, sst.String(label))
	}
	if comment != "" {
		symbolNode.AddStatement(rdfs.Comment, sst.String(comment))
	}
	symbolNode.AddStatement(rep.ContextOfItems, contextNode)

	for _, itemUUID := range itemUUIDs {
		symbolNode.AddStatement(rep.Item, itemUUID)
	}
	return symbolNode, nil
}

func createAxis2Placement2D(graph sst.NamedGraph, x, y, directionDegree float64) sst.IBNode {
	axis2Placement2D := graph.CreateIRINode(uuid.New().String())
	axis2Placement2D.AddStatement(rdf.Type, rep.Axis2Placement2D)

	location := createCartesianPoint(graph, x, y)
	axis2Placement2D.AddStatement(rep.Location, location)

	if directionDegree != 0 {
		axis2Placement2D.AddStatement(rep.RefDirectionDegree, -sst.Double(directionDegree))
	}

	return axis2Placement2D
}

// Cache for Cartesian points, with a separate cache for each graph instance
var pointCache = make(map[sst.NamedGraph]map[string]sst.IBNode)

// generateCacheKey creates a unique key based on x and y coordinates
func generateCacheKey(x, y float64) string {
	return fmt.Sprintf("%f:%f", x, y)
}

func createCartesianPoint(graph sst.NamedGraph, x, y float64) sst.IBNode {
	// Initialize the cache for the graph if it does not already exist
	if pointCache[graph] == nil {
		pointCache[graph] = make(map[string]sst.IBNode)
	}

	// Generate a unique key based on the (x, y) coordinates
	cacheKey := generateCacheKey(x, y)

	// Check if the point already exists in the cache
	if existingPoint, found := pointCache[graph][cacheKey]; found {
		return existingPoint
	}

	// If not in the cache, create a new CartesianPoint
	cartesianPoint := graph.CreateIRINode(uuid.New().String())
	cartesianPoint.AddStatement(rdf.Type, rep.CartesianPoint)

	// Create the coordinate collection
	// coordinate := graph.CreateCollection(sst.Double(x), -sst.Double(y))
	coordinate := sst.NewLiteralCollection(sst.Double(x), -sst.Double(y))
	cartesianPoint.AddStatement(rep.Coordinates, coordinate)

	// Store the newly created point in the cache
	pointCache[graph][cacheKey] = cartesianPoint

	return cartesianPoint
}

func createCartesianPoints(graph sst.NamedGraph, points ...Point) ([]sst.IBNode, error) {
	var cartesianPoints []sst.IBNode
	for _, point := range points {
		cartesianPoint := createCartesianPoint(graph, point.X, point.Y)
		cartesianPoints = append(cartesianPoints, cartesianPoint)
	}
	return cartesianPoints, nil
}

// String -> []Point
func parsePoints(points string) ([]Point, error) {
	var result []Point
	pairs := strings.Fields(points)
	for _, pair := range pairs {
		coords := strings.Split(pair, ",")
		if len(coords) != 2 {
			return nil, fmt.Errorf("invalid point format: %s", pair)
		}
		x, err := strconv.ParseFloat(coords[0], 64)
		if err != nil {
			return nil, err
		}
		y, err := strconv.ParseFloat(coords[1], 64)
		if err != nil {
			return nil, err
		}
		result = append(result, Point{X: x, Y: y})
	}
	return result, nil
}

// string -> []PathCommand
func ParsePathData(data string) ([]PathCommand, error) {
	var commands []PathCommand

	re := regexp.MustCompile(`([MmLlHhVvCcSsQqTtAaZz])|([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)`)
	matches := re.FindAllString(data, -1)

	if matches == nil {
		return nil, fmt.Errorf("invalid path data")
	}

	var currentCommand string
	var params []float64

	for _, match := range matches {
		if len(match) == 1 && strings.ContainsAny(match, "MmLlHhVvCcSsQqTtAaZz") {
			if currentCommand != "" {
				commands = append(commands, PathCommand{Command: currentCommand, Params: params})
				params = nil
			}
			currentCommand = match
			if match == "Z" || match == "z" {
				commands = append(commands, PathCommand{Command: currentCommand, Params: nil})
				currentCommand = ""
			}
		} else {
			param, err := strconv.ParseFloat(match, 64)
			if err != nil {
				return nil, err
			}
			params = append(params, param)
		}
	}

	if currentCommand != "" && len(params) > 0 {
		commands = append(commands, PathCommand{Command: currentCommand, Params: params})
	}

	return commands, nil
}

func pointsToString(points []Point) string {
	var sb strings.Builder
	for _, p := range points {
		sb.WriteString(fmt.Sprintf("%f,%f ", p.X, p.Y))
	}
	return strings.TrimSpace(sb.String())
}

func ensureDirectoriesExist(directories ...string) error {
	for _, dir := range directories {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err := os.MkdirAll(dir, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

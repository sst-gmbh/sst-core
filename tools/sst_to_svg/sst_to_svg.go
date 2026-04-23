// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// This package converts SST 2D geometry into a coressponding SVG file.
package ssttosvg

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"git.semanticstep.net/x/sst/sst"
)

type Polyline struct {
	XMLName     xml.Name `xml:"polyline"`
	Points      string   `xml:"points,attr"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type Polygon struct {
	XMLName     xml.Name `xml:"polygon"`
	Points      string   `xml:"points,attr"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type Path struct {
	XMLName     xml.Name `xml:"path"`
	D           string   `xml:"d,attr"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type Circle struct {
	XMLName     xml.Name `xml:"circle"`
	Cx          string   `xml:"cx,attr,omitempty"`
	Cy          string   `xml:"cy,attr,omitempty"`
	R           string   `xml:"r,attr"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type Ellipse struct {
	XMLName     xml.Name `xml:"ellipse"`
	Cx          string   `xml:"cx,attr,omitempty"`
	Cy          string   `xml:"cy,attr,omitempty"`
	Rx          string   `xml:"rx,attr"`
	Ry          string   `xml:"ry,attr"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type Rectangle struct {
	XMLName     xml.Name `xml:"rect"`
	X           string   `xml:"x,attr,omitempty"`
	Y           string   `xml:"y,attr,omitempty"`
	Width       string   `xml:"width,attr"`
	Height      string   `xml:"height,attr"`
	Rx          string   `xml:"rx,attr,omitempty"`
	Ry          string   `xml:"ry,attr,omitempty"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type Text struct {
	XMLName     xml.Name `xml:"text"`
	X           string   `xml:"x,attr,omitempty"`
	Y           string   `xml:"y,attr,omitempty"`
	Text        string   `xml:",chardata"`
	FontFamily  string   `xml:"font-family,attr,omitempty"`
	FontSize    string   `xml:"font-size,attr,omitempty"`
	Fill        string   `xml:"fill,attr,omitempty"`
	Stroke      string   `xml:"stroke,attr,omitempty"`
	StrokeWidth string   `xml:"stroke-width,attr,omitempty"`
	Bold        string   `xml:"font-weight,attr,omitempty"`
	Underline   string   `xml:"text-decoration,attr,omitempty"`
	Transform   string   `xml:"transform,attr,omitempty"`
}

type xmlResult struct {
	XMLName xml.Name `xml:"http://www.w3.org/2000/svg svg"`
	Version string   `xml:"version,attr"`
	// Height    string   `xml:"height,attr"`
	// Width     string   `xml:"width,attr"`
	Polyline  []Polyline
	Polygon   []Polygon
	Path      []Path
	Circle    []Circle
	Rectangle []Rectangle
	Ellipse   []Ellipse
	Text      []Text
}

// ConvertGraphToSVG converts a graph representation into an SVG file.
// It processes the provided graph, extracts SVG elements, and writes the result to an SVG file
// in the specified output directory.
//
// Parameters:
// - graph: The graph to be converted, typically of type sst.NamedGraph.
//          This graph contains the data for SVG elements such as paths, circles, polygons, etc.
// - baseName: The base name for the output SVG file (e.g., "example_output").
//             If this parameter is empty, a default value like "output" is used.
// - outputDir: The directory where the output SVG file will be saved.
//              If this parameter is empty, the current working directory is used.
//
// Example:
//
//	graph := someFunctionToGenerateGraph() // Generate or load a graph (sst.NamedGraph)
//	err := ConvertGraphToSVG(graph, "example_output", "./svg_output")
//	if err != nil {
//	    log.Fatalf("Failed to create SVG: %v", err)
//	}
//
// This will create a file named "example_output.svg" in the "./svg_output" directory.

func ConvertGraphToSVG(graph sst.NamedGraph, baseName, outputDir string) {
	// Set default values for optional parameters
	if baseName == "" {
		baseName = "output"
	}
	if outputDir == "" {
		outputDir = ".svg" // Default .svg
	}

	// Ensure the output directory exists
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		err := os.Mkdir(outputDir, os.ModePerm)
		if err != nil {
			log.Panicf("Error creating output directory: %v", err)
		}
	}

	// Generate the output file path
	outputFileName := baseName + ".svg"
	outputFilePath := filepath.Join(outputDir, outputFileName)

	// Create the SVG file
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Panicf("Error creating SVG file: %v", err)
	}
	defer outputFile.Close()

	// Start writing SVG content
	writer := bufio.NewWriter(outputFile)

	list := getCurveAndIdListForSvg(graph)

	polylineList := []Polyline{}
	polygonList := []Polygon{}
	pathList := []Path{}
	circleList := []Circle{}
	rectList := []Rectangle{}
	ellipseList := []Ellipse{}
	textList := []Text{}

	for _, v := range list["Polyline"] {
		poly := Polyline{
			Points:      v["position"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			poly.Transform = fmt.Sprintf("rotate(%s)", rotation)
		}

		polylineList = append(polylineList, poly)
	}

	for _, v := range list["Polygon"] {
		poly := Polygon{
			Points:      v["position"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			poly.Transform = fmt.Sprintf("rotate(%s)", rotation)
		}

		polygonList = append(polygonList, poly)
	}

	for _, v := range list["TrimmedCurve"] {
		path := Path{
			D:           v["path_d"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			path.Transform = fmt.Sprintf("rotate(%s)", rotation)
		}

		pathList = append(pathList, path)
	}

	for _, v := range list["BezierCurve"] {
		path := Path{
			D:           v["path_d"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			path.Transform = fmt.Sprintf("rotate(%s)", rotation)
		}

		pathList = append(pathList, path)
	}

	for _, v := range list["Circle"] {
		circle := Circle{
			Cx:          v["cx"],
			Cy:          v["cy"],
			R:           v["r"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		var transforms []string

		if translate, ok := v["translate"]; ok && translate != "" {
			transforms = append(transforms, fmt.Sprintf("translate(%s)", translate))
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			transforms = append(transforms, fmt.Sprintf("rotate(%s)", rotation))
		}

		if len(transforms) > 0 {
			circle.Transform = strings.Join(transforms, " ")
		}

		circleList = append(circleList, circle)
	}

	for _, v := range list["Rectangle"] {
		rectangle := Rectangle{
			X:           v["x"],
			Y:           v["y"],
			Width:       v["width"],
			Height:      v["height"],
			Rx:          v["rx"],
			Ry:          v["ry"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		var transforms []string

		if translate, ok := v["translate"]; ok && translate != "" {
			transforms = append(transforms, fmt.Sprintf("translate(%s)", translate))
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			transforms = append(transforms, fmt.Sprintf("rotate(%s)", rotation))
		}

		if len(transforms) > 0 {
			rectangle.Transform = strings.Join(transforms, " ")
		}

		rectList = append(rectList, rectangle)
	}

	for _, v := range list["Ellipse"] {
		ellipse := Ellipse{
			Cx:          v["cx"],
			Cy:          v["cy"],
			Rx:          v["rx"],
			Ry:          v["ry"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		var transforms []string

		if translate, ok := v["translate"]; ok && translate != "" {
			transforms = append(transforms, fmt.Sprintf("translate(%s)", translate))
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			transforms = append(transforms, fmt.Sprintf("rotate(%s)", rotation))
		}

		if len(transforms) > 0 {
			ellipse.Transform = strings.Join(transforms, " ")
		}

		ellipseList = append(ellipseList, ellipse)
	}

	for _, v := range list["TextLiteral"] {
		text := Text{
			X:           v["x"],
			Y:           v["y"],
			Text:        v["text"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
			FontFamily:  v["font-family"],
			FontSize:    v["font-size"],
			Bold:        "",
			Underline:   "",
		}

		var transforms []string

		if translate, ok := v["translate"]; ok && translate != "" {
			transforms = append(transforms, fmt.Sprintf("translate(%s)", translate))
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			transforms = append(transforms, fmt.Sprintf("rotate(%s)", rotation))
		}

		if len(transforms) > 0 {
			text.Transform = strings.Join(transforms, " ")
		}

		if v["bold"] == "true" {
			text.Bold = "bold"
		}

		if v["underscore"] == "true" {
			text.Underline = "underline"
		}

		textList = append(textList, text)
	}

	for _, v := range list["CompositeCurve"] {

		// Extract path data and styles
		path := Path{
			D:           v["path_d"],
			Fill:        v["fillColour"],
			Stroke:      v["curveColour"],
			StrokeWidth: v["curveWidth"],
		}

		if rotation, ok := v["rotation"]; ok && rotation != "" {
			path.Transform = fmt.Sprintf("rotate(%s)", rotation)
		}

		pathList = append(pathList, path)
	}

	result := xmlResult{
		Version: "1.1",
		// Width:     "1000",
		// Height:    "1000",
		Polyline:  polylineList,
		Polygon:   polygonList,
		Path:      pathList,
		Circle:    circleList,
		Rectangle: rectList,
		Ellipse:   ellipseList,
		Text:      textList,
	}

	xmlStr, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Panic(err)
	}

	writer.Write(xmlStr)

	writer.Flush()
	fmt.Printf("SVG file created: %s\n", outputFileName)
}

// func main() {
// 	// dir := "./sst"
// 	dir := "./ttl"

// 	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			return err
// 		}

// 		// if !info.IsDir() && filepath.Ext(path) == ".sst" {
// 		if !info.IsDir() && filepath.Ext(path) == ".ttl" {
// 			fmt.Println("Found SVG file:", path)
// 			file, _ := os.Open(path)

// 			// convert the content of the turtle file "helloworldwrite.ttl", which is RDF, to a default NamedGraph.
// 			graph, err := sst.RdfRead(bufio.NewReader(file), sst.RdfFormatTurtle, sst.StrictHandler)
// 			if err != nil {
// 				fmt.Printf("Error parsing Turtle file: %v\n", err)
// 				log.Panic(err)
// 			} else {
// 				fmt.Println("Successfully parsed the Turtle file")
// 			}
// 			ConvertGraphToSVG(graph, strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)), "./svg_output")
// 		}

// 		return nil
// 	})

// 	if err != nil {
// 		fmt.Printf("Error walking the path %q: %v\n", dir, err)
// 	}
// }

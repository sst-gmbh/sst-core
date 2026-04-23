// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package svgtosst

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

func CombineShapeStyles(parentStyle, currentStyle ShapeStyle) ShapeStyle {
	combinedStyle := parentStyle

	if currentStyle.Stroke != "" {
		combinedStyle.Stroke = currentStyle.Stroke
	}
	if currentStyle.StrokeWidth != "" {
		combinedStyle.StrokeWidth = currentStyle.StrokeWidth
	}
	if currentStyle.Fill != "" {
		combinedStyle.Fill = currentStyle.Fill
	}

	return combinedStyle
}

type Matrix [3][3]float64

// Identity matrix
var IdentityMatrix = Matrix{
	{1, 0, 0},
	{0, 1, 0},
	{0, 0, 1},
}

// Matrix multiplication
func MultiplyMatrix(m1, m2 Matrix) Matrix {
	var result Matrix
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			result[i][j] = 0
			for k := 0; k < 3; k++ {
				result[i][j] += m1[i][k] * m2[k][j]
			}
		}
	}
	return result
}

// Parse the transform attribute and generate a transformation matrix
func ParseTransform(transform string) Matrix {
	// Initialize as the identity matrix
	result := IdentityMatrix

	// Match different transform operations
	re := regexp.MustCompile(`(\w+)\(([^)]+)\)`)
	matches := re.FindAllStringSubmatch(transform, -1)

	for _, match := range matches {
		op := match[1]
		args := parseArgs(match[2])

		switch op {
		case "translate":
			result = MultiplyMatrix(result, TranslateMatrix(args))
		case "scale":
			result = MultiplyMatrix(result, ScaleMatrix(args))
		case "rotate":
			result = MultiplyMatrix(result, RotateMatrix(args))
		case "matrix":
			result = MultiplyMatrix(result, MatrixTransform(args))
		}
		// fmt.Println("Resulting Matrix:")
		// for _, row := range result {
		// 	fmt.Println(row)
		// }
	}
	return result
}

// Parse arguments into a float64 array
func parseArgs(argStr string) []float64 {
	argStrs := strings.Split(argStr, ",")
	if len(argStrs) == 1 {
		argStrs = strings.Fields(argStr)
	}
	args := make([]float64, len(argStrs))
	for i, s := range argStrs {
		args[i], _ = strconv.ParseFloat(strings.TrimSpace(s), 64)
	}
	return args
}

// Translation transformation matrix
func TranslateMatrix(args []float64) Matrix {
	tx := args[0]
	ty := 0.0
	if len(args) > 1 {
		ty = args[1]
	}
	return Matrix{
		{1, 0, tx},
		{0, 1, ty},
		{0, 0, 1},
	}
}

// Scaling transformation matrix
func ScaleMatrix(args []float64) Matrix {
	sx := args[0]
	sy := sx
	if len(args) > 1 {
		sy = args[1]
	}
	return Matrix{
		{sx, 0, 0},
		{0, sy, 0},
		{0, 0, 1},
	}
}

func RotateMatrix(args []float64) Matrix {
	if len(args) < 1 {
		panic("RotateMatrix requires at least one argument: the rotation angle.")
	}

	angle := args[0] * math.Pi / 180
	cos := math.Cos(angle)
	sin := math.Sin(angle)

	return Matrix{
		{cos, -sin, 0},
		{sin, cos, 0},
		{0, 0, 1},
	}
}

// MatrixTransform creates a transformation matrix from matrix(a, b, c, d, e, f) parameters.
func MatrixTransform(args []float64) Matrix {
	if len(args) != 6 {
		panic("matrix transform requires 6 parameters")
	}
	return Matrix{
		{args[0], args[2], args[4]},
		{args[1], args[3], args[5]},
		{0, 0, 1},
	}
}

// applyMatrixToPoint applies a matrix to a point (x, y) and returns the new matrix
func applyMatrixToPoint(x, y float64, matrix Matrix) Matrix {
	// Perform the matrix multiplication
	newX := matrix[0][0]*x + matrix[0][1]*y + matrix[0][2]
	newY := matrix[1][0]*x + matrix[1][1]*y + matrix[1][2]

	// Return a new matrix representing the transformed point in homogeneous coordinates
	return Matrix{
		{matrix[0][0], matrix[0][1], newX},
		{matrix[1][0], matrix[1][1], newY},
		{0, 0, 1},
	}
}

func CombineTransforms(parentTransform, childTransform string) string {
	if parentTransform == "" {
		return childTransform
	}
	if childTransform == "" {
		return parentTransform
	}
	return parentTransform + " " + childTransform
}

func TransformShape(shape Shape, transform string) Shape {
	switch s := shape.(type) {
	case Text:
		return transformText(s, transform)
	case Rect:
		return transformRect(s, transform)
	case Circle:
		return transformCircle(s, transform)
	case Ellipse:
		return transformEllipse(s, transform)
	case Polygon:
		return transformPolygon(s, transform)
	case Polyline:
		return transformPolyline(s, transform)
	case Line:
		return transformLine(s, transform)
	case Path:
		return transformPath(s, transform)
	default:
		return shape
	}
}

// TransformText applies the transformation and returns the transformed Text
func transformText(text Text, transform string) Text {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Apply the transform to the text's position
	matrix := applyMatrixToPoint(float64(text.X), float64(text.Y), transformMatrix)

	// Calculate the direction from the matrix
	direction := math.Atan2(matrix[1][0], matrix[0][0]) * (180 / math.Pi)

	// Extract new position from the transformation matrix
	newX := matrix[0][2]
	newY := matrix[1][2]

	// Calculate the scale factor (assuming uniform scaling for simplicity)
	scale := math.Sqrt(transformMatrix[0][0]*transformMatrix[0][0] + transformMatrix[1][0]*transformMatrix[1][0])

	// Apply scale to the FontSize
	newFontSize := float64(text.FontSize) * scale

	// Return the transformed Text with updated coordinates and FontSize
	return Text{
		X:              newX,
		Y:              newY,
		Content:        text.Content,
		Transform:      text.Transform,
		Direction:      direction,
		ShapeStyle:     text.ShapeStyle,
		Style:          text.Style,
		FontFamily:     text.FontFamily,
		FontSize:       newFontSize,
		FontStyle:      text.FontStyle,
		FontWeight:     text.FontWeight,
		TextDecoration: text.TextDecoration,
	}
}

// TransformRect applies the transformation and returns Rect or Square depending on the transformed dimensions
func transformRect(rect Rect, transform string) Shape {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Apply the transform to the rectangle's top-left corner
	matrix := applyMatrixToPoint(float64(rect.X), float64(rect.Y), transformMatrix)

	// Calculate the rotation direction from the matrix elements
	direction := math.Atan2(matrix[1][0], matrix[0][0]) * (180 / math.Pi)

	// Extract the transformed position for the top-left corner from the resulting matrix
	newX := matrix[0][2]
	newY := matrix[1][2]

	// Calculate the scale factors
	scaleX := math.Sqrt(transformMatrix[0][0]*transformMatrix[0][0] + transformMatrix[1][0]*transformMatrix[1][0])
	scaleY := math.Sqrt(transformMatrix[0][1]*transformMatrix[0][1] + transformMatrix[1][1]*transformMatrix[1][1])

	// Apply scale to XLength and YLength
	newXLength := float64(rect.XLength) * scaleX
	newYLength := float64(rect.YLength) * scaleY

	// Apply scale to corner radii (RX and RY)
	newRX := float64(rect.RX) * scaleX
	newRY := float64(rect.RY) * scaleY

	// Return the transformed Rect with updated coordinates and rotation direction
	return Rect{
		X:          newX,
		Y:          newY,
		XLength:    newXLength,
		YLength:    newYLength,
		RX:         newRX,
		RY:         newRY,
		Transform:  rect.Transform,
		Direction:  direction,
		ShapeStyle: rect.ShapeStyle,
	}
}

// TransformCircle applies the transformation and returns Circle or Ellipse depending on the radii
func transformCircle(circle Circle, transform string) Shape {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Apply the transform to the center
	matrix := applyMatrixToPoint(float64(circle.CX), float64(circle.CY), transformMatrix)

	// Calculate the rotation direction from the matrix elements
	direction := math.Atan2(matrix[1][0], matrix[0][0]) * (180 / math.Pi)

	// Extract the transformed position for the top-left corner from the resulting matrix
	newX := matrix[0][2]
	newY := matrix[1][2]

	// Calculate scale factors from the transformation matrix
	scaleX := math.Sqrt(transformMatrix[0][0]*transformMatrix[0][0] + transformMatrix[1][0]*transformMatrix[1][0])
	scaleY := math.Sqrt(transformMatrix[0][1]*transformMatrix[0][1] + transformMatrix[1][1]*transformMatrix[1][1])

	// Apply the average scale to the radius
	newRadius := float64(circle.Radius) * (scaleX + scaleY) / 2

	return Circle{
		CX:         newX,
		CY:         newY,
		Radius:     newRadius,
		Transform:  circle.Transform,
		Direction:  direction,
		ShapeStyle: circle.ShapeStyle,
	}
}

// TransformEllipse applies the transformation and returns Ellipse or Circle depending on the radii
func transformEllipse(ellipse Ellipse, transform string) Shape {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Apply the transform to the center point of the ellipse
	matrix := applyMatrixToPoint(float64(ellipse.CX), float64(ellipse.CY), transformMatrix)

	// Calculate the rotation direction from the matrix elements
	direction := math.Atan2(matrix[1][0], matrix[0][0]) * (180 / math.Pi)

	// Extract the transformed position for the top-left corner from the resulting matrix
	newX := matrix[0][2]
	newY := matrix[1][2]

	// Calculate scale factors for RX and RY from the transformation matrix
	scaleX := math.Sqrt(transformMatrix[0][0]*transformMatrix[0][0] + transformMatrix[1][0]*transformMatrix[1][0])
	scaleY := math.Sqrt(transformMatrix[0][1]*transformMatrix[0][1] + transformMatrix[1][1]*transformMatrix[1][1])

	// Apply the scale to RX and RY
	newRX := float64(ellipse.RX) * scaleX
	newRY := float64(ellipse.RY) * scaleY

	// Create and return a new Ellipse object with the transformed center and scaled RX, RY
	return Ellipse{
		CX:         newX,
		CY:         newY,
		RX:         newRX,
		RY:         newRY,
		Transform:  ellipse.Transform,
		Direction:  direction,
		ShapeStyle: ellipse.ShapeStyle,
	}
}

// TransformPolygon applies the transformation and returns the transformed Polygon
func transformPolygon(polygon Polygon, transform string) Polygon {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Calculate the overall rotation direction from the matrix
	direction := math.Atan2(transformMatrix[1][0], transformMatrix[0][0]) * (180 / math.Pi)

	// Parse the points string into a slice of coordinates
	points := parseCoordinatePairs(polygon.Points)

	// Apply the transform to each point
	var transformedPoints []string
	for _, point := range points {
		matrix := applyMatrixToPoint(point[0], point[1], transformMatrix)
		transformedPoints = append(transformedPoints, fmt.Sprintf("%f,%f", matrix[0][2], matrix[1][2]))
	}

	// Reconstruct the points string
	newPoints := strings.Join(transformedPoints, " ")

	// Return the transformed Polygon with updated points
	return Polygon{
		Points:     newPoints,
		Transform:  polygon.Transform,
		Direction:  direction,
		ShapeStyle: polygon.ShapeStyle,
	}
}

// TransformPolyline applies the transformation and returns the transformed Polyline
func transformPolyline(polyline Polyline, transform string) Polyline {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Calculate the overall rotation direction from the matrix
	direction := math.Atan2(transformMatrix[1][0], transformMatrix[0][0]) * (180 / math.Pi)

	// Parse the points string into a slice of coordinates
	points := parseCoordinatePairs(polyline.Points)

	// Apply the transform to each point
	var transformedPoints []string
	for _, point := range points {
		matrix := applyMatrixToPoint(point[0], point[1], transformMatrix)
		transformedPoints = append(transformedPoints, fmt.Sprintf("%f,%f", matrix[0][2], matrix[1][2]))
	}

	// Reconstruct the points string
	newPoints := strings.Join(transformedPoints, " ")

	// Return the transformed Polyline with updated points
	return Polyline{
		Points:     newPoints,
		Transform:  polyline.Transform,
		Direction:  direction,
		ShapeStyle: polyline.ShapeStyle,
	}
}

// parsePoints parses a points string into a slice of coordinate pairs
func parseCoordinatePairs(points string) [][]float64 {
	var result [][]float64
	coords := strings.Fields(string(points)) // Split by spaces

	for _, coord := range coords {
		xy := strings.Split(coord, ",")
		if len(xy) == 2 {
			x, errX := strconv.ParseFloat(xy[0], 64)
			y, errY := strconv.ParseFloat(xy[1], 64)
			if errX == nil && errY == nil {
				result = append(result, []float64{x, y})
			}
		}
	}
	return result
}

// TransformLine applies the transformation and returns the transformed Line
func transformLine(line Line, transform string) Line {
	// Parse the transform to get the transformation matrix
	transformMatrix := ParseTransform(transform)

	// Calculate the overall rotation direction from the matrix
	direction := math.Atan2(transformMatrix[1][0], transformMatrix[0][0]) * (180 / math.Pi)

	// Apply the transform to both endpoints of the line
	newStart := applyMatrixToPoint(float64(line.X1), float64(line.Y1), transformMatrix)
	newEnd := applyMatrixToPoint(float64(line.X2), float64(line.Y2), transformMatrix)

	// Return the transformed Line with updated coordinates and direction
	return Line{
		X1:         newStart[0][2],
		Y1:         newStart[1][2],
		X2:         newEnd[0][2],
		Y2:         newEnd[1][2],
		Transform:  line.Transform,
		Direction:  direction,
		ShapeStyle: line.ShapeStyle,
	}
}

func transformPath(path Path, transform string) Path {
	fmt.Printf("original path.D: %v", path.D)
	transformMatrix := ParseTransform(transform)

	// Parse the path data ("d" attribute) into commands
	commands := parsePathCommands(path.D)

	var transformedCommands []string
	var lastX, lastY float64               // Tracks the logical position
	var lastControlX, lastControlY float64 // For S and T commands

	for _, cmd := range commands {
		var transformedParams []string
		params := cmd.Params

		switch cmd.Command {
		case "M", "m":
			for i := 0; i < len(params); i += 2 {
				x, y := params[i], params[i+1]
				if cmd.Command == "m" {
					x += lastX
					y += lastY
					cmd.Command = "M"
				}
				transformedX := transformMatrix[0][0]*x + transformMatrix[0][1]*y + transformMatrix[0][2]
				transformedY := transformMatrix[1][0]*x + transformMatrix[1][1]*y + transformMatrix[1][2]
				transformedParams = append(transformedParams, fmt.Sprintf("%.6f", transformedX), fmt.Sprintf("%.6f", transformedY))
				lastX, lastY = x, y
			}
		case "L", "l":
			for i := 0; i < len(params); i += 2 {
				x, y := params[i], params[i+1]
				if cmd.Command == "l" {
					x += lastX
					y += lastY
					cmd.Command = "L"
				}
				transformedX := transformMatrix[0][0]*x + transformMatrix[0][1]*y + transformMatrix[0][2]
				transformedY := transformMatrix[1][0]*x + transformMatrix[1][1]*y + transformMatrix[1][2]
				transformedParams = append(transformedParams, fmt.Sprintf("%.6f", transformedX), fmt.Sprintf("%.6f", transformedY))
				lastX, lastY = x, y
			}

		case "H", "h":
			for _, x := range params {
				if cmd.Command == "h" {
					x += lastX
					cmd.Command = "H"
				}
				transformedX := transformMatrix[0][0]*x + transformMatrix[0][2]
				transformedParams = append(transformedParams, fmt.Sprintf("%.6f", transformedX))
				lastX = x
			}

		case "V", "v":
			for _, y := range params {
				if cmd.Command == "v" {
					y += lastY
					cmd.Command = "V"
				}
				transformedY := transformMatrix[1][1]*y + transformMatrix[1][2]
				transformedParams = append(transformedParams, fmt.Sprintf("%.6f", transformedY))
				lastY = y
			}

		case "C", "c":
			for i := 0; i < len(params); i += 6 {
				control1X, control1Y := params[i], params[i+1]
				control2X, control2Y := params[i+2], params[i+3]
				endX, endY := params[i+4], params[i+5]
				if cmd.Command == "c" {
					control1X += lastX
					control1Y += lastY
					control2X += lastX
					control2Y += lastY
					endX += lastX
					endY += lastY
					cmd.Command = "C"
				}
				transformedControl1X := transformMatrix[0][0]*control1X + transformMatrix[0][1]*control1Y + transformMatrix[0][2]
				transformedControl1Y := transformMatrix[1][0]*control1X + transformMatrix[1][1]*control1Y + transformMatrix[1][2]
				transformedControl2X := transformMatrix[0][0]*control2X + transformMatrix[0][1]*control2Y + transformMatrix[0][2]
				transformedControl2Y := transformMatrix[1][0]*control2X + transformMatrix[1][1]*control2Y + transformMatrix[1][2]
				transformedEndX := transformMatrix[0][0]*endX + transformMatrix[0][1]*endY + transformMatrix[0][2]
				transformedEndY := transformMatrix[1][0]*endX + transformMatrix[1][1]*endY + transformMatrix[1][2]
				transformedParams = append(transformedParams,
					fmt.Sprintf("%.6f", transformedControl1X), fmt.Sprintf("%.6f", transformedControl1Y),
					fmt.Sprintf("%.6f", transformedControl2X), fmt.Sprintf("%.6f", transformedControl2Y),
					fmt.Sprintf("%.6f", transformedEndX), fmt.Sprintf("%.6f", transformedEndY))
				lastX, lastY = endX, endY
				lastControlX, lastControlY = control2X, control2Y
			}

		case "S", "s":
			for i := 0; i < len(params); i += 4 {
				control2X, control2Y := params[i], params[i+1]
				endX, endY := params[i+2], params[i+3]
				if cmd.Command == "s" {
					control2X += lastX
					control2Y += lastY
					endX += lastX
					endY += lastY
					cmd.Command = "S"
				}
				control1X := 2*lastX - lastControlX
				control1Y := 2*lastY - lastControlY
				transformedControl1X := transformMatrix[0][0]*control1X + transformMatrix[0][1]*control1Y + transformMatrix[0][2]
				transformedControl1Y := transformMatrix[1][0]*control1X + transformMatrix[1][1]*control1Y + transformMatrix[1][2]
				transformedControl2X := transformMatrix[0][0]*control2X + transformMatrix[0][1]*control2Y + transformMatrix[0][2]
				transformedControl2Y := transformMatrix[1][0]*control2X + transformMatrix[1][1]*control2Y + transformMatrix[1][2]
				transformedEndX := transformMatrix[0][0]*endX + transformMatrix[0][1]*endY + transformMatrix[0][2]
				transformedEndY := transformMatrix[1][0]*endX + transformMatrix[1][1]*endY + transformMatrix[1][2]
				transformedParams = append(transformedParams,
					fmt.Sprintf("%.6f", transformedControl1X), fmt.Sprintf("%.6f", transformedControl1Y),
					fmt.Sprintf("%.6f", transformedControl2X), fmt.Sprintf("%.6f", transformedControl2Y),
					fmt.Sprintf("%.6f", transformedEndX), fmt.Sprintf("%.6f", transformedEndY))
				lastX, lastY = endX, endY
				lastControlX, lastControlY = control2X, control2Y
			}
		case "T", "t":
			for i := 0; i < len(params); i += 2 {
				endX, endY := params[i], params[i+1]
				if cmd.Command == "t" {
					endX += lastX
					endY += lastY
					cmd.Command = "T"
				}
				controlX := 2*lastX - lastControlX
				controlY := 2*lastY - lastControlY
				transformedEndX := transformMatrix[0][0]*endX + transformMatrix[0][1]*endY + transformMatrix[0][2]
				transformedEndY := transformMatrix[1][0]*endX + transformMatrix[1][1]*endY + transformMatrix[1][2]
				transformedParams = append(transformedParams,
					fmt.Sprintf("%.6f", transformedEndX), fmt.Sprintf("%.6f", transformedEndY))
				lastX, lastY = endX, endY
				lastControlX, lastControlY = controlX, controlY
			}
		case "A", "a":
			for i := 0; i < len(params); i += 7 {
				rx, ry := params[i], params[i+1]
				rotation := params[i+2]
				largeArcFlag := int(params[i+3])
				sweepFlag := int(params[i+4])
				x, y := params[i+5], params[i+6]
				if cmd.Command == "a" {
					x += lastX
					y += lastY
					cmd.Command = "A"
				}
				transformedX := transformMatrix[0][0]*x + transformMatrix[0][1]*y + transformMatrix[0][2]
				transformedY := transformMatrix[1][0]*x + transformMatrix[1][1]*y + transformMatrix[1][2]
				transformedParams = append(transformedParams,
					fmt.Sprintf("%.6f", rx), fmt.Sprintf("%.6f", ry),
					fmt.Sprintf("%.6f", rotation), fmt.Sprintf("%d", largeArcFlag),
					fmt.Sprintf("%d", sweepFlag), fmt.Sprintf("%.6f", transformedX), fmt.Sprintf("%.6f", transformedY))
				lastX, lastY = x, y
			}
		case "Z", "z":
			transformedCommands = append(transformedCommands, cmd.Command)
			continue
		}

		transformedCommands = append(transformedCommands, fmt.Sprintf("%s %s", cmd.Command, strings.Join(transformedParams, " ")))
	}

	newD := strings.Join(transformedCommands, " ")
	fmt.Printf("New path.D: %v", newD)

	return Path{
		D:          newD,
		Transform:  path.Transform,
		ShapeStyle: path.ShapeStyle,
	}
}

// parsePathCommands parses the "d" attribute of an SVG path into PathCommand objects
func parsePathCommands(d string) []PathCommand {
	var commands []PathCommand
	var currentParams []float64

	// Regular expression to match commands and numbers
	tokens := regexp.MustCompile(`[MmZzLlHhVvCcSsQqTtAa]|-?\d*\.?\d+(?:[eE][-+]?\d+)?`).FindAllString(string(d), -1)
	var currentCommand string

	for _, token := range tokens {
		// Check if the token is a command (like M, L, A, etc.)
		if strings.ContainsAny(token, "MmZzLlHhVvCcSsQqTtAa") {
			// If there's a current command, add it with its parameters
			if currentCommand != "" && len(currentParams) > 0 {
				commands = append(commands, PathCommand{
					Command: currentCommand,
					Params:  currentParams,
				})
			}
			currentCommand = token
			currentParams = []float64{}

			if token == "Z" || token == "z" {
				commands = append(commands, PathCommand{
					Command: token,
					Params:  nil,
				})
				currentCommand = ""
			}
		} else {
			// Parse numeric parameters
			if value, err := strconv.ParseFloat(token, 64); err == nil {
				currentParams = append(currentParams, value)
			}
		}
	}

	// Add the final command and its parameters
	if currentCommand != "" && len(currentParams) > 0 {
		commands = append(commands, PathCommand{
			Command: currentCommand,
			Params:  currentParams,
		})
	}

	return commands
}

func Transform() {
	circle := Circle{
		CX:        100,                                             // Initial center x
		CY:        100,                                             // Initial center y
		Radius:    50,                                              // Initial radius
		Transform: "scale(3,4) scale(1,2), scale(8,6), rotate(45)", // Translation and scaling transform
	}
	shape := transformCircle(circle, "")
	fmt.Printf("Shape Direction: %.2f degrees\n", shape.GetDirection())
}

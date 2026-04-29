// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package ssttosvg

import (
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/semanticstep/sst-core/sst"
	_ "github.com/semanticstep/sst-core/vocabularies/dict"
	"github.com/semanticstep/sst-core/vocabularies/rep"
	"github.com/semanticstep/sst-core/vocabularies/sso"
	"github.com/google/uuid"
)

func getCurveAndIdListForSvg(graph sst.NamedGraph) map[string][]map[string]string {
	resultList := map[string][]map[string]string{}

	styleMap := generateStyleMap(graph)

	graph.ForIRINodes(func(d sst.IBNode) error {
		if d.TypeOf() != nil {
			if d.TypeOf().Is(rep.SchematicSymbolRepresentation) || d.TypeOf().Is(rep.SymbolRepresentation) || d.TypeOf().Is(rep.SchematicPortRepresentation) {
				// fmt.Println("Matched a node for processing")
				d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					if p.Is(rep.Item) {
						if o.(sst.IBNode).TypeOf().Is(rep.Circle) {
							resultMap := getCircleMap(o, styleMap)
							resultList["Circle"] = append(resultList["Circle"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.Rectangle) || o.(sst.IBNode).TypeOf().Is(rep.RoundedRectangle) {
							resultMap := getRectangleMap(o, styleMap)
							resultList["Rectangle"] = append(resultList["Rectangle"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.Ellipse) {
							resultMap := getEllipseMap(o, styleMap)
							resultList["Ellipse"] = append(resultList["Ellipse"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.Polyline) {
							resultMap := getPolylineMap(o, styleMap)
							resultList["Polyline"] = append(resultList["Polyline"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.Polygon) {
							resultMap := getPolygonMap(o, styleMap)
							resultList["Polygon"] = append(resultList["Polygon"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.TrimmedCurve) {
							resultMap := getTrimmedCurveMap(o, styleMap)
							resultList["TrimmedCurve"] = append(resultList["TrimmedCurve"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.BezierCurve) {
							resultMap := getBezierCurveMap(o, styleMap)
							resultList["BezierCurve"] = append(resultList["BezierCurve"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.TextLiteral) {
							resultMap := getTextMap(o, styleMap)
							resultList["TextLiteral"] = append(resultList["TextLiteral"], resultMap)
						} else if o.(sst.IBNode).TypeOf().Is(rep.StyledItem) {
							resultMap := getStyledItemMap(o, styleMap)
							if kind, found := resultMap["kind"]; found {

								resultList[kind] = append(resultList[kind], resultMap)
							} else {
								fmt.Println("Error: No 'kind' found in resultMap, skipping...")
							}
						} else if o.(sst.IBNode).TypeOf().Is(rep.CompositeCurve) {
							// resultMap := getCompositeCurveMap(o, styleMap)
							// if kind, found := resultMap["kind"]; found {
							// 	resultList[kind] = append(resultList[kind], resultMap)
							// } else {
							// 	fmt.Println("Error: No 'kind' found in resultMap, skipping...")
							// }
							resultMap := getCompositeCurveMap(o, styleMap)
							resultList["CompositeCurve"] = append(resultList["CompositeCurve"], resultMap)
						}
					}
					return nil
				})
			}
		}
		return nil
	})
	return resultList
}

func generateStyleMap(graph sst.NamedGraph) map[uuid.UUID]map[string]string {
	styleMap := make(map[uuid.UUID]map[string]string)

	graph.ForIRINodes(func(d sst.IBNode) error {
		if d.TypeOf() != nil {
			if d.TypeOf().Is(rep.StyledItem) {
				var key uuid.UUID
				attributeMap := make(map[string]string)

				d.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					if p.Is(rep.ItemToStyle) {
						key = o.(sst.IBNode).ID()
					}

					if p.Is(rep.Style) {
						o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
							if p.Is(rep.FillColour) {
								colour := strings.TrimPrefix(string(o.(sst.String)), "color:")
								attributeMap["fillColour"] = colour
							} else if p.Is(rep.CurveColour) {
								colour := strings.TrimPrefix(string(o.(sst.String)), "color:")
								attributeMap["curveColour"] = colour
							} else if p.Is(rep.CurveWidth) {
								attributeMap["curveWidth"] = fmt.Sprintf("%f", float64(o.(sst.Double)))
							}
							return nil
						})
					}
					return nil
				})

				if key != uuid.Nil && len(attributeMap) > 0 {
					styleMap[key] = attributeMap
				}
			}
		}
		return nil
	})

	return styleMap
}

func getCircleMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "Circle"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		var radius, x, y float64
		if p.Is(rep.Radius) {
			radius = float64(o.(sst.Double))
			resultMap["r"] = fmt.Sprintf("%f", radius)
		}
		if p.Is(rep.Position) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.Location) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x = float64(o.Member(0).(sst.Double))
								y = -float64(o.Member(1).(sst.Double))
								// resultMap["x"] = fmt.Sprintf("%f", x)
								// resultMap["y"] = fmt.Sprintf("%f", y)
								resultMap["translate"] = fmt.Sprintf("%f,%f", x, y)
							}
						}
						return nil
					})
				}
				if p.Is(rep.RefDirectionDegree) {
					angle := -float64(o.(sst.Double))
					resultMap["rotation"] = fmt.Sprintf("%f", angle)
				}
				return nil
			})
		}
		return nil
	})
	return resultMap
}

func getRectangleMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "Rectangle"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		var x, y, width, height, rx, ry float64
		if p.Is(rep.XLength) {
			width = float64(o.(sst.Double))
			resultMap["width"] = fmt.Sprintf("%f", width)
		}
		if p.Is(rep.YLength) {
			height = float64(o.(sst.Double))
			resultMap["height"] = fmt.Sprintf("%f", height)
		}
		if p.Is(rep.Position) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.Location) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x = float64(o.Member(0).(sst.Double))
								y = -float64(o.Member(1).(sst.Double))
								// resultMap["x"] = fmt.Sprintf("%f", x)
								// resultMap["y"] = fmt.Sprintf("%f", y)
								resultMap["translate"] = fmt.Sprintf("%f,%f", x, y)
							}
						}
						return nil
					})
				}
				if p.Is(rep.RefDirectionDegree) {
					angle := -float64(o.(sst.Double))
					resultMap["rotation"] = fmt.Sprintf("%f", angle)
				}
				return nil
			})
		}
		if p.Is(rep.RadiusX) {
			rx = float64(o.(sst.Double))
			resultMap["rx"] = fmt.Sprintf("%f", rx)
		}
		if p.Is(rep.RadiusY) {
			ry = float64(o.(sst.Double))
			resultMap["ry"] = fmt.Sprintf("%f", ry)
		}
		return nil
	})
	return resultMap
}

func getEllipseMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "Ellipse"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		var rx, ry, x, y float64
		if p.Is(rep.SemiAxis1) {
			rx = float64(o.(sst.Double))
			resultMap["rx"] = fmt.Sprintf("%f", rx)
		}
		if p.Is(rep.SemiAxis2) {
			ry = float64(o.(sst.Double))
			resultMap["ry"] = fmt.Sprintf("%f", ry)
		}
		if p.Is(rep.Position) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.Location) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x = float64(o.Member(0).(sst.Double))
								y = -float64(o.Member(1).(sst.Double))
								resultMap["translate"] = fmt.Sprintf("%f,%f", x, y)
							}
						}
						return nil
					})
				}
				if p.Is(rep.RefDirectionDegree) {
					angle := -float64(o.(sst.Double))
					resultMap["rotation"] = fmt.Sprintf("%f", angle)
				}
				return nil
			})
		}
		return nil
	})
	return resultMap
}

func getPolylineMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "Polyline"
	pointString := ""
	pathDString := ""

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	firstPoint := true

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.Points) {
			collection, ok := o.(sst.IBNode).AsCollection()
			if ok {
				collection.ForMembers(func(index int, o sst.Term) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x := float64(o.Member(0).(sst.Double))
								y := -float64(o.Member(1).(sst.Double))

								pointString += fmt.Sprintf("%f,%f ", x, y)

								if firstPoint {
									pathDString += fmt.Sprintf("M %f,%f ", x, y)
									firstPoint = false
								} else {
									pathDString += fmt.Sprintf("L %f,%f ", x, y)
								}
							}
						}
						return nil
					})
				})
			}

			resultMap["position"] = strings.TrimSpace(pointString)
			resultMap["path_d"] = strings.TrimSpace(pathDString)
		}
		return nil
	})
	return resultMap
}

func getPolygonMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "Polygon"
	pointString := ""

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.Points) {
			collection, ok := o.(sst.IBNode).AsCollection()
			if ok {
				collection.ForMembers(func(index int, o sst.Term) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x := float64(o.Member(0).(sst.Double))
								y := -float64(o.Member(1).(sst.Double))
								pointString += fmt.Sprintf("%f,%f ", x, y)
							}
						}
						return nil
					})
				})
			}
			resultMap["position"] = pointString
		}
		return nil
	})
	return resultMap
}

func getTrimmedCurveMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "TrimmedCurve"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	var trim1, trim2 []float64
	var center []float64
	var radius, radius_x, radius_y float64
	var angle float64
	var isClockWise bool
	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.BasisCurve) {
			// if trim a circle
			if o.(sst.IBNode).TypeOf().Is(rep.Circle) {
				o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					if p.Is(rep.Radius) {
						radius = float64(o.(sst.Double))
						radius_x = radius
						radius_y = radius
					}
					if p.Is(rep.Position) {
						o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
							if p.Is(rep.Location) {
								o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
									if p.Is(rep.Coordinates) {
										if o.TermKind() == sst.TermKindLiteralCollection {
											o := o.(sst.LiteralCollection)
											x := float64(o.Member(0).(sst.Double))
											y := -float64(o.Member(1).(sst.Double))
											center = append(center, x, y)
										}
									}
									return nil
								})
							}
							if p.Is(rep.RefDirectionDegree) {
								angle = -float64(o.(sst.Double))
								resultMap["rotation"] = fmt.Sprintf("%f", angle)
							}
							return nil
						})
					}
					return nil
				})
			} else if o.(sst.IBNode).TypeOf().Is(rep.Ellipse) {
				o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					if p.Is(rep.SemiAxis1) {
						radius_x = float64(o.(sst.Double))
					}
					if p.Is(rep.SemiAxis2) {
						radius_y = float64(o.(sst.Double))
					}
					if p.Is(rep.Position) {
						o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
							if p.Is(rep.Location) {
								o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
									if p.Is(rep.Coordinates) {
										if o.TermKind() == sst.TermKindLiteralCollection {
											o := o.(sst.LiteralCollection)
											x := float64(o.Member(0).(sst.Double))
											y := -float64(o.Member(1).(sst.Double))
											center = append(center, x, y)
										}
									}
									return nil
								})
							}
							if p.Is(rep.RefDirectionDegree) {
								angle = -float64(o.(sst.Double))
								resultMap["rotation"] = fmt.Sprintf("%f", angle)
							}
							return nil
						})
					}
					return nil
				})
			}
		}
		if p.Is(rep.Trim1) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.Coordinates) {
					if o.TermKind() == sst.TermKindLiteralCollection {
						o := o.(sst.LiteralCollection)
						x := float64(o.Member(0).(sst.Double))
						y := -float64(o.Member(1).(sst.Double))
						trim1 = append(trim1, x, y)
					}
				}
				return nil
			})
		}
		if p.Is(rep.Trim2) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.Coordinates) {
					if o.TermKind() == sst.TermKindLiteralCollection {
						o := o.(sst.LiteralCollection)
						x := float64(o.Member(0).(sst.Double))
						y := -float64(o.Member(1).(sst.Double))
						trim2 = append(trim2, x, y)
					}
				}
				return nil
			})
		}

		if p.Is(rep.SenseAgreement) {
			isClockWise = !bool(o.(sst.Boolean))
		}
		return nil
	})
	d_path := workOutTrimmedCurveForPath(trim1, trim2, center, angle, radius_x, radius_y, isClockWise)
	resultMap["path_d"] = d_path
	return resultMap
}

func getBezierCurveMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "BezierCurve"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	var controlPoints [][]float64
	var degree int

	// Traverse nodes to extract control points and degree
	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		// Check for the degree property
		if p.Is(rep.Degree) {
			degree = int(o.(sst.Integer))
		}

		// Check for the control points list
		if p.Is(rep.ControlPointsList) {
			// Extract control points from the literal collection
			collection, ok := o.(sst.IBNode).AsCollection()

			if ok {
				collection.ForMembers(func(index int, o sst.Term) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						// Extract coordinates from the collection
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x := float64(o.Member(0).(sst.Double))
								y := -float64(o.Member(1).(sst.Double))
								controlPoints = append(controlPoints, []float64{x, y})
							}
						}
						return nil
					})
				})
			}
		}
		return nil
	})

	// Generate the Bézier curve path based on the degree
	var d_path string
	switch degree {
	case 2:
		// Quadratic Bézier curve (Q)
		if len(controlPoints) != 3 {
			return nil
		}
		d_path = workOutQuadraticBezierCurveForPath(controlPoints[0], controlPoints[2], controlPoints[1])
	case 3:
		// Cubic Bézier curve (C)
		if len(controlPoints) != 4 {
			return nil
		}
		d_path = workOutCubicBezierCurveForPath(controlPoints[0], controlPoints[3], controlPoints[1], controlPoints[2])
	default:
		return nil
	}

	resultMap["path_d"] = d_path
	return resultMap
}

// Generates the path for a quadratic Bézier curve (Q)
func workOutQuadraticBezierCurveForPath(startPoint, endPoint, controlPoint []float64) string {
	return fmt.Sprintf(
		"M %f,%f Q %f,%f %f,%f",
		startPoint[0], startPoint[1], // Start point
		controlPoint[0], controlPoint[1], // Control point
		endPoint[0], endPoint[1], // End point
	)
}

// Generates the path for a cubic Bézier curve (C)
func workOutCubicBezierCurveForPath(startPoint, endPoint, controlPoint1, controlPoint2 []float64) string {
	return fmt.Sprintf(
		"M %f,%f C %f,%f %f,%f %f,%f",
		startPoint[0], startPoint[1], // Start point
		controlPoint1[0], controlPoint1[1], // Control point 1
		controlPoint2[0], controlPoint2[1], // Control point 2
		endPoint[0], endPoint[1], // End point
	)
}

func getTextMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "TextLiteral"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.Literal) {
			text := string(o.(sst.String))
			resultMap["text"] = text
		}
		if p.Is(rep.Position) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.Location) {
					o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.Coordinates) {
							if o.TermKind() == sst.TermKindLiteralCollection {
								o := o.(sst.LiteralCollection)
								x := float64(o.Member(0).(sst.Double))
								y := -float64(o.Member(1).(sst.Double))
								// resultMap["x"] = fmt.Sprintf("%f", x)
								// resultMap["y"] = fmt.Sprintf("%f", y)
								resultMap["translate"] = fmt.Sprintf("%f,%f", x, y)
							}
						}
						return nil
					})
				}
				if p.Is(rep.RefDirectionDegree) {
					angle := -float64(o.(sst.Double))
					resultMap["rotation"] = fmt.Sprintf("%f", angle)
				}
				return nil
			})
		}
		if p.Is(rep.Font) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(sso.ID) {
					fontFamily := string(o.(sst.String))
					resultMap["font-family"] = fontFamily
				}
				if p.Is(rep.FontSize) {
					if fontSize, ok := o.(sst.Double); ok {
						resultMap["font-size"] = fmt.Sprintf("%f", float64(fontSize))
					} else {
						fmt.Println("Expected DoubleLiteral for font-size, but got different type")
					}
				}
				return nil
			})
		}
		if p.Is(rep.FontModifier) {
			o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
				if p.Is(rep.TextModifer_bold) {
					resultMap["bold"] = "true"
				}
				if p.Is(rep.TextModifer_underscore) {
					resultMap["underscore"] = "true"
				}
				return nil
			})
		}
		return nil
	})
	return resultMap
}

func workOutTrimmedCurveForPath(trim1 []float64, trim2 []float64, center []float64, xAxisRotation float64, radiusX float64, radiusY float64, isClockWise bool) string {
	isBigCurve := false

	if radiusX == radiusY && radiusX < 0 {
		isClockWise = true
		radiusX = -radiusX
		radiusY = -radiusY
	}

	var angle float64
	pathD := fmt.Sprintf("M %f,%f A %f,%f %f ", trim1[0], trim1[1], radiusX, radiusY, xAxisRotation)

	if radiusX == radiusY {
		length := math.Hypot(trim1[0]-trim2[0], trim1[1]-trim2[1])
		h := radiusX - math.Sqrt(radiusX*radiusX-math.Pow(length/2, 2))
		tan := 2 * h / length
		if isClockWise {
			tan = -tan
		}
		angle = 4 * math.Atan(tan) * 180 / math.Pi
		if angle < 0 {
			angle += 360
		}
	} else {
		dTrim1 := math.Hypot(trim1[0]-center[0], trim1[1]-center[1])
		dTrim2 := math.Hypot(trim2[0]-center[0], trim2[1]-center[1])

		newT1 := []float64{radiusX / dTrim1 * trim1[0], radiusX / dTrim1 * trim1[1]}
		newT2 := []float64{radiusX / dTrim2 * trim2[0], radiusX / dTrim2 * trim2[1]}

		length := math.Hypot(newT1[0]-newT2[0], newT1[1]-newT2[1])
		h := radiusX - math.Sqrt(radiusX*radiusX-math.Pow(length/2, 2))
		tan := 2 * h / length
		if isClockWise {
			tan = -tan
		}
		angle = 4 * math.Atan(tan) * 180 / math.Pi
		if angle < 0 {
			angle += 360
		}
	}

	if angle >= 180 {
		isBigCurve = true
	}

	if isBigCurve {
		pathD += "1,"
	} else {
		pathD += "0,"
	}

	if isClockWise {
		pathD += "1 "
	} else {
		pathD += "0 "
	}

	pathD += fmt.Sprintf("%f,%f", trim2[0], trim2[1])
	return pathD
}

func getStyledItemMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "styledItem"

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	var shapeMap map[string]string

	o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.ItemToStyle) {
			shapeNode := o.(sst.IBNode)
			if shapeNode.TypeOf().Is(rep.Circle) {
				shapeMap = getCircleMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.Rectangle) || shapeNode.TypeOf().Is(rep.RoundedRectangle) {
				shapeMap = getRectangleMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.Ellipse) {
				shapeMap = getEllipseMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.Polyline) {
				shapeMap = getPolylineMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.Polygon) {
				shapeMap = getPolygonMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.TrimmedCurve) {
				shapeMap = getTrimmedCurveMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.BezierCurve) {
				shapeMap = getBezierCurveMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.TextLiteral) {
				shapeMap = getTextMap(o, styleMap)
			} else if shapeNode.TypeOf().Is(rep.CompositeCurve) {
				shapeMap = getCompositeCurveMap(o, styleMap)
			}
		}
		return nil
	})

	for key, value := range shapeMap {
		resultMap[key] = value
	}

	return resultMap
}

func getCompositeCurveMap(o sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	resultMap := make(map[string]string)
	resultMap["kind"] = "CompositeCurve"

	var segments []map[string]string

	if styles, found := styleMap[o.(sst.IBNode).ID()]; found {
		for key, value := range styles {
			resultMap[key] = value
		}
	}

	// Traverse the nodes to extract segments
	err := o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rep.Segments) {
			// Process each CompositeCurveSegment in the collection
			if collection, ok := o.(sst.IBNode).AsCollection(); ok {
				collection.ForMembers(func(index int, segmentNode sst.Term) {
					segmentMap := processCompositeCurveSegment(segmentNode, styleMap)
					if segmentMap != nil {
						segments = append(segments, segmentMap)
					}
				})
			} else {
				log.Println("Warning: Expected a collection for segments, but could not cast.")
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("Error during node traversal: %v\n", err)
	}

	// Combine all segment paths into a single path data
	var combinedPath string
	// firstSegment := true
	isContinuous := "false"
	for _, segment := range segments {
		if path, exists := segment["path_d"]; exists {
			// log.Printf("path: %v, isContinuous: %v", path, segment["isContinuous"])
			if isContinuous == "true" {
				// Remove the "M" if the segment is continuous
				trimmedPath := strings.TrimSpace(path[findNextCommandIndex(path):])
				combinedPath += trimmedPath + " "
			} else {
				// Include the full path with "M" if discontinuous
				combinedPath += path + " "
			}
			isContinuous = segment["isContinuous"]
		}
	}

	// Check if the last segment's Transition is Continuous
	if len(segments) > 0 {
		lastSegment := segments[len(segments)-1]
		if isContinuous, exists := lastSegment["isContinuous"]; exists && isContinuous == "true" {
			combinedPath = strings.TrimSpace(combinedPath) + " z"
		}
	}

	// Trim any trailing space
	resultMap["path_d"] = strings.TrimSpace(combinedPath)
	return resultMap
}

func findNextCommandIndex(path string) int {
	for i := 1; i < len(path); i++ {
		if isSVGCommand(path[i]) {
			return i
		}
	}
	return len(path)
}

func isSVGCommand(ch byte) bool {
	switch ch {
	case 'M', 'L', 'C', 'Q', 'T', 'S', 'A', 'H', 'V', 'Z':
		return true
	default:
		return false
	}
}

func processCompositeCurveSegment(segmentNode sst.Term, styleMap map[uuid.UUID]map[string]string) map[string]string {
	var parentCurveNode sst.Term
	segmentResult := make(map[string]string)

	segmentNode.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {

		if p.Is(rep.ParentCurve) {
			parentCurveNode = o
		}

		if p.Is(rep.Transition) {
			isContinuous := o.(sst.IBNode).Is(rep.TransitionCode_Continuous)
			segmentResult["isContinuous"] = fmt.Sprintf("%t", isContinuous)
		}
		// if p.Is(rep.SameSense) {
		// 	segmentResult["sameSense"] = fmt.Sprintf("%t", o.(sst.BooleanLiteral).BooleanValue())
		// 	log.Printf("SameSense property found: %v\n", o.(sst.BooleanLiteral).BooleanValue())
		// }
		return nil
	})

	// Determine the type of the parent curve and process it
	if parentCurveNode != nil {
		switch {
		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.BezierCurve):
			bezierMap := getBezierCurveMap(parentCurveNode, styleMap)
			if bezierMap != nil {
				segmentResult["path_d"] = bezierMap["path_d"]
			} else {
				log.Println("Failed to process BezierCurve.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.TrimmedCurve):
			trimmedMap := getTrimmedCurveMap(parentCurveNode, styleMap)
			if trimmedMap != nil {
				segmentResult["path_d"] = trimmedMap["path_d"]
			} else {
				log.Println("Failed to process TrimmedCurve.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.Circle):
			circleMap := getCircleMap(parentCurveNode, styleMap)
			if circleMap != nil {
				segmentResult["path_d"] = circleMap["path_d"]
			} else {
				log.Println("Failed to process Circle.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.Rectangle):
			rectangleMap := getRectangleMap(parentCurveNode, styleMap)
			if rectangleMap != nil {
				segmentResult["path_d"] = rectangleMap["path_d"]
			} else {
				log.Println("Failed to process Rectangle.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.Ellipse):
			ellipseMap := getEllipseMap(parentCurveNode, styleMap)
			if ellipseMap != nil {
				segmentResult["path_d"] = ellipseMap["path_d"]
			} else {
				log.Println("Failed to process Ellipse.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.Polyline):
			polylineMap := getPolylineMap(parentCurveNode, styleMap)
			if polylineMap != nil {
				segmentResult["path_d"] = polylineMap["path_d"]
			} else {
				log.Println("Failed to process Polyline.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.Polygon):
			polygonMap := getPolygonMap(parentCurveNode, styleMap)
			if polygonMap != nil {
				segmentResult["path_d"] = polygonMap["path_d"]
			} else {
				log.Println("Failed to process Polygon.")
			}

		case parentCurveNode.(sst.IBNode).TypeOf().Is(rep.TextLiteral):
			textMap := getTextMap(parentCurveNode, styleMap)
			if textMap != nil {
				segmentResult["text"] = textMap["text"]
			} else {
				log.Println("Failed to process Text.")
			}

		default:
			log.Println("Unknown curve type encountered.")
		}
	} else {
		log.Println("No parentCurve found in this CompositeCurveSegment.")
	}

	return segmentResult
}

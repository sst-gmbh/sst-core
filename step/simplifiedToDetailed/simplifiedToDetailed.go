// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// convert SST data in compact form to verbose form as needed for STEP AP242 (ISO 10303-242) export
package simplifiedToDetailed

import (
	"fmt"

	"git.semanticstep.net/x/sst/sst"
	"git.semanticstep.net/x/sst/vocabularies/lci"
	"git.semanticstep.net/x/sst/vocabularies/owl"
	"git.semanticstep.net/x/sst/vocabularies/rdf"
	"git.semanticstep.net/x/sst/vocabularies/rdfs"
	"git.semanticstep.net/x/sst/vocabularies/rep"
	"git.semanticstep.net/x/sst/vocabularies/sso"
)

type UIDRef struct {
	Ref *string `xml:"uidRef,attr,omitempty"`
}

type simplifiedData struct {
	graph                    sst.NamedGraph
	topologyToGeometryOrigin []sst.IBNode
	topologyToGeometryTarget []sst.IBNode
	edgeBasedCurveWithLength []sst.IBNode
	harnessSegment           []sst.IBNode
	harnessNode              []sst.IBNode
	pointOnCurve             []sst.IBNode
	edgeBasedTopology        sst.IBNode
	wiringHarness            sst.IBNode
}

func newSimplifiedData(graph sst.NamedGraph) *simplifiedData {
	sp := new(simplifiedData)
	sp.graph = graph
	sp.topologyToGeometryOrigin = []sst.IBNode{}
	sp.topologyToGeometryTarget = []sst.IBNode{}
	sp.edgeBasedCurveWithLength = []sst.IBNode{}
	sp.harnessSegment = []sst.IBNode{}
	sp.harnessNode = []sst.IBNode{}
	sp.pointOnCurve = []sst.IBNode{}
	sp.edgeBasedTopology = nil
	sp.wiringHarness = nil

	return sp
}

func SimplifiedToDetailed(graph sst.NamedGraph) (sst.NamedGraph, error) {
	sp := newSimplifiedData(graph)

	graph, err := sp.handleConversion(graph)
	if err != nil {
		return graph, fmt.Errorf("error handling conversion: %w", err)
	}

	if len(sp.edgeBasedCurveWithLength) > 0 {
		sp.createConnectedEdgeSet()
	}
	sp.attachHarnessToWiringHarness()
	sp.attachToEdgeBasedTopology()
	sp.createTopologyToGeometry()

	return graph, nil
}

func (sp *simplifiedData) handleConversion(graph sst.NamedGraph) (sst.NamedGraph, error) {
	err := graph.ForIRINodes(func(t sst.IBNode) error {
		if t.TypeOf() != nil {
			switch t.TypeOf().InVocabulary().(type) {
			case sso.IsPart_PartDesign:
				sp.removeElement(t, sso.Part_PartDesign.Element)
				sp.createPartStructure(t)
				t.AddStatement(rdf.Type, sso.PartDesign)
			case sso.IsPart_AssemblyDesign:
				sp.removeElement(t, sso.Part_AssemblyDesign.Element)
				sp.createPartStructure(t)
				t.AddStatement(rdf.Type, sso.AssemblyDesign)
			case sso.IsPart_WiringHarnessAssemblyDesign:
				sp.removeElement(t, sso.Part_WiringHarnessAssemblyDesign.Element)
				sp.createPartStructure(t)
				t.AddStatement(rdf.Type, sso.WiringHarnessAssemblyDesign)
				sp.wiringHarness = t

			case sso.IsPart_WiringHarnessAssemblyDesign_EdgeBasedTopologicalRepresentationWithLengthConstraint:
				sp.removeElement(t, sso.Part_WiringHarnessAssemblyDesign_EdgeBasedTopologicalRepresentationWithLengthConstraint.Element)
				sp.createPartStructure(t)
				t.AddStatement(rdf.Type, sso.WiringHarnessAssemblyDesign)
				sp.wiringHarness = t

				geometricContext := graph.CreateIRINode("", rep.GeometricRepresentationContext)

				geometricContext.AddStatement(rep.CoordinateSpaceDimension, sst.Integer(1))

				edgeBasedTopology := graph.CreateIRINode("", rep.EdgeBasedTopologicalRepresentationWithLengthConstraint)

				sp.edgeBasedTopology = edgeBasedTopology
				edgeBasedTopology.AddStatement(rep.ContextOfItems, geometricContext)
				t.AddStatement(sso.Topology, edgeBasedTopology)

				// get item_feature from combined and add to edgeBasedTopology as item.
				// transfer attached punning data that belong here
				t.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
					if p.Is(rep.Item_feature) {
						if !o.(sst.IBNode).TypeOf().Is(sso.HarnessSegment_EdgeBoundedCurveWithLength) {
							edgeBasedTopology.AddStatement(rep.Item, o.(sst.IBNode))
						}
					}
					if p.IsUuidFragment() && o.TermKind() == sst.TermKindIBNode {
						if o.(sst.IBNode).TypeOf().Is(rep.GeometricModel) {
							edgeBasedTopology.AddStatement(p, o.(sst.IBNode))
						}
					}
					return nil
				})

				sp.removeElement(t, rep.GeometricModel.Element)
				sp.removeElement(t, rep.Item_feature.Element)
			case sso.IsHarnessSegment_EdgeBoundedCurveWithLength, sso.IsHarnessSegment_SubEdge:
				harnessSegment := graph.CreateIRINode("", sso.HarnessSegment)

				sp.harnessSegment = append(sp.harnessSegment, harnessSegment)
				harnessSegment.AddStatement(sso.RepresentedGeometry, t)

				if t.TypeOf().Is(sso.HarnessSegment_EdgeBoundedCurveWithLength) {
					t.AddStatement(rdf.Type, rep.EdgeBoundedCurveWithLength)
					t.AddStatement(rep.SameSense, sst.Boolean(false))
					sp.edgeBasedCurveWithLength = append(sp.edgeBasedCurveWithLength, t)
					sp.removeElement(t, sso.HarnessSegment_EdgeBoundedCurveWithLength.Element)

					boundedCurveWithLength := graph.CreateIRINode("", rep.BoundedCurveWithLength)

					// get length values to assign to boundedCurveWithLength
					t.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.LengthValue) {
							o.(sst.IBNode).ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
								if o.TermKind() == sst.TermKindLiteral {
									if value, ok := o.(sst.Literal).(sst.Double); ok {
										boundedCurveWithLength.AddStatement(rep.LengthValue, sst.Double(value))
									}
								}
								return nil
							})
						}
						if p.Is(rep.TopologyToGeometryItemAssociation) {
							sp.topologyToGeometryOrigin = append(sp.topologyToGeometryOrigin, t)
							sp.topologyToGeometryTarget = append(sp.topologyToGeometryTarget, o.(sst.IBNode))
						}
						return nil
					})
					t.AddStatement(rep.EdgeGeometry, boundedCurveWithLength)
				}

				// remove rep:LengthValue collection
				sp.removeElement(t, rep.LengthValue.Element)
				sp.removeElement(t, rep.TopologyToGeometryItemAssociation.Element)
				if t.TypeOf().Is(sso.HarnessSegment_SubEdge) {
					t.AddStatement(rdf.Type, rep.SubEdge)
					sp.removeElement(t, sso.HarnessSegment_SubEdge.Element)
				}
			case sso.IsHarnessNode_VertexPoint, sso.IsHarnessNode_VertexPointOnCurve:
				t.AddStatement(rdf.Type, rep.VertexPoint)
				harnessNode := graph.CreateIRINode("", sso.HarnessNode)

				// get topology_to_geometry connection
				topologyToGeometry := t.GetObjects(rep.TopologyToGeometryItemAssociation)
				for _, topologyObject := range topologyToGeometry {
					sp.topologyToGeometryOrigin = append(sp.topologyToGeometryOrigin, t)
					sp.topologyToGeometryTarget = append(sp.topologyToGeometryTarget, topologyObject.(sst.IBNode))
				}

				harnessNode.AddStatement(sso.RepresentedGeometry, t)
				sp.harnessNode = append(sp.harnessNode, harnessNode)
				sp.removeElement(t, rep.TopologyToGeometryItemAssociation.Element)

				if t.TypeOf().Is(sso.HarnessNode_VertexPoint) {
					sp.removeElement(t, sso.HarnessNode_VertexPoint.Element)
					point := graph.CreateIRINode("", rep.Point)

					t.AddStatement(rep.VertexGeometry, point)
				}
				if t.TypeOf().Is(sso.HarnessNode_VertexPointOnCurve) {
					sp.removeElement(t, sso.HarnessNode_VertexPointOnCurve.Element)
					pointCurve := graph.CreateIRINode("", rep.PointOnCurve)

					sp.pointOnCurve = append(sp.pointOnCurve, pointCurve)

					// extract basisCurve and pointParameter add it to pointCurve
					t.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
						if p.Is(rep.VertexGeometry) {
							pointCurve.AddStatement(rep.BasisCurve, o.(sst.IBNode))
						}
						if o.TermKind() == sst.TermKindLiteral {
							if value, ok := o.(sst.Literal).(sst.Double); ok {
								pointCurve.AddStatement(rep.PointParameter, sst.Double(value))
							}
						}
						return nil
					})

					sp.removeElement(t, rep.VertexGeometry.Element)
					t.AddStatement(rep.VertexGeometry, pointCurve)
					sp.removeElement(t, rep.PointParameter.Element)
				}
			}
		}
		return nil
	})

	if err != nil {
		return graph, err
	}

	return graph, nil
}

func (sp *simplifiedData) createPartStructure(t sst.IBNode) error {
	part := sp.graph.CreateIRINode("", sso.Part)

	// transfer all rdf:Type statements to part
	t.ForAll(func(_ int, s, p sst.IBNode, o sst.Term) error {
		if p.Is(rdf.Type) {
			part.AddStatement(rdf.Type, o.(sst.IBNode))
			sp.removeElement(t, o.(sst.IBNode).InVocabulary().VocabularyElement())
		}
		if o.TermKind() == sst.TermKindLiteral {
			if p.Is(sso.ID) {
				part.AddStatement(sso.ID, o.(sst.String))
			}
			if p.Is(rdfs.Comment) {
				part.AddStatement(rdfs.Comment, o.(sst.String))
			}
		}
		return nil
	})

	partVersion := sp.graph.CreateIRINode("", sso.PartVersion)

	part.AddStatement(sso.HasPartVersion, partVersion)
	partVersion.AddStatement(sso.HasProductDefinition, t)

	return nil
}

func (sp *simplifiedData) removeElement(node sst.IBNode, elementType sst.Element) {
	node.ForDelete(func(index int, s, p sst.IBNode, o sst.Term) bool {
		if o.TermKind() == sst.TermKindIBNode {
			if p.Is(rdf.Type) && o.(sst.IBNode).Is(elementType) {
				return true
			}
			if o.(sst.IBNode) != nil && p.Is(elementType) {
				return true
			}
			if p != nil && o.(sst.IBNode).TypeOf() != nil && o.(sst.IBNode).TypeOf().Is(elementType) {
				return true
			}
		}

		if o.TermKind() == sst.TermKindLiteral {
			if p.Is(elementType) {
				return true
			}
		}

		return false
	})
}

func (sp *simplifiedData) createConnectedEdgeSet() error {
	part := sp.graph.CreateIRINode("", rep.ConnectedEdgeSet)

	for _, edge := range sp.edgeBasedCurveWithLength {
		part.AddStatement(rep.CesEdges, edge)
	}

	if sp.edgeBasedTopology == nil {
		sp.edgeBasedTopology.AddStatement(rep.Item, part)
		return fmt.Errorf("edgeBasedTopology is nil")
	}

	return nil
}

func (sp *simplifiedData) attachHarnessToWiringHarness() error {
	for _, node := range sp.harnessSegment {
		sp.wiringHarness.AddStatement(lci.HasFeature, node)
	}

	for _, node := range sp.harnessNode {
		sp.wiringHarness.AddStatement(lci.HasFeature, node)
	}

	return nil
}

func (sp *simplifiedData) attachToEdgeBasedTopology() error {
	for _, point := range sp.pointOnCurve {
		sp.edgeBasedTopology.AddStatement(rep.Item, point)
	}
	return nil
}

func (sp *simplifiedData) createTopologyToGeometry() error {
	if len(sp.topologyToGeometryOrigin) == len(sp.topologyToGeometryTarget) {
		modelAssociation := sp.graph.CreateIRINode("", owl.ObjectProperty)

		modelAssociation.AddStatement(rdfs.SubPropertyOf, rep.TopologyToGeometryModelAssociation)

		// get defining_geometry from wiring_harness for punning
		// definingGeometry := sp.wiringHarness.GetObjects(sso.DefiningGeometry)
		for _, definingGeometry := range sp.wiringHarness.GetObjects(sso.DefiningGeometry) {
			if definingGeometry.(sst.IBNode) != nil {
				definingGeometry.(sst.IBNode).AddStatement(modelAssociation, sp.edgeBasedTopology)

				// create TopologyToGeometryItemAssociation and create punning
				for i, origin := range sp.topologyToGeometryOrigin {
					itemAssociation := sp.graph.CreateIRINode("", owl.ObjectProperty)
					modelAssociation.AddStatement(rep.TransformationOperator, itemAssociation)
					itemAssociation.AddStatement(rdfs.SubPropertyOf, rep.TopologyToGeometryItemAssociation)

					// create punning
					sp.topologyToGeometryTarget[i].AddStatement(itemAssociation, origin)
				}
			}
		}
	}
	return nil
}

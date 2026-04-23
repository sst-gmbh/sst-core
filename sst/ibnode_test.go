// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"math/bits"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockedTestGraph struct {
	namedGraph
	mock.Mock
}

func Test_nodeCopyReplacements_mergeNodeToNamedGraph(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	type args struct {
		s              *ibNode
		toC            map[*namedGraph]namedGraphsAndOffset
		fromC          *namedGraph
		fromMatch      fromMatchFunc
		fragment       string
		stateAssertion func(assert.TestingT)
	}
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "l1",
	}
	lc1 := literalCollection{
		typedResource: typedResource{&literalCollectionResourceType.ibNode},
		dataType:      &literalTypeLangString.ibNode,
		members:       []string{"m1", "m2"},
	}
	newMockedGraphWithNodes := func(c *namedGraph, dd ...*ibNode) (g mockedTestGraph) {
		g = mockedTestGraph{namedGraph: *c}
		for _, d := range dd {
			df := d.Fragment()
			g.On("iriNode", df).Return(d, nil)
		}
		g.On("iriNode", mock.AnythingOfType("string")).
			Return((*ibNode)(nil), ErrIRINodeNotFound)
		return
	}
	type test struct {
		r         nodeCopyReplacements
		args      args
		assertion assert.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "move and merge node with references",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1, s2c1 ibNodeString
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s2c1.typedResource},
							{p: &p.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
						},
					}
					// var g1 = mockedTestGraph{
					// 	namedGraph: c1,
					// }
					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
					}
					var s1c2, s3c2, s4c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s3c2.typedResource},
							{p: &p.ibNode, t: &l1.typedResource},
							{p: &p.ibNode, t: &s4c2.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_01_00_00_00},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						// fragment: "s1",
					}
					s3c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 3,
							triplexEnd:   4,
							flags:        iriNodeType | stringNode,
						},
					}
					s4c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 4,
							triplexEnd:   5,
							flags:        iriNodeType | stringNode,
						},
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s4c2.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s2c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
							}, nodeTriples(t, &s3c2.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s4c2.ibNode},
							}, nodeTriples(t, &s4c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move referenced from merge candidate",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1, s2c1 ibNodeString
					var s1c2, s3c2, s4c2 ibNodeString
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s2c1.typedResource},
							{p: &p.ibNode, t: &s1c1.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_00_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
						},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
					}
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s3c2.typedResource},
							{p: &p.ibNode, t: &l1.typedResource},
							{p: &p.ibNode, t: &s4c2.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_00_00_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
						},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s3c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 2,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
					}
					s4c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 3,
							triplexEnd:   4,
							flags:        iriNodeType | stringNode,
						},
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
							}, nodeTriples(t, &s3c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "merge moved node",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1, s2c1 ibNodeString
					var s1c2, s3c2 ibNodeString
					//
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s2c1.typedResource},
							{p: &p.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							// "s1": &s1c2,
						},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
					}
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s3c2.typedResource},
							{p: &p.ibNode, t: &l1.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_00_00},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 2,
							triplexEnd:   4,
							flags:        iriNodeType | stringNode,
						},
						// fragment: "s1",
					}
					s3c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 2,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s2c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
							}, nodeTriples(t, &s3c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move and merge self referenced node",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1 ibNodeString
					//
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &l1.typedResource},
						},
						triplexKinds: nil,
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
						},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					var s1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s1c2.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
						},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move and merge forward referenced property",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s2c1, p1c1 ibNodeString
					//
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &l1.typedResource},
						},
						triplexKinds: nil,
						stringNodes: map[string]*ibNodeString{
							"s2": &s2c1,
							"p1": &p1c1,
						},
					}

					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s2",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   0,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					var s1c2, s2c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &s2c2.typedResource},
							{p: &p1c2.ibNode, t: &l1.typedResource},
							{p: &p1c2.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_00_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"s2": &s2c2,
							"p1": &p1c2,
						},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 2,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s2",
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   0,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					// g1 = newMockedGraphWithNodes(&c1, &s2c1.ibNode, &p1c1.ibNode)
					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c2.ibNode, p: &p1c1.ibNode, o: &s2c2.ibNode},
								{s: &s1c2.ibNode, p: &p1c1.ibNode, o: String("l1")},
							}, nodeTriples(t, &s1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "copy node with references",
			test: func() (tt test) {
				tt = test{
					r: nodeCopyReplacements{},
					args: func() args {
						var s1c1 ibNodeString
						p := newTestProperty()
						//
						c1 := namedGraph{
							stage:          st.(*stage),
							triplexStorage: []triplex{},
							triplexKinds:   nil,
							stringNodes: map[string]*ibNodeString{
								"s1c1": &s1c1,
							},
						}
						// g1 := mockedTestGraph{
						// 	namedGraph: c1,
						// }
						s1c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &c1,
								triplexStart: 0,
								triplexEnd:   0,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c1",
						}

						var s1c2, s3c2, s4c2 ibNodeString
						c2 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c2.typedResource},
								{p: &p.ibNode, t: &l1.typedResource},
								{p: &p.ibNode, t: &s4c2.typedResource},
								{p: &p.ibNode, t: &s1c2.typedResource},
								{p: &p.ibNode, t: &s1c2.typedResource},
							},
							triplexKinds: []uint{0b_01_01_00_00},
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c2,
							},
						}
						s1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   3,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 3,
								triplexEnd:   4,
								flags:        iriNodeType | stringNode,
							},
						}
						s4c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 4,
								triplexEnd:   5,
								flags:        iriNodeType | stringNode,
							},
						}

						return args{
							s:         &s1c2.ibNode,
							toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
							fromC:     &c2,
							fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
							fragment:  "s1c2",
							stateAssertion: func(t assert.TestingT) {
								assert.Equal(t, 6, len(tt.r))
								s1c2c, s3c2c, s4c2c := tt.r[&s1c2.ibNode], tt.r[&s3c2.ibNode], tt.r[&s4c2.ibNode]
								assert.Equal(t, &c1, s1c2c.ng)
								assert.ElementsMatch(t, []spo{
									{s: s1c2c, p: &p.ibNode, o: s3c2c},
									{s: s1c2c, p: &p.ibNode, o: String("l1")},
									{s: s4c2c, p: &p.ibNode, o: s1c2c},
								}, nodeTriples(t, s1c2c))
								assert.Equal(t, &c2, s3c2c.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s1c2.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
								}, nodeTriples(t, s3c2c))
								assert.Equal(t, &c2, s4c2c.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s4c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
								}, nodeTriples(t, s4c2c))
							},
						}
					}(),
					assertion: assert.NoError,
				}
				return
			}(),
		},
		{
			name: "copy replaced node",
			test: func() (tt test) {
				r := nodeCopyReplacements{}
				tt = test{
					r: r,
					args: func() args {
						p := newTestProperty()
						//
						c1 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{}, {},
							},
							triplexKinds: nil,
						}
						g1 := mockedTestGraph{
							namedGraph: c1,
						}
						s1c1 := ibNodeString{
							ibNode: ibNode{
								ng:           &g1.namedGraph,
								triplexStart: 0,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							// fragment: "s1c1",
						}
						var s1c2, s3c2, s4c2 ibNodeString
						c2 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c2.typedResource},
								{p: &p.ibNode, t: &l1.typedResource},
								{p: &p.ibNode, t: &s4c2.typedResource},
								{p: &p.ibNode, t: &s1c2.typedResource},
								{p: &p.ibNode, t: &s1c2.typedResource},
							},
							triplexKinds: []uint{0b_01_01_00_00},
						}
						s1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   3,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 3,
								triplexEnd:   4,
								flags:        iriNodeType | stringNode,
							},
						}
						s1c2c, s3c2c := s1c2, s3c2
						r[&s1c2.ibNode] = &s1c2c.ibNode
						r[&s1c2c.ibNode] = &s1c2c.ibNode
						r[&s3c2.ibNode] = &s3c2c.ibNode
						r[&s3c2c.ibNode] = &s3c2c.ibNode
						s4c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 4,
								triplexEnd:   5,
								flags:        iriNodeType | stringNode,
							},
						}
						g1 = newMockedGraphWithNodes(&c1, &s1c1.ibNode)
						return args{
							s:         &s1c2.ibNode,
							toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
							fromC:     &c2,
							fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
							fragment:  "s1c2",
							stateAssertion: func(t assert.TestingT) {
								assert.Equal(t, 6, len(tt.r))
								s4c2c := tt.r[&s4c2.ibNode]
								assert.Equal(t, &s1c2c.ibNode, tt.r[&s1c2.ibNode])
								assert.Equal(t, &s3c2c.ibNode, tt.r[&s3c2.ibNode])
								assert.ElementsMatch(t, []spo{
									{s: &s1c2c.ibNode, p: &p.ibNode, o: &s3c2c.ibNode},
									{s: &s1c2c.ibNode, p: &p.ibNode, o: String("l1")},
									{s: s4c2c, p: &p.ibNode, o: &s1c2c.ibNode},
								}, nodeTriples(t, &s1c2c.ibNode))
								assert.Equal(t, &c2, s3c2c.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s1c2.ibNode, p: &p.ibNode, o: &s3c2.ibNode},
								}, nodeTriples(t, &s3c2c.ibNode))
								assert.Equal(t, &c2, s4c2c.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s4c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
								}, nodeTriples(t, s4c2c))
							},
						}
					}(),
					assertion: assert.NoError,
				}
				return
			}(),
		},
		{
			name: "copy and merge self referenced node forward first",
			test: test{
				r: nodeCopyReplacements{},
				args: func() args {
					p := newTestProperty()
					var s1c1 ibNodeString
					//
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &l1.typedResource},
						},
						triplexKinds: nil,
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
						},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					var s1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s1c2.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
						},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
						fromC:     &c2,
						fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
							}, nodeTriples(t, &s1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "copy and merge self referenced node inverse first",
			test: test{
				r: nodeCopyReplacements{},
				args: func() args {
					p := newTestProperty()
					var s1c1 ibNodeString
					//
					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &l1.typedResource},
						},
						triplexKinds: nil,
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
						},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					var s1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s1c2.typedResource},
							{p: &p.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
						},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
						fromC:     &c2,
						fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
							}, nodeTriples(t, &s1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move both related nodes with triples on target",
			test: func() (tt test) {
				tt = test{
					r: nodeMoveReplacements(),
					args: func() args {
						p := newTestProperty()
						//
						var s1c1, s3c1 ibNodeString
						c1 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c1.typedResource},
								{p: &p.ibNode, t: &s1c1.typedResource},
							},
							triplexKinds: []uint{0b_01},
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c1,
								"s3c2": &s3c1,
							},
						}
						s1c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &c1,
								triplexStart: 0,
								triplexEnd:   1,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &c1,
								triplexStart: 1,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s3c2",
						}
						var s1c2, s3c2 ibNodeString
						c2 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c2.typedResource},
								{p: &p.ibNode, t: &l1.typedResource},
								{p: &p.ibNode, t: &s1c2.typedResource},
							},
							triplexKinds: []uint{0b_01},
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c2,
								"s3c2": &s3c2,
							},
						}
						s1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 2,
								triplexEnd:   3,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s3c2",
						}
						return args{
							s:         &s1c2.ibNode,
							toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
							fromC:     &c2,
							fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
							fragment:  "s1c2",
							stateAssertion: func(t assert.TestingT) {
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
									{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
									{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								}, nodeTriples(t, &s1c1.ibNode))
								assert.Equal(t, &c1, s3c2.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
								}, nodeTriples(t, &s3c2.ibNode))
								s3c1c := tt.r.mergeNodeToNamedGraph(&s3c2.ibNode, tt.args.toC, tt.args.fromMatch, s3c2.fragment)
								assert.Same(t, &s3c1.ibNode, s3c1c)
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
									{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								}, nodeTriples(t, &s1c1.ibNode))
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
								}, nodeTriples(t, &s3c1.ibNode))
							},
						}
					}(),
					assertion: assert.NoError,
				}
				return
			}(),
		},
		{
			name: "copy both related nodes with triples on target",
			test: func() (tt test) {
				tt = test{
					r: nodeCopyReplacements{},
					args: func() args {
						p := newTestProperty()
						//
						var s1c1, s3c1 ibNodeString
						c1 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c1.typedResource},
								{p: &p.ibNode, t: &s1c1.typedResource},
							},
							triplexKinds: []uint{0b_01},
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c1,
								"s3c2": &s3c1,
							},
						}

						g1 := mockedTestGraph{
							namedGraph: c1,
						}

						s1c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &g1.namedGraph,
								triplexStart: 0,
								triplexEnd:   1,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &g1.namedGraph,
								triplexStart: 1,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s3c2",
						}
						var s1c2, s3c2 ibNodeString
						c2 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c2.typedResource},
								{p: &p.ibNode, t: &l1.typedResource},
								{p: &p.ibNode, t: &s1c2.typedResource},
							},
							triplexKinds: []uint{0b_01},
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c2,
								"s3c2": &s3c2,
							},
						}
						s1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 2,
								triplexEnd:   3,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s3c2",
						}
						g1 = newMockedGraphWithNodes(&c1, &s1c1.ibNode, &s3c1.ibNode)
						return args{
							s:         &s1c2.ibNode,
							toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
							fromC:     &c2,
							fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
							fragment:  "s1c2",
							stateAssertion: func(t assert.TestingT) {
								assert.Equal(t, 4, len(tt.r))
								s1c2c, s3c2c := tt.r[&s1c2.ibNode], tt.r[&s3c2.ibNode]
								assert.Same(t, s1c2c, &s1c1.ibNode)
								assert.Equal(t, &c1, s3c2c.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
									{s: s3c2c, p: &p.ibNode, o: &s1c1.ibNode},
									{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								}, nodeTriples(t, &s1c1.ibNode))
								assert.Equal(t, &c2, s3c2.ng)
								assert.ElementsMatch(t, []spo{
									{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
								}, nodeTriples(t, &s3c2.ibNode))
								s3c1c := tt.r.mergeNodeToNamedGraph(&s3c2.ibNode, tt.args.toC, tt.args.fromMatch, s3c2.fragment)
								assert.Same(t, &s3c1.ibNode, s3c1c)
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
									{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								}, nodeTriples(t, &s1c1.ibNode))
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
								}, nodeTriples(t, &s3c1.ibNode))
							},
						}
					}(),
					assertion: assert.NoError,
				}
				return
			}(),
		},
		{
			name: "move node with literal collection",
			test: func() (tt test) {
				tt = test{
					r: nodeMoveReplacements(),
					args: func() args {
						p := newTestProperty()
						//
						var s1c1, s3c1 ibNodeString
						c1 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &s3c1.typedResource},
								{p: &p.ibNode, t: &s1c1.typedResource},
							},
							triplexKinds: []uint{0b_01},
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c1,
								"s3":   &s3c1,
							},
						}
						s1c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &c1,
								triplexStart: 0,
								triplexEnd:   1,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &c1,
								triplexStart: 1,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s3",
						}
						var s1c2, s3c2 ibNodeString
						c2 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &s3c2.ibNode, t: &lc1.typedResource},
							},
							triplexKinds: nil,
							stringNodes: map[string]*ibNodeString{
								"s1c2": &s1c2,
								"s3":   &s3c2,
							},
						}
						s1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   1,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1c2",
						}
						s3c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   0,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s3",
						}
						return args{
							s:         &s1c2.ibNode,
							toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
							fromC:     &c2,
							fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
							fragment:  "s1c2",
							stateAssertion: func(t assert.TestingT) {
								assert.ElementsMatch(t, []spo{
									{s: &s3c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
									{s: &s1c1.ibNode, p: &s3c1.ibNode, o: &lc1},
								}, nodeTriples(t, &s1c1.ibNode))
							},
						}
					}(),
					assertion: assert.NoError,
				}
				return
			}(),
		},
		{
			name: "move and merge node with tracked property references",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, s2c1, p1c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &s2c1.typedResource},
							{p: &p1c1.ibNode, t: &s1c1.typedResource},
							{p: &s2c1.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_10_00_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 2,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					var s1c2, s3c2, s4c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &s3c2.typedResource},
							{p: &p1c2.ibNode, t: &l1.typedResource},
							{p: &p1c2.ibNode, t: &s4c2.typedResource},
							{p: &p1c2.ibNode, t: &s1c2.typedResource},
							{p: &p1c2.ibNode, t: &s1c2.typedResource},
							{p: &s1c2.ibNode, t: &s3c2.typedResource},
							{p: &s1c2.ibNode, t: &l1.typedResource},
							{p: &s1c2.ibNode, t: &s4c2.typedResource},
						},
						triplexKinds: []uint{0b_10_10_10_01_01_00_00_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s3c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 3,
							triplexEnd:   4,
							flags:        iriNodeType | stringNode,
						},
					}
					s4c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 4,
							triplexEnd:   5,
							flags:        iriNodeType | stringNode,
						},
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 5,
							triplexEnd:   8,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p1c1.ibNode, o: &s1c1.ibNode},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s3c2.ibNode},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s4c2.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s3c2.ibNode},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s4c2.ibNode},
							}, nodeTriples(t, &p1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move and merge tracked property node from merge candidate",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, s2c1, p1c1 ibNodeString
					var s3c2, s4c2, p1c2 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &s2c1.typedResource},
							{p: &p1c2.ibNode, t: &s3c2.typedResource},
							{p: &p1c2.ibNode, t: &l1.typedResource},
							{p: &p1c2.ibNode, t: &s4c2.typedResource},
							{p: &p1c1.ibNode, t: &s1c1.typedResource},
							{p: &s2c1.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_10_00_00_00_00_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   4,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 4,
							triplexEnd:   5,
							flags:        iriNodeType | stringNode,
						},
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:    &c1,
							flags: iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &s1c1.typedResource},
							{p: &p1c2.ibNode, t: &s1c1.typedResource},
							{p: &s1c1.ibNode, t: &s3c2.typedResource},
							{p: &s1c1.ibNode, t: &l1.typedResource},
							{p: &s1c1.ibNode, t: &s4c2.typedResource},
						},
						triplexKinds: []uint{0b_10_10_10_01_01},
						stringNodes: map[string]*ibNodeString{
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s3c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
					}
					s4c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 2,
							triplexEnd:   5,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &p1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "p1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p1c1.ibNode, o: &s1c1.ibNode},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s3c2.ibNode},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s4c2.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p1c1.ibNode, o: &s1c1.ibNode},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s3c2.ibNode},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s4c2.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move and merge referenced node from tracked property merge candidate",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, s2c1, s3c1, p1c1 ibNodeString
					var s3c2, s4c2, p1c2 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &s2c1.typedResource},
							{p: &p1c2.ibNode, t: &s3c2.typedResource},
							{p: &p1c2.ibNode, t: &l1.typedResource},
							{p: &p1c2.ibNode, t: &s4c2.typedResource},
							{p: &p1c1.ibNode, t: &s1c1.typedResource},
							{p: &s2c1.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_10_00_00_00_00_01},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"s3": &s3c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					g1 := mockedTestGraph{
						namedGraph: c1,
					}
					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &g1.namedGraph,
							triplexStart: 0,
							triplexEnd:   4,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &g1.namedGraph,
							triplexStart: 4,
							triplexEnd:   5,
							flags:        iriNodeType | stringNode,
						},
					}
					s3c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &g1.namedGraph,
							triplexStart: 0,
							triplexEnd:   0,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s3",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &g1.namedGraph,
							triplexStart: 5,
							triplexEnd:   6,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &s1c1.typedResource},
							{p: &p1c2.ibNode, t: &s1c1.typedResource},
							{p: &s1c1.ibNode, t: &s3c2.typedResource},
							{p: &s1c1.ibNode, t: &l1.typedResource},
							{p: &s1c1.ibNode, t: &s4c2.typedResource},
						},
						triplexKinds: []uint{0b_10_10_10_01_01},
						stringNodes: map[string]*ibNodeString{
							"s3": &s3c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s3c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s3",
					}
					s4c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 2,
							triplexEnd:   5,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					g1 = newMockedGraphWithNodes(&c1, &s3c1.ibNode, &p1c1.ibNode)
					return args{
						s:         &s3c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s3",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s3c1.ibNode},
							}, nodeTriples(t, &s3c1.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move_and_merge_self_referenced_node_with_tracked_predicate",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &l1.typedResource},
						},
						triplexKinds: []uint{0b_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					var s1c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &s1c2.typedResource},
							{p: &p1c2.ibNode, t: &s1c2.typedResource},
							{p: &s1c2.ibNode, t: &s1c2.typedResource},
						},
						triplexKinds: []uint{0b_10_01_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 2,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &p1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move and merge tracked predicate for self referenced node",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1, p1c1 ibNodeString
					var p1c2 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &l1.typedResource},
							{p: &p1c2.ibNode, t: &s1c1.typedResource},
							{p: &p1c2.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_01_00_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					g1 := mockedTestGraph{
						namedGraph: c1,
					}
					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &g1.namedGraph,
							triplexStart: 0,
							triplexEnd:   3,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &g1.namedGraph,
							triplexStart: 0,
							triplexEnd:   0,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &s1c1.ibNode, t: &s1c1.typedResource},
						},
						triplexKinds: []uint{0b_10},
						stringNodes: map[string]*ibNodeString{
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					g1 = newMockedGraphWithNodes(&c1, &p1c1.ibNode)
					return args{
						s:         &p1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "p1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &p1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "copy_and_merge_self_referenced_node_with_tracked_predicate",
			test: func() (tt test) {
				tt = test{
					r: nodeCopyReplacements{},
					args: func() args {
						p := newTestProperty()
						var s1c1 ibNodeString

						c1 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p.ibNode, t: &l1.typedResource},
							},
							triplexKinds: []uint{0b_00},
							stringNodes: map[string]*ibNodeString{
								"s1": &s1c1,
							},
							flags: namedGraphFlags{trackPredicates: true},
						}
						s1c1 = ibNodeString{
							ibNode: ibNode{
								ng:           &c1,
								triplexStart: 0,
								triplexEnd:   1,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1",
						}
						var s1c2, p1c2 ibNodeString
						c2 := namedGraph{
							stage: st.(*stage),
							triplexStorage: []triplex{
								{p: &p1c2.ibNode, t: &s1c2.typedResource},
								{p: &p1c2.ibNode, t: &s1c2.typedResource},
								{p: &s1c2.ibNode, t: &s1c2.typedResource},
							},
							triplexKinds: []uint{0b_10_01_00},
							stringNodes: map[string]*ibNodeString{
								"s1": &s1c2,
								"p1": &p1c2,
							},
							flags: namedGraphFlags{trackPredicates: true},
						}
						s1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 0,
								triplexEnd:   2,
								flags:        iriNodeType | stringNode,
							},
							fragment: "s1",
						}
						p1c2 = ibNodeString{
							ibNode: ibNode{
								ng:           &c2,
								triplexStart: 2,
								triplexEnd:   3,
								flags:        iriNodeType | stringNode,
							},
							fragment: "p1",
						}

						return args{
							s:         &s1c2.ibNode,
							toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1}},
							fromC:     &c2,
							fromMatch: func(t *ibNode) bool { return t != nil && t.ng == &c2 },
							fragment:  "s1",
							stateAssertion: func(t assert.TestingT) {
								assert.ElementsMatch(t, []spo{
									{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
									{s: &s1c1.ibNode, p: tt.r[&p1c2.ibNode], o: &s1c1.ibNode},
								}, nodeTriples(t, &s1c1.ibNode))
								assert.ElementsMatch(t, []spo{
									{s: &s1c2.ibNode, p: &p1c2.ibNode, o: &s1c2.ibNode},
								}, nodeTriples(t, &p1c2.ibNode))
							},
						}
					}(),
					assertion: assert.NoError,
				}
				return
			}(),
		},
		{
			name: "move_and_merge_node_with_object_triplex",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					p := newTestProperty()
					var s1c1, s2c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &s2c1.typedResource},
							{p: &p.ibNode, t: &s1c1.typedResource},
						},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"s2": &s2c1,
						},
						triplexKinds: []uint{0b_00_01},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					s2c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s2",
					}
					var s1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p.ibNode, t: &l1.typedResource},
							{p: &p.ibNode, t: &s2c1.typedResource},
						},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
						},
						triplexKinds: []uint{0b_01_00},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p.ibNode, o: String("l1")},
								{s: &s2c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s2c1.ibNode, p: &p.ibNode, o: &s1c1.ibNode},
							}, nodeTriples(t, &s2c1.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move_and_merge_with_tracked_property_literal",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, p1c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &l1.typedResource},
							{p: &s1c1.ibNode, t: &l1.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:    &c1,
							flags: iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					l1c2a := literal{
						typedResource: typedResource{&literalTypeString.ibNode},
						value:         "l1",
					}
					l1c2b := literal{
						typedResource: typedResource{&literalTypeString.ibNode},
						value:         "l1",
					}
					var s1c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &l1c2a.typedResource},
							{p: &s1c2.ibNode, t: &l1c2b.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: String("l1")},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: String("l1")},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: String("l1")},
							}, nodeTriples(t, &p1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move_and_merge_with_tracked_property_lit_collection",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, p1c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &lc1.typedResource},
							{p: &s1c1.ibNode, t: &lc1.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					lc1c2a := literalCollection{
						typedResource: typedResource{&literalCollectionResourceType.ibNode},
						dataType:      &literalTypeLangString.ibNode,
						members:       []string{"m1", "m2"},
					}
					lc1c2b := literalCollection{
						typedResource: typedResource{&literalCollectionResourceType.ibNode},
						dataType:      &literalTypeLangString.ibNode,
						members:       []string{"m1", "m2"},
					}
					var s1c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &lc1c2a.typedResource},
							{p: &s1c2.ibNode, t: &lc1c2b.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &s1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "s1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &lc1},
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &lc1c2a},
							}, nodeTriples(t, &s1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c2.ibNode, o: &lc1},
							}, nodeTriples(t, &p1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move_and_merge_tracked_property_with_literal",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, p1c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &l1.typedResource},
							{p: &s1c1.ibNode, t: &l1.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					l1c2a := literal{
						typedResource: typedResource{&literalTypeString.ibNode},
						value:         "l1",
					}
					l1c2b := literal{
						typedResource: typedResource{&literalTypeString.ibNode},
						value:         "l1",
					}
					var s1c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &l1c2a.typedResource},
							{p: &s1c2.ibNode, t: &l1c2b.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &p1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "p1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: String("l1")},
								{s: &s1c2.ibNode, p: &p1c1.ibNode, o: String("l1")},
							}, nodeTriples(t, &p1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c2.ibNode, p: &p1c1.ibNode, o: String("l1")},
							}, nodeTriples(t, &s1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
		{
			name: "move_and_merge_tracked_property_with_lit_collection",
			test: test{
				r: nodeMoveReplacements(),
				args: func() args {
					var s1c1, p1c1 ibNodeString

					c1 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c1.ibNode, t: &lc1.typedResource},
							{p: &s1c1.ibNode, t: &lc1.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c1,
							"p1": &p1c1,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}

					s1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c1 = ibNodeString{
						ibNode: ibNode{
							ng:           &c1,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}
					lc1c2a := literalCollection{
						typedResource: typedResource{&literalCollectionResourceType.ibNode},
						dataType:      &literalTypeLangString.ibNode,
						members:       []string{"m1", "m2"},
					}
					lc1c2b := literalCollection{
						typedResource: typedResource{&literalCollectionResourceType.ibNode},
						dataType:      &literalTypeLangString.ibNode,
						members:       []string{"m1", "m2"},
					}
					var s1c2, p1c2 ibNodeString
					c2 := namedGraph{
						stage: st.(*stage),
						triplexStorage: []triplex{
							{p: &p1c2.ibNode, t: &lc1c2a.typedResource},
							{p: &s1c2.ibNode, t: &lc1c2b.typedResource},
						},
						triplexKinds: []uint{0b_10_00},
						stringNodes: map[string]*ibNodeString{
							"s1": &s1c2,
							"p1": &p1c2,
						},
						flags: namedGraphFlags{trackPredicates: true},
					}
					s1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 0,
							triplexEnd:   1,
							flags:        iriNodeType | stringNode,
						},
						fragment: "s1",
					}
					p1c2 = ibNodeString{
						ibNode: ibNode{
							ng:           &c2,
							triplexStart: 1,
							triplexEnd:   2,
							flags:        iriNodeType | stringNode,
						},
						fragment: "p1",
					}

					return args{
						s:         &p1c2.ibNode,
						toC:       map[*namedGraph]namedGraphsAndOffset{&c2: {c: &c1, offset: 0}},
						fromC:     &c2,
						fromMatch: nil,
						fragment:  "p1",
						stateAssertion: func(t assert.TestingT) {
							assert.ElementsMatch(t, []spo{
								{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &lc1},
								{s: &s1c2.ibNode, p: &p1c1.ibNode, o: &lc1c2b},
							}, nodeTriples(t, &p1c1.ibNode))
							assert.ElementsMatch(t, []spo{
								{s: &s1c2.ibNode, p: &p1c1.ibNode, o: &lc1c2a},
							}, nodeTriples(t, &s1c2.ibNode))
						},
					}
				}(),
				assertion: assert.NoError,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.s == nil {
				return
			}
			for fromC, toCOffset := range tt.args.toC {
				toCOffset.offset = copyTriplexesAndImports(toCOffset.c, fromC)
				tt.args.toC[fromC] = toCOffset
			}
			tt.r.mergeNodeToNamedGraph(tt.args.s, tt.args.toC, tt.args.fromMatch, tt.args.fragment)
			if tt.args.stateAssertion != nil {
				tt.args.stateAssertion(t)
			}
		})
	}
}

func Test_copyNodeToNamedGraph(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	type args struct {
		to *namedGraph
		t  *ibNode
	}
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "l1",
	}
	l2 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "l2",
	}
	type test struct {
		args           args
		stateAssertion func(assert.TestingT)
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "copy_node_with_inverse_right",
			test: func() (tt test) {
				p := newTestProperty()
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: []uint{0b_01_00},
				}
				var s1c2, s2c2, s3c2 ibNodeString
				c2 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p.ibNode, t: &s1c2.typedResource},
						{p: &p.ibNode, t: &s2c2.typedResource},
						{p: &p.ibNode, t: &l1.typedResource},
						{p: &p.ibNode, t: &s3c2.typedResource},
						{p: &p.ibNode, t: &l2.typedResource},
						{p: &p.ibNode, t: &s1c2.typedResource},
					},
					triplexKinds: []uint{0b_01_00_00_01},
				}
				s1c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 1,
						triplexEnd:   5,
						flags:        iriNodeType | stringNode,
					},
				}
				s2c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
				}
				s3c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 5,
						triplexEnd:   6,
						flags:        iriNodeType | stringNode,
					},
				}
				stateAssertion := func(t assert.TestingT) {
					assert.ElementsMatch(t, []spo{
						{s: &s1c2.ibNode, p: &p.ibNode, o: &s2c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l1")},
						{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l2")},
					}, nodeTriples(t, &s1c2.ibNode))
					assert.Equal(t, []uint{0b_01_00_00_00_00_01_00}, c1.triplexKinds)
				}
				tt = test{
					args: args{
						to: &c1,
						t:  &s1c2.ibNode,
					},
					stateAssertion: stateAssertion,
				}
				return
			}(),
		},
		{
			name: "copy_node_with_inverse_left",
			test: func() (tt test) {
				p := newTestProperty()
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: []triplex{},
					triplexKinds:   []uint{0b_00},
				}
				var s1c2, s2c2, s3c2 ibNodeString
				c2 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p.ibNode, t: &s1c2.typedResource},
						{p: &p.ibNode, t: &s2c2.typedResource},
						{p: &p.ibNode, t: &l1.typedResource},
						{p: &p.ibNode, t: &s3c2.typedResource},
						{p: &p.ibNode, t: &l2.typedResource},
						{p: &p.ibNode, t: &s1c2.typedResource},
					},
					triplexKinds: []uint{0b_01_00_00_01},
				}
				s1c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 1,
						triplexEnd:   5,
						flags:        iriNodeType | stringNode,
					},
				}
				s2c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
				}
				s3c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 5,
						triplexEnd:   6,
						flags:        iriNodeType | stringNode,
					},
				}
				stateAssertion := func(t assert.TestingT) {
					assert.ElementsMatch(t, []spo{
						{s: &s1c2.ibNode, p: &p.ibNode, o: &s2c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l1")},
						{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l2")},
					}, nodeTriples(t, &s1c2.ibNode))
					assert.Equal(t, []uint{0b_01_00_00}, c1.triplexKinds)
				}
				tt = test{
					args: args{
						to: &c1,
						t:  &s1c2.ibNode,
					},
					stateAssertion: stateAssertion,
				}
				return
			}(),
		},
		{
			name: "copy_node_on_uint_edge",
			test: func() (tt test) {
				p := newTestProperty()
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: make([]triplex, bits.UintSize/4),
					triplexKinds:   nil,
				}
				var s1c2, s2c2, s3c2 ibNodeString
				c2 := namedGraph{
					stage: st.(*stage),
					triplexStorage: append(make([]triplex, bits.UintSize-5),
						triplex{p: &p.ibNode, t: &s1c2.typedResource},
						triplex{p: &p.ibNode, t: &s2c2.typedResource},
						triplex{p: &p.ibNode, t: &l1.typedResource},
						triplex{p: &p.ibNode, t: &s3c2.typedResource},
						triplex{p: &p.ibNode, t: &l2.typedResource},
						triplex{p: &p.ibNode, t: &s1c2.typedResource},
					),
					triplexKinds: []uint{0b0, 0b_00_01_00_00_01 << (bits.UintSize - 10)},
				}
				s1c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize - 4,
						triplexEnd:   bits.UintSize,
						flags:        iriNodeType | stringNode,
					},
				}
				s2c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize - 5,
						triplexEnd:   bits.UintSize - 4,
						flags:        iriNodeType | stringNode,
					},
				}
				s3c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize,
						triplexEnd:   bits.UintSize + 1,
						flags:        iriNodeType | stringNode,
					},
				}
				stateAssertion := func(t assert.TestingT) {
					assert.ElementsMatch(t, []spo{
						{s: &s1c2.ibNode, p: &p.ibNode, o: &s2c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l1")},
						{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l2")},
					}, nodeTriples(t, &s1c2.ibNode))
					assert.Equal(t, []uint{0b_01_00_00 << (bits.UintSize / 2)}, c1.triplexKinds)
				}
				tt = test{
					args: args{
						to: &c1,
						t:  &s1c2.ibNode,
					},
					stateAssertion: stateAssertion,
				}
				return
			}(),
		},
		{
			name: "copy_node_crossing_uint_complete",
			test: func() (tt test) {
				p := newTestProperty()
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: make([]triplex, bits.UintSize/3),
					triplexKinds:   nil,
				}
				var s1c2, s2c2, s3c2 ibNodeString
				type tx = triplex
				c2 := namedGraph{
					stage: st.(*stage),
					triplexStorage: append(make([]tx, bits.UintSize-4),
						tx{p: &p.ibNode, t: &s1c2.typedResource},
						tx{p: &p.ibNode, t: &s2c2.typedResource},
						tx{p: &p.ibNode, t: &l1.typedResource},
						tx{p: &p.ibNode, t: &s3c2.typedResource},
						tx{p: &p.ibNode, t: &l2.typedResource},
						tx{p: &p.ibNode, t: &s1c2.typedResource},
					),
					triplexKinds: []uint{0, 0b_01_00_00_01 << (bits.UintSize - 8), 0},
				}
				s1c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize - 3,
						triplexEnd:   bits.UintSize + 1,
						flags:        iriNodeType | stringNode,
					},
				}
				s2c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize - 4,
						triplexEnd:   bits.UintSize - 3,
						flags:        iriNodeType | stringNode,
					},
				}
				s3c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize + 1,
						triplexEnd:   bits.UintSize + 2,
						flags:        iriNodeType | stringNode,
					},
				}
				stateAssertion := func(t assert.TestingT) {
					assert.ElementsMatch(t, []spo{
						{s: &s1c2.ibNode, p: &p.ibNode, o: &s2c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l1")},
						{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l2")},
					}, nodeTriples(t, &s1c2.ibNode))
					assert.Equal(t, []uint{0b01_00_00 << (bits.UintSize / 3 * 2)}, c1.triplexKinds)
				}
				tt = test{
					args: args{
						to: &c1,
						t:  &s1c2.ibNode,
					},
					stateAssertion: stateAssertion,
				}
				return
			}(),
		},
		{
			name: "copy_node_crossing_uint_incomplete",
			test: func() (tt test) {
				p := newTestProperty()
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: make([]triplex, bits.UintSize/3),
					triplexKinds:   nil,
				}
				var s1c2, s2c2, s3c2 ibNodeString
				type tx = triplex
				c2 := namedGraph{
					stage: st.(*stage),
					triplexStorage: append(make([]tx, bits.UintSize-4),
						tx{p: &p.ibNode, t: &s1c2.typedResource},
						tx{p: &p.ibNode, t: &s2c2.typedResource},
						tx{p: &p.ibNode, t: &l1.typedResource},
						tx{p: &p.ibNode, t: &s3c2.typedResource},
						tx{p: &p.ibNode, t: &l2.typedResource},
						tx{p: &p.ibNode, t: &s1c2.typedResource},
					),
					triplexKinds: []uint{0, 0b_01_00_00_01 << (bits.UintSize - 8)},
				}
				s1c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize - 3,
						triplexEnd:   bits.UintSize + 1,
						flags:        iriNodeType | stringNode,
					},
				}
				s2c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize - 4,
						triplexEnd:   bits.UintSize - 3,
						flags:        iriNodeType | stringNode,
					},
				}
				s3c2 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: bits.UintSize + 1,
						triplexEnd:   bits.UintSize + 2,
						flags:        iriNodeType | stringNode,
					},
				}
				stateAssertion := func(t assert.TestingT) {
					assert.ElementsMatch(t, []spo{
						{s: &s1c2.ibNode, p: &p.ibNode, o: &s2c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l1")},
						{s: &s3c2.ibNode, p: &p.ibNode, o: &s1c2.ibNode},
						{s: &s1c2.ibNode, p: &p.ibNode, o: String("l2")},
					}, nodeTriples(t, &s1c2.ibNode))
					assert.Equal(t, []uint{0b_01_00_00 << ((bits.UintSize / 3) * 2)}, c1.triplexKinds)
				}
				tt = test{
					args: args{
						to: &c1,
						t:  &s1c2.ibNode,
					},
					stateAssertion: stateAssertion,
				}
				return
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copyNodeToNamedGraph(tt.args.to, tt.args.t)
			tt.stateAssertion(t)
		})
	}
}

type spo struct {
	s, p *ibNode
	o    Term
}

func nodeTriples(t assert.TestingT, st *ibNode) []spo {
	var stSpo []spo
	assert.NoError(t, st.forAll(func(_ int, s, p *ibNode, object Term) error {
		stSpo = append(stSpo, spo{s: s, p: p, o: object})
		return nil
	}))
	return stSpo
}

func TestLiteral_DataType(t *testing.T) {
	assert.Equal(t, TermKindLiteral, literal{}.TermKind())
}

func TestLiteral_Value(t *testing.T) {
	xmlLiteralType := ibNodeString{
		fragment: "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral",
		ibNode:   ibNode{flags: iriNodeType | stringNode},
	}
	tests := []struct {
		name    string
		literal literal
		want    interface{}
	}{
		{
			name: "string",
			literal: literal{
				typedResource: typedResource{
					typeOf: &literalTypeString.ibNode,
				},
				value: "string value",
			},
			want: String("string value"),
		},
		{
			name: "double",
			literal: literal{
				typedResource: typedResource{
					typeOf: &literalTypeDouble.ibNode,
				},
				value: float64Bytes(11.1),
			},
			want: Double(11.1),
		},
		{
			name: "xml-literal",
			literal: literal{
				typedResource: typedResource{
					typeOf: &xmlLiteralType.ibNode,
				},
				value: "<literal></literal>",
			},
			want: "<literal></literal>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.literal.apiValue())
		})
	}
}

func TestNewIBNode(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	g := namedGraph{
		stage: st.(*stage)}
	t1 := ibNode{
		typedResource: typedResource{
			typeOf: nil,
		},
		ng:           &g,
		triplexStart: 0,
		triplexEnd:   0,
		flags:        iriNodeType | stringNode,
	}
	type args struct {
		graph    namedGraph
		fragment string
		typeOf   *ibNode
	}
	tests := []struct {
		name string
		args args
		want *ibNode
	}{
		{
			name: "simple",
			args: args{
				graph:    namedGraph{stage: st.(*stage)},
				fragment: "",
				typeOf:   nil,
			},
			want: &t1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := newIBNodeStringAsIBNode(&tt.args.graph, tt.args.fragment, tt.args.typeOf)
			assert.Equal(t, tt.want, tree)
		})
	}
}

func newIBNodeStringAsIBNode(graph *namedGraph, fragment string, typeOf *ibNode) *ibNode {
	return &newStringNode(graph, fragment, typeOf, stringNode|iriNodeType).ibNode
}

func TestIBNode_ObjectKind(t *testing.T) {
	tree := ibNode{}
	assert.Equal(t, TermKindIBNode, tree.TermKind())
}

func TestIBNode_Graph(t *testing.T) {
	t.Skip("TODO: Implement")
	type fields struct {
		typedResource typedResource
		ng            *namedGraph
		triplexStart  triplexOffset
		triplexEnd    triplexOffset
	}
	tests := []struct {
		name   string
		fields fields
		want   namedGraph
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &ibNode{
				typedResource: tt.fields.typedResource,
				ng:            tt.fields.ng,
				// fragment:        tt.fields.fragment,
				triplexStart: tt.fields.triplexStart,
				triplexEnd:   tt.fields.triplexEnd,
			}
			assert.Equal(t, tt.want, tr.OwningGraph())
		})
	}
}
func Test_NewIRI(t *testing.T) {
	tests := []struct {
		in      string
		isValid bool
	}{
		// ----- Valid IRI -----
		{"http://example.org/", true},
		{"https://example.org/path/to/page", true},
		{"https://example.org/路径/中文", true}, // Unicode path (RFC 3987)
		{"http://例子.测试/页面#片段", true},        // Unicode domain + fragment
		{"ftp://ftp.example.com/pub/file.txt", true},
		// {"mailto:alice@example.com", true},
		{"data:text/plain;charset=UTF-8,Hello%20World", true},

		// ----- Valid IRI with fragment -----
		{"https://example.com/a/b/c?query=1#frag", true},

		// ----- Valid URN -----
		{"urn:uuid:123e4567-e89b-12d3-a456-426614174000", true},
		{"urn:isbn:9780306406157", true},
		{"urn:example:animal:ferret:nose", true},                     // Generic NID/NSS example
		{"urn:uuid:123e4567-e89b-12d3-a456-426614174000#main", true}, // URN with fragment

		// ----- URN edge cases -----
		{"urn:uuid:ABCDEFAB-CDEF-1234-5678-ABCD12345678", true}, // Uppercase UUID allowed
		{"urn:example:Hello-World_123", true},                   // Valid NSS characters

		// ----- Invalid scheme -----
		{"1http://example.org", false},     // Scheme cannot start with a digit
		{"-http://example.org", false},     // Scheme cannot start with a symbol
		{"http//missing-colon.com", false}, // Missing colon
		{"noscheme", false},                // No scheme at all

		// ----- Invalid IRI -----
		{"http://exa mple.org/", false},   // Space is not allowed
		{"http://example.org/%ZZ", false}, // Invalid escape sequence
		// {"http://", false},                // Incomplete IRI

		// ----- Invalid URN -----
		{"urn:", false},                    // Missing NID and NSS
		{"urn:uuid", false},                // Missing NSS
		{"urn:uuid:1234 5678", false},      // Space inside NSS is invalid
		{"urn:uuid:1234#frag ment", false}, // Fragment contains whitespace
		{"urn::missingnid", false},         // Empty NID
	}

	for _, tt := range tests {
		_, err := NewIRI(tt.in)
		if tt.isValid && err != nil {
			t.Errorf("expected VALID but got error: input=%q err=%v", tt.in, err)
		}
		if !tt.isValid && err == nil {
			t.Errorf("expected INVALID but got success: input=%q", tt.in)
		}
	}
}

func TestIBNode_fragment(t *testing.T) {
	t.Skip("TODO: Implement")
	type fields struct {
		typedResource typedResource
		ng            *namedGraph
		triplexStart  triplexOffset
		triplexEnd    triplexOffset
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &ibNode{
				typedResource: tt.fields.typedResource,
				ng:            tt.fields.ng,
				// fragment:        tt.fields.fragment,
				triplexStart: tt.fields.triplexStart,
				triplexEnd:   tt.fields.triplexEnd,
			}
			got := tr.Fragment()
			assert.Equal(t, tt.want, got)
		})
	}
}

func asIRI(iri string) IRI {
	asIRI, err := NewIRI(iri)
	if err != nil {
		panic(err)
	}
	return asIRI
}

func TestIBNode_ResourceID(t *testing.T) {
	graph := namedGraph{}
	graph.baseIRI = "http://example.com/base"
	graphUUID := namedGraph{}
	graphUUID.baseIRI = "urn:uuid:b9525759-8f0b-48c8-8c41-82b1bb716cc4"
	type fields struct {
		typedResource typedResource
		ng            *namedGraph
		fragment      string
		triplexStart  triplexOffset
		triplexEnd    triplexOffset
	}
	tests := []struct {
		name   string
		fields fields
		want   IRI
	}{
		{
			name: "iri_node",
			fields: fields{
				ng:       &graph,
				fragment: "main",
			},
			want: asIRI("http://example.com/base#main"),
		},
		{
			name: "node_for_self",
			fields: fields{
				ng:       &graph,
				fragment: "",
			},
			want: asIRI("http://example.com/base"),
		},
		{
			name: "id_iri_node",
			fields: fields{
				ng:       &graphUUID,
				fragment: "main",
			},
			want: asIRI("urn:uuid:b9525759-8f0b-48c8-8c41-82b1bb716cc4#main"),
		},
		{
			name: "id_node_for_self",
			fields: fields{
				ng:       &graphUUID,
				fragment: "",
			},
			want: asIRI("urn:uuid:b9525759-8f0b-48c8-8c41-82b1bb716cc4"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &ibNodeString{
				ibNode: ibNode{
					typedResource: tt.fields.typedResource,
					ng:            tt.fields.ng,
					triplexStart:  tt.fields.triplexStart,
					triplexEnd:    tt.fields.triplexEnd,
					flags:         iriNodeType | stringNode,
				},
				fragment: tt.fields.fragment,
			}
			assert.Equal(t, tt.want, tr.iri())

		})
	}
}

func TestIBNode_TripleCount(t *testing.T) {
	type fields struct {
		typedResource typedResource
		ng            *namedGraph
		triplexStart  triplexOffset
		triplexEnd    triplexOffset
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &ibNode{
				typedResource: tt.fields.typedResource,
				ng:            tt.fields.ng,
				// fragment:        tt.fields.fragment,
				triplexStart: tt.fields.triplexStart,
				triplexEnd:   tt.fields.triplexEnd,
			}
			assert.Equal(t, tt.want, tr.TripleCount())
		})
	}
}

func TestIBNode_ForAll(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	graph := namedGraph{}
	t1 := ibNodeString{ibNode: ibNode{flags: iriNodeType | stringNode}, fragment: "http://semanticstep.net/sst#t1"}
	t2 := ibNodeString{ibNode: ibNode{flags: iriNodeType | stringNode}, fragment: "http://semanticstep.net/sst#t2"}
	p1 := newIBNodeStringAsIBNode(&graph, "part1", &t1.ibNode)
	termColMustOK := func(c TermCollection, ok bool) TermCollection {
		if !ok {
			assert.FailNow(t, "termColMust")
		}
		return c
	}
	l1 := literal{
		typedResource: typedResource{
			typeOf: &literalTypeString.ibNode,
		},
		value: "part value",
	}
	l2 := literal{
		typedResource: typedResource{
			typeOf: &literalTypeDouble.ibNode,
		},
		value: float64Bytes(11.1),
	}
	toUintSlice := func(b uint64) []uint {
		var e1 uint
		if bits.UintSize < 64 {
			e1 = uint((b >> bits.UintSize) & uintAllMask) //nolint:staticcheck
		}
		return []uint{uint(b & uintAllMask), e1}
	}
	graph = namedGraph{
		stage: st.(*stage),
		triplexStorage: []triplex{
			{
				p: p1,
				t: nil,
			},
			{},
			{},
			{},

			{},
			{
				p: p1,
				t: nil,
			},
			{},
			{},

			{},
			{
				p: p1,
				t: nil,
			},
			{},
			{},

			{
				p: p1,
				t: nil,
			},
			{},
			{},
			{},

			{
				p: p1,
				t: &l1.typedResource,
			},
			{
				p: p1,
				t: &l2.typedResource,
			},
			{},
			{},

			{p: &rdfFirstProperty.ibNode, t: nil},
			{p: &rdfFirstProperty.ibNode, t: nil},
			{p: p1, t: nil},
			{p: p1, t: nil},

			{p: &rdfFirstProperty.ibNode, t: nil},
			{p: p1, t: nil},
			{p: p1, t: nil},
			{p: p1, t: nil},
			{p: &rdfFirstProperty.ibNode, t: nil},
			{p: p1, t: nil},
		},
		triplexKinds: toUintSlice((1 << ((4 + 1) * 2)) | (1 << ((8 + 1) * 2)) |
			(1 << ((20 + 0) * 2)) | (1 << ((20 + 2) * 2)) | (1 << ((24 + 2) * 2)) | (1 << ((24 + 4) * 2)) | (1 << ((24 + 5) * 2))),
	}
	type fields struct {
		typedResource typedResource
		ng            *namedGraph
		fragment      string
		triplexStart  triplexOffset
		triplexEnd    triplexOffset
	}
	type args struct {
		c func(t *testing.T, index int, s, p IBNode, object Term) error
	}
	test1Cnt := 0
	test2Cnt := 0
	test3Cnt := 0
	test4Cnt := 0
	test5Cnt := 0
	test6Cnt := 0
	testTrees := map[string][]ibNode{}
	tests := []struct {
		name      string
		allFields []fields
		fixtures  []func([]ibNode)
		args      args
		assertion assert.ErrorAssertionFunc
		t         *testing.T
	}{
		{
			name: "iri node",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "main",
					triplexStart: 0,
					triplexEnd:   4,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "orig",
					triplexStart: 4,
					triplexEnd:   8,
				},
			},
			fixtures: []func([]ibNode){
				func(trees []ibNode) {
					graph.triplexStorage[0*4+0].t = &trees[1].typedResource
					graph.triplexStorage[1*4+1].t = &trees[0].typedResource
				},
			},
			args: args{
				c: func(t *testing.T, index int, s, p IBNode, object Term) error {
					assert.Equal(t, 0, index)
					assert.Equal(t, &testTrees["iri node"][0], s)
					assert.Equal(t, p1, p)
					assert.Equal(t, &testTrees["iri node"][1], object)
					test1Cnt++
					return nil
				},
			},
			assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
				if assert.NoError(tt, err) {
					assert.Equal(tt, 1, test1Cnt)
					assert.Equal(tt, 0, test2Cnt+test3Cnt)
					return true
				}
				return false
			},
		},
		{
			name: "hole and inverse iri node",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "orig",
					triplexStart: 8,
					triplexEnd:   12,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "main",
					triplexStart: 12,
					triplexEnd:   16,
				},
			},
			fixtures: []func([]ibNode){
				func(trees []ibNode) {
					graph.triplexStorage[2*4+1].t = &trees[1].typedResource
					graph.triplexStorage[3*4+0].t = &trees[0].typedResource
				},
			},
			args: args{
				c: func(t *testing.T, index int, s, p IBNode, object Term) error {
					assert.Equal(t, 1, index)
					assert.Equal(t, &testTrees["hole and inverse iri node"][1], s)
					assert.Equal(t, p1, p)
					assert.Equal(t, &testTrees["hole and inverse iri node"][0], object)
					test2Cnt++
					return nil
				},
			},
			assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
				if assert.NoError(tt, err) {
					assert.Equal(tt, 1, test2Cnt)
					return true
				}
				return false
			},
		},
		{
			name: "literals",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "main",
					triplexStart: 16,
					triplexEnd:   20,
				},
			},
			args: args{
				c: func(t *testing.T, index int, s, p IBNode, object Term) error {
					assert.Equal(t, test3Cnt, index)
					assert.Equal(t, &testTrees["literals"][0], s)
					assert.Equal(t, p1, p)
					if assert.Equal(t, TermKindLiteral, object.TermKind()) {
						switch test3Cnt {
						case 0:
							assert.Equal(t, String("part value"), object.(Literal).apiValue())
						case 1:
							assert.Equal(t, Double(11.1), object.(Literal).apiValue())
						}
					}
					test3Cnt++
					return nil
				},
			},
			assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
				if assert.NoError(tt, err) {
					assert.Equal(tt, 2, test3Cnt)
					return true
				}
				return false
			},
		},
		{
			name: "node used by collection",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 20 + 0,
					triplexEnd:   20 + 1,
				},
				{
					typedResource: typedResource{
						typeOf: &termCollectionResourceType.ibNode,
					},
					ng:           &graph,
					triplexStart: 20 + 1,
					triplexEnd:   20 + 3,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 20 + 3,
					triplexEnd:   20 + 4,
				},
			},
			fixtures: []func([]ibNode){
				func(trees []ibNode) {
					graph.triplexStorage[20+0].t = &trees[1].typedResource
					graph.triplexStorage[20+1].t = &trees[0].typedResource
					graph.triplexStorage[20+2].t = &trees[2].typedResource
					graph.triplexStorage[20+3].t = &trees[1].typedResource
				},
			},
			args: args{
				c: func(t *testing.T, index int, s, p IBNode, object Term) error {
					assert.Equal(t, 0, index)
					assert.Equal(t, &testTrees["node used by collection"][1], s)
					assert.Equal(t, &rdfFirstProperty.ibNode, p)
					assert.Equal(t, &testTrees["node used by collection"][0], object)
					test4Cnt++
					return nil
				},
			},
			assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
				if assert.NoError(tt, err) {
					assert.Equal(tt, 1, test4Cnt)
					return true
				}
				return false
			},
		},
		{
			name: "collection with extra triple",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &termCollectionResourceType.ibNode,
					},
					ng:           &graph,
					triplexStart: 24 + 0,
					triplexEnd:   24 + 3,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 24 + 3,
					triplexEnd:   24 + 4,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 24 + 4,
					triplexEnd:   24 + 5,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 24 + 5,
					triplexEnd:   24 + 6,
				},
			},
			fixtures: []func([]ibNode){
				func(trees []ibNode) {
					graph.triplexStorage[24+0].t = &trees[2].typedResource
					graph.triplexStorage[24+1].t = &trees[3].typedResource
					graph.triplexStorage[24+2].t = &trees[1].typedResource
					graph.triplexStorage[24+3].t = &trees[0].typedResource
					graph.triplexStorage[24+4].t = &trees[0].typedResource
					graph.triplexStorage[24+5].t = &trees[0].typedResource
				},
			},
			args: args{
				c: func(t *testing.T, index int, s, p IBNode, object Term) error {
					assert.GreaterOrEqual(t, index, 0)
					assert.Less(t, index, 3)
					switch index {
					case 0:
						s, ok := s.AsCollection()
						assert.True(t, ok)
						assert.Equal(t, termColMustOK(testTrees["collection with extra triple"][0].AsCollection()), s)
						assert.Equal(t, &rdfFirstProperty.ibNode, p)
						assert.Equal(t, testTrees["collection with extra triple"][2], *object.(*ibNode))
					case 1:
						s, ok := s.AsCollection()
						assert.True(t, ok)
						assert.Equal(t, termColMustOK(testTrees["collection with extra triple"][0].AsCollection()), s)
						assert.Equal(t, p1, p)
						assert.Equal(t, testTrees["collection with extra triple"][3], *object.(*ibNode))
					case 2:
						assert.Equal(t, testTrees["collection with extra triple"][1], *s.(*ibNode))
						assert.Equal(t, p1, p)
						assert.Equal(t, testTrees["collection with extra triple"][0], *object.(*ibNode))
					}
					test5Cnt++
					return nil
				},
			},
			assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
				if assert.NoError(tt, err) {
					assert.Equal(tt, 3, test5Cnt)
					return true
				}
				return false
			},
		},
		{
			name: "node referencing collection",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 20 + 3,
					triplexEnd:   20 + 4,
				},
				{
					typedResource: typedResource{
						typeOf: &termCollectionResourceType.ibNode,
					},
					ng:           &graph,
					triplexStart: 20 + 1,
					triplexEnd:   20 + 3,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					triplexStart: 20 + 0,
					triplexEnd:   20 + 1,
				},
			},
			fixtures: []func([]ibNode){
				func(trees []ibNode) {
					graph.triplexStorage[20+0].t = &trees[1].typedResource
					graph.triplexStorage[20+1].t = &trees[0].typedResource
					graph.triplexStorage[20+2].t = &trees[2].typedResource
					graph.triplexStorage[20+3].t = &trees[1].typedResource
				},
			},
			args: args{
				c: func(t *testing.T, index int, s, p IBNode, object Term) error {
					assert.Equal(t, 0, index)
					assert.Equal(t, &testTrees["node referencing collection"][0], s)
					assert.Equal(t, p1, p)
					assert.Equal(t, &testTrees["node referencing collection"][1], object)
					test6Cnt++
					return nil
				},
			},
			assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
				if assert.NoError(tt, err) {
					assert.Equal(tt, 1, test6Cnt)
					return true
				}
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trees := make([]ibNode, 0, len(tt.allFields))
			for _, fields := range tt.allFields {
				tr := ibNode{
					typedResource: fields.typedResource,
					ng:            fields.ng,
					// fragment:        fields.fragment,
					triplexStart: fields.triplexStart,
					triplexEnd:   fields.triplexEnd,
				}
				trees = append(trees, tr)
			}
			testTrees[tt.name] = trees
			for _, fixture := range tt.fixtures {
				fixture(trees)
			}
			if assert.NotEmpty(t, trees) {
				tt.assertion(t,
					trees[0].ForAll(func(index int, s, p IBNode, object Term) error {
						return tt.args.c(t, index, s, p, object)
					}))
			}
		})
	}
}

func TestIBNode_AddAll(t *testing.T) {
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "l1",
	}
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		c func() (p *ibNode, object Term, ok bool)
	}
	type test struct {
		fields    fields
		args      args
		assertion func(t assert.TestingT) bool
	}
	staticTestStage := stage{}
	tests := []struct {
		name string
		test
	}{
		{
			name: "collection first property fails",
			test: func() test {
				var s1c1, s2c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage:          &staticTestStage,
					triplexStorage: []triplex{},
					triplexKinds:   nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				g1 = c1
				var cIndex int
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						c: func() (p *ibNode, object Term, ok bool) {
							if cIndex == 0 {
								p = &rdfFirstProperty.ibNode
								object = &s2c1.ibNode
								ok = true
							}
							cIndex++
							return
						},
					},
					assertion: func(t assert.TestingT) bool {
						// return assert.ErrorIs(t, ErrCannotSetCollectionMemberPredicate)
						return true
					},
				}
			}(),
		},
		{
			name: "tracked property with node to literal",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
					},
					triplexKinds: []uint{0b_10_01_00},
					flags:        namedGraphFlags{trackPredicates: true},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = c1
				var cIndex int
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						c: func() (p *ibNode, object Term, ok bool) {
							if cIndex == 0 {
								p = &p1c1.ibNode
								object = &l1
								ok = true
							}
							cIndex++
							return
						},
					},
					assertion: func(t assert.TestingT) bool {
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: String("l1")},
						}, nodeTriples(t, &s1c1.ibNode))
						assert.ElementsMatch(t, []spo{}, nodeTriples(t, &s2c1.ibNode))
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: String("l1")},
						}, nodeTriples(t, &p1c1.ibNode))
						return true
					},
				}
			}(),
		},
		{
			name: "tracked property with literal to node",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_10_00},
					flags:        namedGraphFlags{trackPredicates: true},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = c1
				var cIndex int
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						c: func() (p *ibNode, o Term, ok bool) {
							if cIndex == 0 {
								p = &p1c1.ibNode
								o = &s2c1.ibNode
								ok = true
							}
							cIndex++
							return
						},
					},
					assertion: func(t assert.TestingT) bool {
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s2c1.ibNode},
						}, nodeTriples(t, &s1c1.ibNode))
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s2c1.ibNode},
						}, nodeTriples(t, &s2c1.ibNode))
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &s2c1.ibNode},
						}, nodeTriples(t, &p1c1.ibNode))
						return true
					},
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tt.fields.ibNode

			if tt.name == "collection first property fails" {
				assert.Panics(t, func() {
					tr.DeleteTriples()
					tr.addAll(tt.args.c, false)
					assert.True(t, tt.assertion(t))
				})
			} else {
				tr.DeleteTriples()
				tr.addAll(tt.args.c, false)
				assert.True(t, tt.assertion(t))
			}
		})
	}
}

func TestIBNode_AddAll2(t *testing.T) {
	graph := namedGraph{}
	t1 := ibNodeString{ibNode: ibNode{flags: iriNodeType | stringNode}, fragment: "http://semanticstep.net/sst#t1"}
	t2 := ibNodeString{ibNode: ibNode{flags: iriNodeType | stringNode}, fragment: "http://semanticstep.net/sst#t2"}
	p1 := newIBNodeStringAsIBNode(&graph, "part1", &t1.ibNode)
	p2 := newIBNodeStringAsIBNode(&graph, "part2", &t1.ibNode)
	l1 := literal{
		typedResource: typedResource{
			typeOf: &literalTypeString.ibNode,
		},
		value: "part value",
	}
	staticStage := stage{}
	graph = namedGraph{
		stage: &staticStage,
		triplexStorage: []triplex{
			{
				p: p1,
				t: nil,
			},
			{},
			{},
			{},

			{
				p: p1,
				t: nil,
			},
			{},
			{},
			{},

			{
				p: nil,
				t: nil,
			},
			{
				p: p2,
				t: &l1.typedResource,
			},
			{},
			{},
		},
		triplexKinds: []uint{1 << ((4 + 0) * 2)},
	}
	type fields struct {
		typedResource typedResource
		ng            *namedGraph
		fragment      string
		triplexStart  triplexOffset
		triplexEnd    triplexOffset
	}
	type args struct {
		c func() (p *ibNode, o Term, ok bool)
	}
	test1Cnt := 0
	// test2Cnt := 0
	// test3Cnt := 0
	var testTrees [][]ibNode
	tests := []struct {
		name           string
		allFields      []fields
		fixtures       []func([]ibNode)
		args           args
		assertion      assert.ErrorAssertionFunc
		extraAssertion func(*testing.T, []ibNode)
	}{
		{
			name: "another node",
			allFields: []fields{
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "main",
					triplexStart: 0,
					triplexEnd:   4,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "orig",
					triplexStart: 4,
					triplexEnd:   8,
				},
				{
					typedResource: typedResource{
						typeOf: &t2.ibNode,
					},
					ng:           &graph,
					fragment:     "set",
					triplexStart: 8,
					triplexEnd:   12,
				},
			},
			fixtures: []func([]ibNode){
				func(trees []ibNode) {
					graph.triplexStorage[0*4+0].t = &trees[1].typedResource
					graph.triplexStorage[1*4+0].t = &trees[0].typedResource
				},
			},
			args: args{
				c: func() (p *ibNode, o Term, ok bool) {
					defer func() { test1Cnt++ }()
					if test1Cnt < 1 {
						p = p1
						o = &testTrees[0][2]
						ok = true
					}
					return
				},
			},
			assertion: assert.NoError,
			extraAssertion: func(t *testing.T, trees []ibNode) {
				assert.Equal(t, triplexOffset(8), trees[2].triplexStart)
				assert.Equal(t, triplexOffset(12), trees[2].triplexEnd)
				assert.Equal(t, []triplex{
					{
						p: p1,
						t: &trees[0].typedResource,
					},
					{
						p: p2,
						t: &l1.typedResource,
					},
					{},
					{},
				}, graph.triplexStorage[8:12])
				assert.Equal(t, []uint{1 << ((8 + 0) * 2)}, graph.triplexKinds)
			},
		},
		// {
		// 	name: "duplicated literal",
		// 	allFields: []fields{
		// 		{
		// 			typedResource: typedResource{
		// 				typeOf: &t2.ibNode,
		// 			},
		// 			ng:       &graph,
		// 			fragment: "main",
		// 		},
		// 	},
		// 	args: args{
		// 		c: func() (p *ibNode, o Object, ok bool) {
		// 			defer func() { test2Cnt++ }()
		// 			if test2Cnt < 2 {
		// 				p = p2
		// 				o = String("literal value")
		// 				ok = true
		// 			}
		// 			return
		// 		},
		// 	},
		// 	assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
		// 		if assert.Error(tt, err) {
		// 			assert.ErrorIs(tt, err, ErrDuplicatePropertyObjectPair)
		// 			return true
		// 		}
		// 		return false
		// 	},
		// 	extraAssertion: func(*testing.T, []ibNode) {},
		// },
		// {
		// 	name: "duplicated node",
		// 	allFields: []fields{
		// 		{
		// 			typedResource: typedResource{
		// 				typeOf: &t2.ibNode,
		// 			},
		// 			ng:       &graph,
		// 			fragment: "main",
		// 		},
		// 		{
		// 			typedResource: typedResource{
		// 				typeOf: &t2.ibNode,
		// 			},
		// 			ng:       &graph,
		// 			fragment: "",
		// 		},
		// 	},
		// 	args: args{
		// 		c: func() (p *ibNode, o Object, ok bool) {
		// 			defer func() { test3Cnt++ }()
		// 			if test3Cnt < 2 {
		// 				p = p1
		// 				o = &testTrees[2][1]
		// 				ok = true
		// 			}
		// 			return
		// 		},
		// 	},
		// 	assertion: func(tt assert.TestingT, err error, _ ...interface{}) bool {
		// 		if assert.Error(tt, err) {
		// 			assert.ErrorIs(tt, err, ErrDuplicatePropertyObjectPair)
		// 			return true
		// 		}
		// 		return false
		// 	},
		// 	extraAssertion: func(t *testing.T, trees []ibNode) {
		// 		assert.Equal(t, triplexOffset(triplexAllocCount*2+12), trees[1].triplexStart)
		// 		assert.Equal(t, triplexOffset(triplexAllocCount*3+12), trees[1].triplexEnd)
		// 		expectedTriplexes := [triplexAllocCount]triplex{
		// 			{
		// 				p: p1,
		// 				t: &trees[0].typedResource,
		// 			},
		// 		}
		// 		assert.Equal(t, expectedTriplexes[:],
		// 			graph.triplexStorage[triplexAllocCount*2+12:triplexAllocCount*3+12])
		// 		assert.Equal(t, []uint{1<<((triplexAllocCount*2+12)*2) | 1<<((8+0)*2)}, graph.triplexKinds)
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trees := make([]ibNode, 0, len(tt.allFields))
			for _, fields := range tt.allFields {
				tr := ibNode{
					typedResource: fields.typedResource,
					ng:            fields.ng,
					triplexStart:  fields.triplexStart,
					triplexEnd:    fields.triplexEnd,
					flags:         iriNodeType | stringNode,
				}
				trees = append(trees, tr)
			}
			testTrees = append(testTrees, trees)
			for _, fixture := range tt.fixtures {
				fixture(trees)
			}
			if assert.NotEmpty(t, trees) {
				trees[0].DeleteTriples()
				trees[0].addAll(tt.args.c, false)
				tt.extraAssertion(t, trees)
			}
		})
	}
}

func TestIBNode_GetTriple(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "l1",
	}
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		index int
	}
	type test struct {
		fields    fields
		args      args
		wantS     IBNode
		wantP     IBNode
		wantO     Term
		assertion assert.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "forward reference to object list",
			test: func() test {
				p := newTestProperty()
				var s1c1, s3c1 ibNodeString
				var l2c1 ibNode
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p.ibNode, t: &l2c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource}, // inverse
						{p: &rdfFirstProperty.ibNode, t: &s3c1.typedResource},
						{p: &rdfFirstProperty.ibNode, t: &l2c1.typedResource}, // inverse
					},
					triplexKinds: []uint{0b_01_00_01_00},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				l2c1 = ibNode{
					ng:           &g1,
					triplexStart: 1,
					triplexEnd:   3,
					flags:        0,
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						index: 0,
					},
					wantS:     &s1c1.ibNode,
					wantP:     &p.ibNode,
					wantO:     &l2c1,
					assertion: assert.NoError,
				}
			}(),
		},
		{
			name: "inverse reference to object list",
			test: func() test {
				p := newTestProperty()
				var s1c1, s3c1 ibNodeString
				var l2c1 ibNode
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p.ibNode, t: &l2c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource}, // inverse
						{p: &rdfFirstProperty.ibNode, t: &s3c1.typedResource},
						{p: &rdfFirstProperty.ibNode, t: &l2c1.typedResource}, // inverse
					},
					triplexKinds: []uint{0b_01_00_01_00},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				l2c1 = ibNode{
					ng:            &c1,
					typedResource: typedResource{typeOf: &termCollectionResourceType.ibNode},
					triplexStart:  1,
					triplexEnd:    3,
					flags:         0,
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &s3c1.ibNode,
					},
					args: args{
						index: 0,
					},
					wantS:     &l2c1,
					wantP:     &rdfFirstProperty.ibNode,
					wantO:     &s3c1.ibNode,
					assertion: assert.NoError,
				}
			}(),
		},
		{
			name: "tracked property to node",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_01_00_00},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 3,
						triplexEnd:   5,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &p1c1.ibNode,
					},
					args: args{
						index: 0,
					},
					wantS:     &s1c1.ibNode,
					wantP:     &p1c1.ibNode,
					wantO:     &s2c1.ibNode,
					assertion: assert.NoError,
				}
			}(),
		},
		{
			name: "tracked property to literal",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_01_00_00},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 3,
						triplexEnd:   5,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &p1c1.ibNode,
					},
					args: args{
						index: 1,
					},
					wantS:     &s1c1.ibNode,
					wantP:     &p1c1.ibNode,
					wantO:     String("l1"),
					assertion: assert.NoError,
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tt.fields.ibNode

			gotS, gotP, gotO := tr.GetTriple(tt.args.index)
			assert.Equal(t, tt.wantS, gotS)
			assert.Equal(t, tt.wantP, gotP)
			assert.Equal(t, tt.wantO, gotO)
		})
	}
}

func TestIBNode_AddTriple_simple(t *testing.T) {
	t.Run("ibNode", func(t *testing.T) {
		// Create IBNode's
		// NOTICE this uses internals and will be different in API
		graph := namedGraph{}
		t1 := ibNodeString{ibNode: ibNode{flags: iriNodeType | stringNode}, fragment: "http://semanticstep.net/sst#t1"}
		t2 := ibNodeString{ibNode: ibNode{flags: iriNodeType | stringNode}, fragment: "http://semanticstep.net/sst#t2"}
		p1 := newIBNodeStringAsIBNode(&graph, "part", &t1.ibNode)
		st1 := newIBNodeStringAsIBNode(&graph, "main", &literalTypeString.ibNode)
		ot2 := newIBNodeStringAsIBNode(&graph, "", &t2.ibNode)

		// Example of using AddStatement and ForAll
		err := st1.addTriple(p1, ot2, false, 0)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, 1, st1.TripleCount())
		tripleCnt := 0
		err = st1.forAll(func(index int, s, p *ibNode, o Term) error {
			assert.Equal(t, 0, index)
			assert.Equal(t, p1, p)
			assert.Equal(t, ot2, o)
			tripleCnt++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, tripleCnt)
		expectedOt2 := ibNode{
			typedResource: typedResource{
				typeOf: &t2.ibNode,
			},
			ng:           &graph,
			triplexStart: triplexAllocCount,
			triplexEnd:   triplexAllocCount * 2,
			flags:        iriNodeType | stringNode,
		}
		assert.Equal(t, &expectedOt2, ot2)
	})
	t.Run("string-literal", func(t *testing.T) {
		p1 := newIBNodeStringAsIBNode(&namedGraph{}, "id", &literalTypeString.ibNode)
		st1 := newIBNodeStringAsIBNode(&namedGraph{}, "main", &literalTypeString.ibNode)
		l := String("some value")

		// Example of using AddStatement and ForAll
		err := st1.addTriple(p1, &l, false, 0)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, 1, st1.TripleCount())
		tripleCnt := 0
		err = st1.forAll(func(index int, s, p *ibNode, o Term) error {
			assert.Equal(t, 0, index)
			assert.Equal(t, p1, p)
			assert.Equal(t, l.DataType(), o.(Literal).DataType())
			assert.Equal(t, String("some value"), o.(Literal).apiValue())
			tripleCnt++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, tripleCnt)
	})
	t.Run("double-literal", func(t *testing.T) {
		p1 := newIBNodeStringAsIBNode(&namedGraph{}, "id", &literalTypeString.ibNode)
		st1 := newIBNodeStringAsIBNode(&namedGraph{}, "main", &literalTypeString.ibNode)
		l := Double(10.5)

		// Example of using AddStatement and ForAll
		err := st1.addTriple(p1, &l, false, 0)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, 1, st1.TripleCount())
		tripleCnt := 0
		err = st1.forAll(func(index int, s, p *ibNode, o Term) error {
			assert.Equal(t, 0, index)
			assert.Equal(t, p1, p)
			assert.Equal(t, l.DataType(), o.(Literal).DataType())
			assert.Equal(t, Double(10.5), o.(Literal).apiValue())
			tripleCnt++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, tripleCnt)
	})
}

func TestIBNode_AddTriple(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "l1",
	}
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		p      IBNode
		object Term
	}
	type test struct {
		fields    fields
		args      args
		assertion func(t assert.TestingT) bool
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "nil_property",
			test: func() test {
				var s1c1 ibNodeString
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						p:      nil,
						object: String("l1"),
					},
					assertion: func(t assert.TestingT) bool {
						return true
					},
				}
			}(),
		},
		{
			name: "nil_object",
			test: func() test {
				var s1c1, s2c1 ibNodeString

				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						p:      &s2c1.ibNode,
						object: nil,
					},
					assertion: func(t assert.TestingT) bool {
						return true
					},
				}
			}(),
		},
		{
			name: "duplicate triplex add test",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &s2c1.ibNode, t: &p1c1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_00_00_00_00},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 4,
						triplexEnd:   6,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = c1

				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						p:      &s2c1.ibNode,
						object: &p1c1.ibNode,
					},
					assertion: func(t assert.TestingT) bool {
						return true
					},
				}
			}(),
		},
		{
			name: "collection first property fails",
			test: func() test {
				var s1c1, s2c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: []triplex{},
					triplexKinds:   nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						p:      &rdfFirstProperty.ibNode,
						object: &s2c1.ibNode,
					},
					assertion: func(t assert.TestingT) bool {
						return true
					},
				}
			}(),
		},
		{
			name: "tracked predicate with literal collection",
			test: func() test {
				var s1c1, p1c1 ibNodeString
				lc1 := literalCollection{
					typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
					dataType:      &literalTypeDouble.ibNode,
					members:       []string{float64Bytes(1.0), float64Bytes(2.0), float64Bytes(3.0)},
				}
				var g1 namedGraph
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: []triplex{},
					triplexKinds:   nil,
					flags:          namedGraphFlags{trackPredicates: true},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						p:      &p1c1.ibNode,
						object: &lc1,
					},
					assertion: func(t assert.TestingT) bool {
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &lc1},
						}, nodeTriples(t, &s1c1.ibNode))
						assert.ElementsMatch(t, []spo{
							{s: &s1c1.ibNode, p: &p1c1.ibNode, o: &lc1},
						}, nodeTriples(t, &p1c1.ibNode))
						return true
					},
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tt.fields.ibNode
			if tt.name == "nil_property" || tt.name == "nil_object" ||
				tt.name == "duplicate triplex add test" {
				assert.Panics(t, func() {
					tr.AddStatement(tt.args.p, tt.args.object)
					tt.assertion(t)
				})
			} else {
				tr.AddStatement(tt.args.p, tt.args.object)
				tt.assertion(t)
			}
		})
	}
}

func TestIBNode_AddStatement(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		predicate Node
		object    Term
	}
	type test struct {
		fields fields
		args   args
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "nil_property",
			test: func() test {
				var s1c1 ibNodeString

				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						predicate: nil,
						object:    String("l1"),
					},
				}
			}(),
		},
		{
			name: "nil_object",
			test: func() test {
				var s1c1, s2c1 ibNodeString

				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						predicate: &s2c1.ibNode,
						object:    nil,
					},
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tt.fields.ibNode
			defer func() {
				if r := recover(); r != nil {
					var expected string
					if tt.name == "nil_property" {
						expected = "triple predicate not set"
					} else {
						expected = "triple object not set"
					}
					if r.(error).Error() != expected {
						t.Errorf("expected panic with %v, but got %v", expected, r)
					} else {
						// fmt.Println(tt.name, "pass")
					}
				} else {
					t.Errorf("expected panic, but function did not panic")
				}
			}()

			tr.AddStatement(tt.args.predicate, tt.args.object)
		})
	}
}

func Test_ibNode_Delete(t *testing.T) {
	newMockedGraphWithDeletedNodes := func(c *namedGraph, dd ...*ibNode) (g mockedTestGraph) {
		g = mockedTestGraph{namedGraph: *c}
		for _, d := range dd {
			g.On("deleteNode", d).Once()
		}
		g.On("deleteNode", mock.AnythingOfType("*sst.ibNode")).Panic("unexpected call")
		return
	}
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "val1",
	}
	staticTestStage := stage{}
	type test struct {
		d         *ibNode
		assertion assert.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "delete_node_without_references",
			test: func() test {
				var s1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &s1c1.ibNode)
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.Equal(t, false, s1c1.IsValid())
							assert.Equal(t, triplexOffset(0), s1c1.triplexStart)
							assert.Equal(t, triplexOffset(0), s1c1.triplexEnd)
							return true
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_node_with_literal",
			test: func() test {
				p := newTestProperty()
				var s1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_00},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &s1c1.ibNode)
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, s1c1.IsValid())
							return true
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_node_with_references",
			test: func() test {
				p := newTestProperty()
				var s1c1, s2c1, s3c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p.ibNode, t: &s2c1.typedResource},
						{p: &p.ibNode, t: &s3c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_01_01_00},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &s1c1.ibNode)
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, s1c1.IsValid())
							assert.NoError(t, s2c1.forAll(func(_ int, _, _ *ibNode, _ Term) error {
								assert.Fail(t, "triples unexpected")
								return nil
							}))
							return assert.NoError(t, s3c1.forAll(func(_ int, _, _ *ibNode, _ Term) error {
								assert.Fail(t, "triples unexpected")
								return nil
							}))
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_node_with_tracked_predicate",
			test: func() test {
				p := newTestProperty()
				var s1c1, s2c1, s3c1, p1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p.ibNode, t: &s3c1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
					},
					triplexKinds: []uint{0b_10_00_01_01_00},
					flags: namedGraphFlags{
						trackPredicates: true,
					},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 4,
						triplexEnd:   5,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &s1c1.ibNode)
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, s1c1.IsValid())
							for _, ib := range []*ibNodeString{&s2c1, &s3c1, &p1c1} {
								assert.NoError(t, ib.forAll(func(_ int, _, _ *ibNode, _ Term) error {
									assert.Failf(t, "triples unexpected", "ibNode: %v", ib.fragment)
									return nil
								}))
							}
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_tracked_predicate_for_node",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
					},
					triplexKinds: []uint{0b_10_01_00},
					flags: namedGraphFlags{
						trackPredicates: true,
					},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &p1c1.ibNode)
				tt := test{
					d: &p1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, p1c1.IsValid())
							for _, ib := range []*ibNodeString{&s1c1, &s2c1} {
								assert.NoError(t, ib.forAll(func(_ int, _, _ *ibNode, _ Term) error {
									assert.Failf(t, "triples unexpected", "ibNode: %v", ib.fragment)
									return nil
								}))
							}
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_object_node_with_tracked_predicate",
			test: func() test {
				p := newTestProperty()
				var s1c1, s2c1, s3c1, p1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p.ibNode, t: &s3c1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource},
						{p: &s2c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_10_00_00_01_01},
					flags: namedGraphFlags{
						trackPredicates: true,
					},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 4,
						triplexEnd:   5,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &s1c1.ibNode)
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, s1c1.IsValid())
							for _, ib := range []*ibNodeString{&s2c1, &s3c1, &p1c1} {
								assert.NoError(t, ib.forAll(func(_ int, _, _ *ibNode, _ Term) error {
									assert.Failf(t, "triples unexpected", "ibNode: %v", ib.fragment)
									return nil
								}))
							}
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_tracked_predicate_for_object_node",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s2c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_10_00_01},
					flags: namedGraphFlags{
						trackPredicates: true,
					},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &p1c1.ibNode)
				tt := test{
					d: &p1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, p1c1.IsValid())
							for _, ib := range []*ibNodeString{&s1c1, &s2c1} {
								assert.NoError(t, ib.forAll(func(_ int, _, _ *ibNode, _ Term) error {
									assert.Failf(t, "triples unexpected", "ibNode: %v", ib.fragment)
									return nil
								}))
							}
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_node_with_tracked_predicate_and_literal",
			test: func() test {
				var s1c1, p1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_10_00},
					flags: namedGraphFlags{
						trackPredicates: true,
					},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &s1c1.ibNode)
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, s1c1.IsValid())
							return assert.NoError(t, p1c1.forAll(func(_ int, _, _ *ibNode, _ Term) error {
								assert.Fail(t, "triples unexpected")
								return nil
							}))
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_tracked_predicate_for_node_with_literal",
			test: func() test {
				var s1c1, p1c1 ibNodeString

				c1 := namedGraph{
					stage: &staticTestStage,
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
					},
					triplexKinds: []uint{0b_10_00},
					flags: namedGraphFlags{
						trackPredicates: true,
					},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				g1 = newMockedGraphWithDeletedNodes(&c1, &p1c1.ibNode)
				tt := test{
					d: &p1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						if assert.NoError(t, err) {
							// g1.AssertExpectations(t.(mock.TestingT))
							assert.False(t, p1c1.IsValid())
							return assert.NoError(t, s1c1.forAll(func(_ int, _, _ *ibNode, _ Term) error {
								assert.Fail(t, "triples unexpected")
								return nil
							}))
						}
						return false
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_already_deleted_node",
			test: func() test {
				var s1c1 ibNodeString

				// c1 := namedGraph{
				// 	stage:          &staticTestStage,
				// 	triplexStorage: []triplex{},
				// 	triplexKinds:   nil,
				// }
				// g1 := mockedTestGraph{
				// 	namedGraph: c1,
				// }
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           nil,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				// g1 = mockedTestGraph{namedGraph: namedGraph{&c1}}
				tt := test{
					d: &s1c1.ibNode,
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						return assert.ErrorIs(t, err, ErrNodeDeletedOrInvalid)
					},
				}
				return tt
			}(),
		},
		{
			name: "delete_base_node",
			test: test{
				d: &literalTypeString.ibNode,
				assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
					return assert.ErrorIs(t, err, ErrNodeNotModifiable)
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "delete_already_deleted_node" || tt.name == "delete_base_node" {
				assert.Panics(t, func() { tt.d.Delete() })
			} else {
				tt.d.Delete()
			}

		})
	}
}

func TestNewLiteralCollection(t *testing.T) {
	type args struct {
		member0   Literal
		members1N []Literal
	}
	tests := []struct {
		name      string
		args      args
		want      *literalCollection
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "single_string_member",
			args: args{
				member0:   String("string value"),
				members1N: nil,
			},
			want: &literalCollection{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeString.ibNode,
				members:       []string{"string value"},
			},

			assertion: assert.NoError,
		},
		{
			name: "two_string_members",
			args: args{
				member0: String("string value 1"),
				members1N: []Literal{
					String("string value 2"),
				},
			},
			want: &literalCollection{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeString.ibNode,
				members:       []string{"string value 1", "string value 2"},
			},
			assertion: assert.NoError,
		},
		{
			name: "three_string_members",
			args: args{
				member0: String("string value 1"),
				members1N: []Literal{
					String("string value 2"),
					String("string value 3"),
				},
			},
			want: &literalCollection{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeString.ibNode,
				members:       []string{"string value 1", "string value 2", "string value 3"},
			},
			assertion: assert.NoError,
		},
		{
			name: "string_and_double_fails",
			args: args{
				member0: String("string value 1"),
				members1N: []Literal{
					Double(2),
				},
			},
			want: &literalCollection{},
			assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrLiteralDataTypesDiffer)
			},
		},
		{
			name: "two_integer_members",
			args: args{
				member0: &literal{
					typedResource: typedResource{
						typeOf: &literalTypeInteger.ibNode,
					},
					value: "\x00\x00\x00\x00\x00\x00\x00\x00",
				},
				members1N: []Literal{
					&literal{
						typedResource: typedResource{
							typeOf: &literalTypeInteger.ibNode,
						},
						value: "\x01\x00\x00\x00\x00\x00\x00\x00",
					},
				},
			},
			want: &literalCollection{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeInteger.ibNode,
				members:       []string{"\x00\x00\x00\x00\x00\x00\x00\x00", "\x01\x00\x00\x00\x00\x00\x00\x00"},
			},
			assertion: assert.NoError,
		},
		{
			name: "two_double_members",
			args: args{
				member0: Double(0),
				members1N: []Literal{
					Double(1),
				},
			},
			want: &literalCollection{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeDouble.ibNode,
				members:       []string{"\x00\x00\x00\x00\x00\x00\x00\x00", "\x3f\xf0\x00\x00\x00\x00\x00\x00"},
			},
			assertion: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "string_and_double_fails" {
				assert.Panics(t, func() {
					got := NewLiteralCollection(tt.args.member0, tt.args.members1N...)
					if assert.Equal(t, tt.want, got) {
						assert.Same(t, tt.want.dataType, got.(*literalCollection).dataType)
					}
				})
			} else {
				got := NewLiteralCollection(tt.args.member0, tt.args.members1N...)
				if assert.Equal(t, tt.want, got) {
					assert.Same(t, tt.want.dataType, got.(*literalCollection).dataType)
				}
			}
		})
	}
}

func Test_literalCollection_ForMembers(t *testing.T) {
	type fields struct {
		typedResource typedResource
		dataType      *ibNode
		members       []string
	}
	type args struct {
		e         func(index int, l Literal)
		assertion func(testingT assert.TestingT)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "single_member",
			fields: fields{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeString.ibNode,
				members:       []string{"string value"},
			},
			args: func() args {
				type gotT struct {
					index int
					l     Literal
				}
				var got []gotT
				a := args{
					e: func(index int, l Literal) {
						got = append(got, gotT{index: index, l: l})
					},
					assertion: func(t assert.TestingT) {
						assert.Equal(t, []gotT{{index: 0, l: literal{
							typedResource: typedResource{typeOf: &literalTypeString.ibNode},
							value:         "string value",
						}}}, got)
					},
				}
				return a
			}(),
		},
		{
			name: "two_members",
			fields: fields{
				typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
				dataType:      &literalTypeString.ibNode,
				members:       []string{"string value 1", "string value 2"},
			},
			args: func() args {
				type gotT struct {
					index int
					l     Literal
				}
				var got []gotT
				a := args{
					e: func(index int, l Literal) {
						got = append(got, gotT{index: index, l: l})
					},
					assertion: func(t assert.TestingT) {
						assert.Equal(t, []gotT{
							{index: 0, l: literal{
								typedResource: typedResource{typeOf: &literalTypeString.ibNode},
								value:         "string value 1",
							}},
							{index: 1, l: literal{
								typedResource: typedResource{typeOf: &literalTypeString.ibNode},
								value:         "string value 2",
							}},
						}, got)
					},
				}
				return a
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &literalCollection{
				typedResource: tt.fields.typedResource,
				dataType:      tt.fields.dataType,
				members:       tt.fields.members,
			}
			c.forInternalMembers(tt.args.e)
			tt.args.assertion(t)
		})
	}
}

func Test_newTermCollection(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	type args struct {
		graph namedGraph

		members1N []Term
	}
	type test struct {
		args          args
		wantAssertion assert.ValueAssertionFunc
		assertion     assert.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		test
	}{
		{
			name: "single_ibNode",
			test: func() test {
				var s1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}

				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				return test{
					args: args{
						graph:     g1,
						members1N: []Term{&s1c1.ibNode},
					},
					wantAssertion: func(t assert.TestingT, val interface{}, _ ...interface{}) bool {
						got := val.(TermCollection)
						var allCnt int
						assert.NoError(t, got.(*ibNode).forAll(func(index int, s, p *ibNode, o Term) error {
							allCnt++
							assert.Equal(t, got.(*ibNode), s)
							assert.True(t, p.Is(rdfFirst))
							assert.Equal(t, &s1c1.ibNode, o)
							return nil
						}))
						return assert.Equal(t, 1, allCnt)
					},
					assertion: func(t assert.TestingT, err error, _ ...interface{}) bool {
						return true
					},
				}
			}(),
		},
		{
			name: "two_different_ibNodes",
			test: func() test {
				var s1c1, s2c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				return test{
					args: args{
						graph:     g1,
						members1N: []Term{&s1c1.ibNode, &s2c1.ibNode},
					},
					wantAssertion: func(t assert.TestingT, val interface{}, _ ...interface{}) bool {
						got := val.(TermCollection)
						var allCnt int
						assert.NoError(t, got.(*ibNode).forAll(func(index int, s, p *ibNode, o Term) error {
							allCnt++
							assert.Equal(t, got.(*ibNode), s)
							assert.True(t, p.Is(rdfFirst))
							if allCnt == 1 {
								assert.Equal(t, &s1c1.ibNode, o)
								return nil
							}
							assert.Equal(t, &s2c1.ibNode, o)
							return nil
						}))
						return assert.Equal(t, 2, allCnt)
					},
					assertion: assert.NoError,
				}
			}(),
		},
		{
			name: "two_same_ibNodes",
			test: func() test {
				var s1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				return test{
					args: args{
						graph: g1,
						// member0:   &s1c1.ibNode,
						members1N: []Term{&s1c1.ibNode, &s1c1.ibNode},
					},
					wantAssertion: func(t assert.TestingT, val interface{}, _ ...interface{}) bool {
						got := val.(TermCollection)
						var allCnt int
						assert.NoError(t, got.(*ibNode).forAll(func(index int, s, p *ibNode, o Term) error {
							allCnt++
							assert.Equal(t, got.(*ibNode), s)
							assert.True(t, p.Is(rdfFirst))
							assert.Equal(t, &s1c1.ibNode, o)
							return nil
						}))
						return assert.Equal(t, 2, allCnt)
					},
					assertion: assert.NoError,
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newTermCollection(&tt.args.graph, tt.args.members1N...)
			tt.assertion(t, err)
			tt.wantAssertion(t, got)
		})
	}
}

func TestTermCollection_ForMembers(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		e func(index int, o Term)
	}
	type test struct {
		fields    fields
		args      args
		assertion func()
	}
	tests := []struct {
		name string
		test func(t assert.TestingT) test
	}{
		{
			name: "for_single_node_member",
			test: func(t assert.TestingT) test {
				var s1c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				list, err := newTermCollection(&g1, &s1c1)
				assert.NoError(t, err)
				var callbackCnt int
				return test{
					fields: fields{
						ibNode: list.(*ibNode),
					},
					args: args{
						e: func(index int, o Term) {
							assert.Equal(t, 0, index)
							assert.Equal(t, s1c1.ibNode, *o.(*ibNode))
							callbackCnt++
						},
					},
					assertion: func() {
						assert.Equal(t, 1, callbackCnt)
					},
				}
			},
		},
		{
			name: "for_two_node_members",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				list, err := newTermCollection(&g1, &s1c1, &s2c1)
				assert.NoError(t, err)
				var callbackCnt int
				return test{
					fields: fields{
						ibNode: list.(*ibNode),
					},
					args: args{
						e: func(index int, o Term) {
							assert.Less(t, index, 2)
							assert.GreaterOrEqual(t, index, 0)
							switch index {
							case 0:
								assert.Equal(t, s1c1.ibNode, *o.(*ibNode))
							case 1:
								assert.Equal(t, s2c1.ibNode, *o.(*ibNode))
							}
							callbackCnt++
						},
					},
					assertion: func() {
						assert.Equal(t, 2, callbackCnt)
					},
				}
			},
		},
		{
			name: "for_node_member_and_additional_triple",
			test: func(t assert.TestingT) test {
				p := newTestProperty()
				l := literal{
					typedResource: typedResource{&literalTypeString.ibNode},
					value:         "literal value",
				}
				var s1c1 ibNodeString
				var col1 ibNodeUuid
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: &rdfFirstProperty.ibNode, t: &col1.typedResource},
						{p: &rdfFirstProperty.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &l.typedResource},
					},
					triplexKinds: []uint{0b_00_00_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				col1 = ibNodeUuid{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					id: uuid.UUID{},
				}
				g1 = c1
				var callbackCnt int
				return test{
					fields: fields{
						ibNode: &col1.ibNode,
					},
					args: args{
						e: func(index int, o Term) {
							assert.Equal(t, 0, index)
							assert.Equal(t, s1c1.ibNode, *o.(*ibNode))
							callbackCnt++
						},
					},
					assertion: func() {
						assert.Equal(t, 1, callbackCnt)
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttt := tt.test(t)
			c := ttt.fields.ibNode

			c.ForMembers(ttt.args.e)
			ttt.assertion()
		})
	}
}

func TestTermCollection_Members(t *testing.T) {
	st := OpenStage(DefaultTriplexMode)
	type fields struct {
		ibNode *ibNode
	}
	type test struct {
		fields fields
		want   []Term
	}
	tests := []struct {
		name string
		test func(t assert.TestingT) test
	}{
		{
			name: "two_node_members",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: st.(*stage),
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				g1 = c1
				list, err := newTermCollection(&g1, &s1c1, &s2c1)
				assert.NoError(t, err)
				return test{
					fields: fields{
						ibNode: list.(*ibNode),
					},
					want: []Term{&s1c1.ibNode, &s2c1.ibNode},
				}
			},
		},
		{
			name: "two_literal_collection_members",
			test: func(t assert.TestingT) test {
				var g1 namedGraph
				c1 := namedGraph{
					stage:          st.(*stage),
					triplexStorage: nil,
					triplexKinds:   nil,
				}
				g1 = c1
				lc1 := literalCollection{
					typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
					dataType:      &literalTypeDouble.ibNode,
					members:       []string{float64Bytes(1.0), float64Bytes(2.0), float64Bytes(3.0)},
				}
				lc2 := literalCollection{
					typedResource: typedResource{typeOf: &literalCollectionResourceType.ibNode},
					dataType:      &literalTypeDouble.ibNode,
					members:       []string{float64Bytes(1.1), float64Bytes(2.1), float64Bytes(3.1)},
				}
				toDoubles := func(lc *literalCollection) []float64 {
					var dm []float64
					lc.forInternalMembers(func(_ int, l Literal) {
						dm = append(dm, float64(l.apiValue().(Double)))
					})
					return dm
				}
				assert.Equal(t, []float64{1, 2, 3}, toDoubles(&lc1))
				assert.Equal(t, []float64{1.1, 2.1, 3.1}, toDoubles(&lc2))
				list, err := newTermCollection(&g1, &lc1, &lc2)
				assert.NoError(t, err)
				return test{
					fields: fields{
						ibNode: list.(*ibNode),
					},
					want: []Term{&lc1, &lc2},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttt := tt.test(t)
			c := ttt.fields.ibNode
			assert.Equal(t, ttt.want, c.Members())
		})
	}
}

func TestTermCollection_SetMembers(t *testing.T) {
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		members1N []Term
	}
	type test struct {
		fields    fields
		args      args
		assertion func(t assert.TestingT) bool
	}
	staticTestStage := OpenStage(DefaultTriplexMode).(*stage)
	tests := []struct {
		name string
		test func(t assert.TestingT) test
	}{
		{
			name: "set_single_member",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1 ibNodeString
				var g1 namedGraph
				c1 := namedGraph{
					stage: staticTestStage,
					triplexStorage: []triplex{
						{p: nil, t: nil},
						{p: nil, t: nil},
					},
					triplexKinds: nil,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				g1 = c1
				list, err := newTermCollection(&g1, &s1c1)
				assert.NoError(t, err)
				return test{
					fields: fields{
						ibNode: list.(*ibNode),
					},
					args: args{
						members1N: []Term{&s2c1.ibNode},
					},
					assertion: func(t assert.TestingT) bool {
						return assert.Same(t, &s2c1.typedResource, g1.triplexStorage[list.(*ibNode).triplexStart].t)
					},
				}
			},
		},
		{
			name: "replace_single_member_with_members_no_grow",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1, s3c1 ibNodeString
				var list ibNodeUuid
				var g1 namedGraph
				c1 := namedGraph{
					stage: staticTestStage,
					triplexStorage: []triplex{
						{p: &rdfFirstProperty.ibNode, t: &list.typedResource},
						{p: nil, t: nil},
						{p: &rdfFirstProperty.ibNode, t: &s1c1.typedResource},
						{p: nil, t: nil},
					},
					triplexKinds: []uint{0b_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				list = ibNodeUuid{
					ibNode: ibNode{
						typedResource: typedResource{
							typeOf: &termCollectionResourceType.ibNode,
						},
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   4,
						flags:        uuidNode | blankNodeType,
					},
					id: uuid.New(),
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &list.ibNode,
					},
					args: args{
						members1N: []Term{&s2c1.ibNode, &s3c1.ibNode},
					},
					assertion: func(t assert.TestingT) bool {
						if assert.Equal(t, triplexOffset(2), list.triplexEnd-list.triplexStart) {
							assert.Same(t, &s2c1.typedResource, c1.triplexStorage[list.triplexStart].t)
							return assert.Same(t, &s3c1.typedResource, c1.triplexStorage[list.triplexStart+1].t)
						}
						return false
					},
				}
			},
		},
		{
			name: "replace_single_member_with_members_grow",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1, s3c1 ibNodeString
				var list ibNodeUuid
				var g1 namedGraph
				c1 := namedGraph{
					stage: staticTestStage,
					triplexStorage: []triplex{
						{p: &rdfFirstProperty.ibNode, t: &list.typedResource},
						{p: nil, t: nil},
						{p: &rdfFirstProperty.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				list = ibNodeUuid{
					ibNode: ibNode{
						typedResource: typedResource{
							typeOf: &termCollectionResourceType.ibNode,
						},
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   3,
						flags:        uuidNode | blankNodeType,
					},
					id: uuid.New(),
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &list.ibNode,
					},
					args: args{
						members1N: []Term{&s2c1.ibNode, &s3c1.ibNode},
					},
					assertion: func(t assert.TestingT) bool {
						if assert.Equal(t, triplexOffset(1)+triplexAllocCount, list.triplexEnd-list.triplexStart) {
							assert.Same(t, &s2c1.typedResource, g1.triplexStorage[list.triplexStart].t)
							return assert.Same(t, &s3c1.typedResource, g1.triplexStorage[list.triplexStart+1].t)
						}

						return false
					},
				}
			},
		},
		{
			name: "replace_single_member_with_members_triple_no_grow",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1, s3c1 ibNodeString
				var list ibNodeUuid
				var g1 namedGraph
				p := newTestProperty()
				l1 := literal{
					typedResource: typedResource{&literalTypeString.ibNode},
					value:         "l1",
				}
				c1 := namedGraph{
					stage: staticTestStage,
					triplexStorage: []triplex{
						{p: &rdfFirstProperty.ibNode, t: &list.typedResource},
						{p: nil, t: nil},
						{p: &rdfFirstProperty.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &l1.typedResource},
						{p: nil, t: nil},
					},
					triplexKinds: []uint{0b_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				list = ibNodeUuid{
					ibNode: ibNode{
						typedResource: typedResource{
							typeOf: &termCollectionResourceType.ibNode,
						},
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   5,
						flags:        uuidNode | blankNodeType,
					},
					id: uuid.New(),
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &list.ibNode,
					},
					args: args{
						members1N: []Term{&s2c1.ibNode, &s3c1.ibNode},
					},
					assertion: func(t assert.TestingT) bool {
						if assert.Equal(t, triplexOffset(3), list.triplexEnd-list.triplexStart) {
							assert.Same(t, &s2c1.typedResource, g1.triplexStorage[list.triplexStart].t)
							assert.Same(t, &s3c1.typedResource, g1.triplexStorage[list.triplexStart+1].t)
							return assert.Same(t, &l1.typedResource, g1.triplexStorage[list.triplexStart+2].t)
						}
						return false
					},
				}
			},
		},
		{
			name: "replace_single_member_with_members_triples_grow",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1, s3c1 ibNodeString
				var list ibNodeUuid
				var g1 namedGraph
				p := newTestProperty()
				l1, l2 := literal{
					typedResource: typedResource{&literalTypeString.ibNode},
					value:         "l1",
				}, literal{
					typedResource: typedResource{&literalTypeString.ibNode},
					value:         "l2",
				}
				c1 := namedGraph{
					stage: staticTestStage,
					triplexStorage: []triplex{
						{p: &rdfFirstProperty.ibNode, t: &list.typedResource},
						{p: nil, t: nil},
						{p: &rdfFirstProperty.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &l1.typedResource},
						{p: &p.ibNode, t: &l2.typedResource},
					},
					triplexKinds: []uint{0b_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   1,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 1,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				list = ibNodeUuid{
					ibNode: ibNode{
						typedResource: typedResource{
							typeOf: &termCollectionResourceType.ibNode,
						},
						ng:           &g1,
						triplexStart: 2,
						triplexEnd:   5,
						flags:        uuidNode | blankNodeType,
					},
					id: uuid.New(),
				}
				g1 = c1
				return test{
					fields: fields{
						ibNode: &list.ibNode,
					},
					args: args{
						members1N: []Term{&s2c1.ibNode, &s3c1.ibNode},
					},
					assertion: func(t assert.TestingT) bool {
						if assert.Equal(t, triplexOffset(3)+triplexAllocCount, list.triplexEnd-list.triplexStart) {
							assert.Same(t, &s2c1.typedResource, g1.triplexStorage[list.triplexStart].t)
							assert.Same(t, &s3c1.typedResource, g1.triplexStorage[list.triplexStart+1].t)
							assert.Same(t, &l2.typedResource, g1.triplexStorage[list.triplexStart+2].t)
							return assert.Same(t, &l1.typedResource, g1.triplexStorage[list.triplexStart+3].t)
						}
						return false
					},
				}
			},
		},
		{
			name: "replace_single_member_with_members_inverse_no_grow",
			test: func(t assert.TestingT) test {
				var s1c1, s2c1, s3c1 ibNodeString
				var list ibNodeUuid
				// var g1 namedGraph
				p := newTestProperty()
				c1 := namedGraph{
					stage: staticTestStage,
					triplexStorage: []triplex{
						{p: &rdfFirstProperty.ibNode, t: &list.typedResource},
						{p: &p.ibNode, t: &list.typedResource},
						{p: &rdfFirstProperty.ibNode, t: &s1c1.typedResource},
						{p: &p.ibNode, t: &s1c1.typedResource},
						{p: nil, t: nil},
					},
					triplexKinds: []uint{0b_01_00_00_01},
				}
				g1 := mockedTestGraph{
					namedGraph: c1,
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   2,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s2",
				}
				s3c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &g1.namedGraph,
						triplexStart: 0,
						triplexEnd:   0,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s3",
				}
				list = ibNodeUuid{
					ibNode: ibNode{
						typedResource: typedResource{
							typeOf: &termCollectionResourceType.ibNode,
						},
						ng:           &g1.namedGraph,
						triplexStart: 2,
						triplexEnd:   5,
						flags:        uuidNode | blankNodeType,
					},
					id: uuid.New(),
				}
				return test{
					fields: fields{
						ibNode: &list.ibNode,
					},
					args: args{
						members1N: []Term{&s2c1.ibNode, &s3c1.ibNode},
					},
					assertion: func(t assert.TestingT) bool {
						if assert.Equal(t, triplexOffset(3), list.triplexEnd-list.triplexStart) {
							assert.Same(t, &s2c1.typedResource, g1.triplexStorage[list.triplexStart].t)
							assert.Same(t, &s3c1.typedResource, g1.triplexStorage[list.triplexStart+1].t)
							assert.Same(t, &s1c1.typedResource, g1.triplexStorage[list.triplexStart+2].t)
							assert.Equal(t, subjectTriplexKind, (&list.ibNode).triplexKindAt(1))
						}
						return false
					},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttt := tt.test(t)
			c := ttt.fields.ibNode
			c.SetMembers(ttt.args.members1N...)
			ttt.assertion(t)
		})
	}
}

func newTestProperty() ibNodeString {
	pg := namedGraph{
		stage:          OpenStage(DefaultTriplexMode).(*stage),
		triplexStorage: []triplex{},
	}
	return ibNodeString{
		ibNode: ibNode{
			ng:    &pg,
			flags: iriNodeType | stringNode,
		},
		fragment: "p",
	}
}

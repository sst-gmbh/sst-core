// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bufio"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadDiff(t *testing.T) {
	t.Skip("diff functionality needs further development")
	type args struct {
		stageCreator func(*testing.T) Stage
		rBase        *bufio.Reader
		rDiff        *bufio.Reader
	}
	tests := []struct {
		name           string
		args           args
		want           NamedGraph
		errAssertion   assert.ErrorAssertionFunc
		graphAssertion func(*testing.T, NamedGraph)
	}{
		{
			name: "empty_default_graph_to_empty_diff",
			args: args{
				stageCreator: func(t *testing.T) Stage {
					return OpenStage(DefaultTriplexMode)
				},
				rBase: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
				rDiff: bb("\x00\x00\x00\x00\x00\x00\x00\x00"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				if assert.NotEqual(t, nil, graph) {
					assert.Equal(t, "e38c8811-1028-4442-af79-2cce833feb4e", graph.ID().String())
				}
			},
		},
		{
			name: "empty_graph_to_to_graph_with_nodes",
			args: args{
				stageCreator: func(t *testing.T) Stage {
					// return NewStageWithID(uuid.MustParse("e38c8811-1028-4442-af79-2cce833feb4e"))
					return OpenStage(DefaultTriplexMode)
				},
				rBase: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
				rDiff: bb("\x00\x00\x00\x01\x02\x1fhttp://semanticstep.net/schema#" +
					"\x00\x02\x00\x01\x02\x02s1\x02\x02\x02s2\x04\x02\x02\x00\x02\x02\x02p1\x02\x02\x02p2\x02" +
					"\x01\x01\x0e\x01\x01\x00\x01\x01\x12\x02\x01\x00\x02"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				if assert.NotEqual(t, nil, graph) {
					assert.Equal(t, 2, graph.IRINodeCount())
				}
			},
		},
		{
			name: "graph_diff_with_nodes",
			args: args{
				stageCreator: func(t *testing.T) Stage {
					// return NewStageWithID(uuid.MustParse("e38c8811-1028-4442-af79-2cce833feb4e"))
					return OpenStage(DefaultTriplexMode)
				},
				rBase: nil,
				rDiff: bb("\x00\x00\x00\x01\x02\x1fhttp://semanticstep.net/schema#" +
					"\x00\x02\x00\x01\x02\x02s1\x02\x02\x02s2\x04\x02\x02\x00\x02\x02\x02p1\x02\x02\x02p2\x02" +
					"\x01\x01\x0e\x01\x01\x00\x01\x01\x12\x02\x01\x00\x02"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				if assert.NotEqual(t, nil, graph) {
					assert.Equal(t, 2, graph.IRINodeCount())
				}
			},
		},
		// {
		// 	name: "graph_base_with_nodes",
		// 	args: args{
		// 		stageCreator: func(t *testing.T) Stage {
		// 			// return NewStageWithID(uuid.MustParse("e38c8811-1028-4442-af79-2cce833feb4e"))
		// 			return OpenStage(DefaultTriplexMode)
		// 		},
		// 		rBase: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e" +
		// 			"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
		// 			"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
		// 			"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
		// 		rDiffs: nil,
		// 	},
		// 	errAssertion: assert.NoError,
		// 	graphAssertion: func(t *testing.T, graph NamedGraph) {
		// 		if assert.NotEqual(t, nil, graph) {
		// 			assert.Equal(t, 2, graph.IRINodeCount())
		// 		}
		// 	},
		// },
		// {
		// 	name: "graph with nodes no imports to graph with imports",
		// 	args: args{
		// 		stageCreator: func(t *testing.T) Stage {
		// 			// return NewStageWithID(uuid.MustParse("e38c8811-1028-4442-af79-2cce833feb4e"))
		// 			return OpenStage(DefaultTriplexMode)
		// 		},
		// 		rBase: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x01" +
		// 			"\x1fhttp://semanticstep.net/schema#\x02\x01\x02s1\x01\x02s2\x02\x01\x02\x02p1\x01\x02p2\x01" +
		// 			"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
		// 		rDiffs: []ReaderWithByteReader{bb("\x00\x01\x02-urn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1" +
		// 			"\x00\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x02\x04\x02")},
		// 	},
		// 	errAssertion: assert.NoError,
		// 	graphAssertion: func(t *testing.T, graph NamedGraph) {
		// 		// if assert.NotEqual(t, nil, graph) {
		// 		// 	assert.Equal(t, 1, len(graph.Imports().Direct()))
		// 		// }
		// 	},
		// },
		// {
		// 	name: "graph with imports to graph with literals",
		// 	args: args{
		// 		stageCreator: func(t *testing.T) Stage {
		// 			// return NewStageWithID(uuid.MustParse("d270203d-2598-4a71-80b1-50576a1fda84"))
		// 			return OpenStage(DefaultTriplexMode)
		// 		},
		// 		rBase: bb("SST-1.0\x00\x2durn:uuid:d270203d-2598-4a71-80b1-50576a1fda84" +
		// 			"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
		// 			"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
		// 			"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
		// 		rDiffs: []ReaderWithByteReader{bb("\x01\x00\x01-urn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1" +
		// 			"\x00\x00\x00\x01\x01\x00\x01\x00\x03\x04\x01\x02s2\x03\x01\x01\x00\x00\x00\x01\x00\x02\x02\x02p3\x02" +
		// 			"\x03\x00\x0d\x01\x01\x03\x0e\x00\x04str1" +
		// 			"\x12\x04\x85\xd2\xa9\x9a\x21\x16\x05\xbf\xe6\xcf\x99\xb3\xe6\xcc\x993\x03\x00\x11\x02\x01\x00\x02")},
		// 	},
		// 	errAssertion: assert.NoError,
		// 	graphAssertion: func(t *testing.T, graph NamedGraph) {
		// 		s1, err := graph.GetIRINodeByFragment("s1")
		// 		assert.NoError(t, err)
		// 		assert.Equal(t, 3, s1.TripleCount())
		// 	},
		// },
		{
			name: "graph with lesser literal value modification",
			args: args{
				stageCreator: func(t *testing.T) Stage {
					// return NewStageWithID(uuid.MustParse("2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792"))
					return OpenStage(DefaultTriplexMode)
				},
				rBase: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x14"),
				rDiff: bb("\x00\x00\x00\x00\x00" +
					"\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x01\x00\x03\x01\x05\x04\x14\x06\x04\x16"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				s1 := graph.GetIRINodeByFragment("s1")
				assert.NotNil(t, s1)
				assert.Equal(t, 1, s1.TripleCount())
			},
		},
		{
			name: "graph with literal to graph with literal and literal collection",
			args: args{
				stageCreator: func(t *testing.T) Stage {
					// return NewStageWithID(uuid.MustParse("d270203d-2598-4a71-80b1-50576a1fda84"))
					return OpenStage(DefaultTriplexMode)
				},
				rBase: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x14"),
				rDiff: bb("\x00\x00\x00\x00\x00" +
					"\x01\x00\x00\x00\x00\x03\x02\x00\x01\x00\x01\x02\x02p2\x02\x01" +
					"\x00\x01\x01\x04\x0a\x7f\x02\x00\x03cl1\x00\x03cl2"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				assert.Equal(t, 1, graph.IRINodeCount())
				s1 := graph.GetIRINodeByFragment("s1")
				assert.NotNil(t, s1)
				var litInt *int64
				var litCol *literalCollection = &literalCollection{}
				assert.NoError(t, s1.ForAll(func(_ int, _, _ IBNode, o Term) error {
					switch o.TermKind() {
					case TermKindLiteral:
						assert.Nil(t, litInt)
						i := int64(o.(Integer))
						litInt = &i
					case TermKindLiteralCollection:
						assert.Equal(t, &literalCollection{}, litCol)
						litCol = o.(*literalCollection)
					case TermKindIBNode, TermKindTermCollection:
						assert.Fail(t, "object kind expectation failed")
					}
					return nil
				}))
				assert.Equal(t, int64(10), *litInt)
				if assert.NotNil(t, litCol) && assert.Equal(t, 2, litCol.MemberCount()) {
					if assert.Equal(t, &literalTypeString.ibNode, litCol.Member(0).DataType()) {
						assert.Equal(t, String("cl1"), litCol.Member(0).(String))
					}
					if assert.Equal(t, &literalTypeString.ibNode, litCol.Member(1).DataType()) {
						assert.Equal(t, String("cl2"), litCol.Member(1).(String))
					}
				}
			},
		},
		{
			name: "graph with literal collection modification",
			args: args{
				stageCreator: func(t *testing.T) Stage {
					// return NewStageWithID(uuid.MustParse("2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792"))
					return OpenStage(DefaultTriplexMode)
				},
				rBase: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x02\x02\x02p1\x01\x02p2\x01\x00" +
					"\x02\x01\x04\x14\x02\x7f\x02\x00\x03cl0\x00\x03cl1"),
				rDiff: bb("\x00\x00\x00\x00\x00" +
					"\x01\x00\x00\x00\x00\x00\x01" +
					"\x00\x00\x00\x02\x01\x00\x03\x01\x04\x09" +
					"\x7f\x02\x00\x03cl0\x00\x03cl1\x0a\x7f\x02\x00\x03cl2\x00\x03cl3"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				assert.Equal(t, 1, graph.IRINodeCount())
				s1 := graph.GetIRINodeByFragment("s1")
				assert.NotNil(t, s1)
				var litInt *int64
				var litCol *literalCollection = &literalCollection{}
				assert.NoError(t, s1.ForAll(func(_ int, _, _ IBNode, o Term) error {
					switch o.TermKind() {
					case TermKindLiteral:
						assert.Nil(t, litInt)
						i := int64(o.(Integer))
						litInt = &i
					case TermKindLiteralCollection:
						assert.Equal(t, &literalCollection{}, litCol)
						litCol = o.(*literalCollection)
					case TermKindIBNode, TermKindTermCollection:
						assert.Fail(t, "object kind expectation failed")
					}
					return nil
				}))
				assert.Equal(t, int64(10), *litInt)
				if assert.NotNil(t, litCol) && assert.Equal(t, 2, litCol.MemberCount()) {
					if assert.Equal(t, &literalTypeString.ibNode, litCol.Member(0).DataType()) {
						assert.Equal(t, String("cl2"), litCol.Member(0).(String))
					}
					if assert.Equal(t, &literalTypeString.ibNode, litCol.Member(1).DataType()) {
						assert.Equal(t, String("cl3"), litCol.Member(1).(String))
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SstReadDiff(DefaultTriplexMode, tt.args.rBase, tt.args.rDiff)
			if tt.errAssertion(t, err) {
				if tt.graphAssertion != nil {
					tt.graphAssertion(t, got)
				}
			}
		})
	}
}

// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fromTurtleFile(t *testing.T, fileName string) (dcd rdfReader, parseError readErrorHandler) {
	file, err := os.Open(fileName)
	t.Cleanup(func() {
		assert.NoError(t, file.Close())
	})
	if assert.NoError(t, err) {
		dcd = newTripleReader(file, RdfFormatTurtle)
	}
	parseError = RecoverHandler
	return
}

func fromTurtleContent(
	_ *testing.T,
	content string,
) (rdr rdfReader, parseError readErrorHandler) {
	r := strings.NewReader(content)
	rdr = newTripleReader(r, RdfFormatTurtle)
	parseError = RecoverHandler
	return
}

func Test_FromReaderToStage(t *testing.T) {
	type args struct {
		rdr        rdfReader
		parseError readErrorHandler
	}
	toArgs := func(rdr rdfReader, parseError readErrorHandler) args {
		return args{rdr: rdr, parseError: parseError}
	}
	tests := []struct {
		name           string
		args           args
		stageAssertion func(t *testing.T, stage Stage)
		errorAssertion assert.ErrorAssertionFunc
	}{
		{
			name: "double_collection",
			args: toArgs(fromTurtleContent(t,
				`<http://example.org/point3d> <http://example.org/stuff/1.0/coords> ( 1.0 2.0 3.0 ) .`+"\n")),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				assert.Equal(t, 1, bg.IRINodeCount())
				var hasTriple bool
				assert.NoError(t, bg.ForIRINodes(func(d IBNode) error {
					return d.ForAll(func(_ int, s, p IBNode, o Term) error {
						hasTriple = true
						if assert.IsType(t, &literalCollection{}, o) {
							o := o.(*literalCollection)
							if assert.Equal(t, 3, o.MemberCount()) {
								assert.Equal(t, Double(1.0), o.Member(0).(Double))
								assert.Equal(t, Double(2.0), o.Member(1).(Double))
								assert.Equal(t, Double(3.0), o.Member(2).(Double))
							}
						}
						return nil
					})
				}))
				assert.True(t, hasTriple)
			},
			errorAssertion: assert.NoError,
		},
		{
			name: "string_collection",
			args: toArgs(fromTurtleContent(t,
				`<http://example.org/point3d> <http://example.org/stuff/1.0/coords> ( "str1" "str2" ) .`+"\n")),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				assert.Equal(t, 1, bg.IRINodeCount())
				var hasTriple bool
				assert.NoError(t, bg.ForIRINodes(func(d IBNode) error {
					return d.ForAll(func(_ int, s, p IBNode, o Term) error {
						hasTriple = true
						if assert.IsType(t, &literalCollection{}, o) {
							o := o.(*literalCollection)
							if assert.Equal(t, 2, o.MemberCount()) {
								assert.Equal(t, String("str1"), o.Member(0).(String))
								assert.Equal(t, String("str2"), o.Member(1).(String))
							}
						}
						return nil
					})
				}))
				assert.True(t, hasTriple)
			},
			errorAssertion: assert.NoError,
		},
		{
			name: "mixed_literal_collection",
			args: toArgs(fromTurtleContent(t,
				`<http://example.org/point3d> <http://example.org/stuff/1.0/coords> ( 1.0 "2.0" 3.0 ) .`+"\n")),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				assert.Equal(t, 1, bg.IRINodeCount())
				var hasTriple bool
				assert.NoError(t, bg.ForIRINodes(func(d IBNode) error {
					return d.ForAll(func(_ int, s, p IBNode, o Term) error {
						hasTriple = true
						if assert.IsType(t, &ibNode{}, o) {
							col, ok := o.(IBNode).AsCollection()
							if assert.True(t, ok, "expect o a TermCollection") {
								assert.Equal(t, 3, col.MemberCount())
							}
						}
						return nil
					})
				}))
				assert.True(t, hasTriple)
			},
			errorAssertion: assert.NoError,
		},
		{
			name: "node_collection",
			args: toArgs(fromTurtleContent(t,
				`<http://content.example.org/basket> <http://example.org/stuff/1.0/hasFruit> _:b0 .
_:b0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://example.org/banana> .
_:b0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b1 .
_:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://example.org/apple> .
_:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:b2 .
_:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <http://example.org/pear> .
_:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
`)),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				assert.Equal(t, 1, bg.IRINodeCount())
				var hasTriple bool
				assert.NoError(t, bg.ForIRINodes(func(d IBNode) error {
					return d.ForAll(func(_ int, s, p IBNode, o Term) error {
						hasTriple = true
						if assert.IsType(t, &ibNode{}, o) {
							col, ok := o.(IBNode).AsCollection()
							if assert.True(t, ok, "expect o a TermCollection") {
								assert.Equal(t, 3, col.MemberCount())
							}
						}
						return nil
					})
				}))
				assert.True(t, hasTriple)
			},
			errorAssertion: assert.NoError,
		},
		{
			name: "collection_with_extra_triple",
			args: toArgs(fromTurtleContent(t,
				`@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
		<http://content.example.org/node1>
			<http://example.org/stuff/1.0/path>
				[ rdf:first <http://content.example.org/node2> ;
				  rdf:rest ( <http://content.example.org/node3> ) ;
				  <http://example.org/stuff/1.0/extra>
						[ <http://example.org/stuff/1.0/info> "info2" , "info1" ]
				] .
		`)),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				n1 := bg.GetIRINodeByFragment("")
				assert.NotNil(t, n1)
				var colExtra IBNode
				assert.NoError(t, n1.ForAll(func(_ int, s, p IBNode, o Term) error {
					if s == n1 {
						if assert.IsType(t, &ibNode{}, o) {
							col, ok := o.(IBNode).AsCollection()
							if assert.True(t, ok) {
								assert.Equal(t, 2, col.MemberCount())
								assert.NoError(t, col.(*ibNode).ForAll(func(_ int, s, p IBNode, o Term) error {
									if s == col.(IBNode) && !p.Is(rdfFirst) {
										colExtra = p
									}
									return nil
								}))
							}
						}
					}
					return nil
				}))
				assert.Equal(t, "http://example.org/stuff/1.0/extra", colExtra.IRI().String())
			},
			errorAssertion: assert.NoError,
		},
		{
			name: "collection_with_literal_middle_extra_triple",
			args: toArgs(fromTurtleContent(t,
				`@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
		<http://content.example.org/node1>
			<http://example.org/stuff/1.0/path>
				[ rdf:first "val1" ;
				  rdf:rest
					[ rdf:first "val2" ;
					  rdf:rest rdf:nil ;
					  <http://example.org/stuff/1.0/extra> "info2"
					]
				] .
		`)),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				assert.Equal(t, 1, bg.IRINodeCount())
				n1 := bg.GetIRINodeByFragment("")
				assert.NotNil(t, n1)
				var collectionTested bool
				assert.NoError(t, n1.ForAll(func(_ int, s, p IBNode, o Term) error {
					if s == n1 {
						if assert.IsType(t, &ibNode{}, o) {
							_, ok := o.(IBNode).AsCollection()
							assert.False(t, ok)
							collectionTested = true
						}
					}
					return nil
				}))
				assert.True(t, collectionTested)

			},
			errorAssertion: assert.NoError,
		},
		{
			name: "collection_with_middle_extra_triple",
			args: toArgs(fromTurtleContent(t,
				`@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
		<http://content.example.org/node1>
			<http://example.org/stuff/1.0/path>
				[ rdf:first <http://content.example.org/node3> ;
				  rdf:rest
					[ rdf:first <http://content.example.org/node3> ;
					  rdf:rest rdf:nil ;
					  <http://example.org/stuff/1.0/extra> "info2"
					]
				] .
		`)),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				n1 := bg.GetIRINodeByFragment("")
				assert.NotNil(t, n1)
				var collectionTested bool
				assert.NoError(t, n1.ForAll(func(_ int, s, p IBNode, o Term) error {
					if s == n1 {
						if assert.IsType(t, &ibNode{}, o) {
							_, ok := o.(IBNode).AsCollection()
							assert.False(t, ok)
							collectionTested = true
						}
					}
					return nil
				}))
				assert.True(t, collectionTested)
			},
			errorAssertion: assert.NoError,
		},
		{
			name: "collection_with_literal_collections",
			args: toArgs(fromTurtleContent(t,
				`@prefix :      <urn:uuid:50643e1b-a652-4a7a-9b52-a91c0425ab1a#> .
@prefix stuff: <http://ontology.semanticstep.net/stuff#> .

:node1  stuff:p1  ( ( 1.0 1.5 2.0 ) ( 3.0 2.5 4.0 ) ) .`)),
			stageAssertion: func(t *testing.T, stage Stage) {
				bg := stage.getFirstLoadedGraph()
				assert.NotNil(t, bg)
				assert.Equal(t, 2, bg.IRINodeCount())
				n1 := bg.GetIRINodeByFragment("node1")
				assert.NotNil(t, n1)
				var collectionTested bool
				assert.NoError(t, n1.ForAll(func(_ int, s, p IBNode, o Term) error {
					if s == n1 && assert.IsType(t, &ibNode{}, o) {
						collectionTested = true
						col1, ok := o.(IBNode).AsCollection()
						if !assert.True(t, ok) {
							return nil
						}
						col1.ForMembers(func(_ int, o Term) {
							if !assert.IsType(t, &literalCollection{}, o) {
								return
							}
						})

					}
					return nil
				}))
				assert.True(t, collectionTested)

			},
			errorAssertion: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.rdr == nil {
				t.Skip()
			}
			// stage, content, mixedReferenced, _, err := fromReaderToStage(tt.args.rdr, tt.args.parseError)
			// if tt.errorAssertion(t, err) {
			// 	tt.stageAssertion(t, stage)
			// 	tt.contentAssertion(t, content)
			// 	tt.mixedReferencedAssertion(t, mixedReferenced)
			// }
			st, err := fromReaderToStage(tt.args.rdr, tt.args.parseError)
			if tt.errorAssertion(t, err) {
				tt.stageAssertion(t, st)
			}
		})
	}
}

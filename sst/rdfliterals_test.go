// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestFromReaderToStage_UUIDNamedGraphLiterals(t *testing.T) {
	type args struct {
		tripleInput string
		graphID     uuid.UUID
		readHandler readErrorHandler
	}
	tests := []struct {
		name           string
		args           args
		errorAssertion assert.ErrorAssertionFunc
		graphAssertion assert.ValueAssertionFunc
	}{
		{
			name: "simple_string",
			args: args{
				tripleInput: "<urn:uuid:bbfd21e7-b3e8-46ca-9b28-8d582cefb514#thing> <http://www.w3.org/2000/01/rdf-schema#comment> \"This is a\\nlong string\\nwith\\n\\ttabs\\nand spaces.\" .",
				graphID:     uuid.MustParse("bbfd21e7-b3e8-46ca-9b28-8d582cefb514"),
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			graphAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				graph := g.(NamedGraph)
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						assert.Equal(t, "This is a\nlong string\nwith\n\ttabs\nand spaces.", string(o.(String)))
					}
					return nil
				}))
			},
		},
		{
			name: "en_string",
			args: args{
				tripleInput: "<urn:uuid:d8250979-a218-405f-af73-94edf4272fc6#thing> <http://www.w3.org/2000/01/rdf-schema#comment> \"english string\"@en .",
				graphID:     uuid.MustParse("d8250979-a218-405f-af73-94edf4272fc6"),
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			graphAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				graph := g.(NamedGraph)
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						l := o.(LangString)
						assert.Equal(t, "english string", l.Val)
						assert.Equal(t, "en", l.LangTag)
					}
					return nil
				}))
			},
		},
		{
			name: "boolean_true",
			args: args{
				tripleInput: "<urn:uuid:bbfd21e7-b3e8-46ca-9b28-8d582cefb514#thing> <http://www.w3.org/2000/01/rdf-schema#comment> true .",
				graphID:     uuid.MustParse("bbfd21e7-b3e8-46ca-9b28-8d582cefb514"),
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			graphAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				graph := g.(NamedGraph)
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						assert.Equal(t, true, bool(o.(Boolean)))
					}
					return nil
				}))
			},
		},
		{
			name: "boolean_false",
			args: args{
				tripleInput: "<urn:uuid:bbfd21e7-b3e8-46ca-9b28-8d582cefb514#thing> <http://www.w3.org/2000/01/rdf-schema#comment> \"false\"^^<http://www.w3.org/2001/XMLSchema#boolean> .",
				graphID:     uuid.MustParse("bbfd21e7-b3e8-46ca-9b28-8d582cefb514"),
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			graphAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				graph := g.(NamedGraph)
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						assert.Equal(t, false, bool(o.(Boolean)))
					}
					return nil
				}))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTripleReader(strings.NewReader(tt.args.tripleInput), RdfFormatTurtle)
			st, err := fromReaderToStage(reader, tt.args.readHandler)
			tt.errorAssertion(t, err)
			tt.graphAssertion(t, st.NamedGraphs()[0])
		})
	}
}

func TestFromReaderToStage_IRINamedGraphLiterals(t *testing.T) {
	type args struct {
		tripleInput string
		readHandler readErrorHandler
	}
	tests := []struct {
		name           string
		args           args
		errorAssertion assert.ErrorAssertionFunc
		stageAssertion assert.ValueAssertionFunc
	}{
		{
			name: "simple_string",
			args: args{
				tripleInput: "<http://example.com/graph#thing> <http://www.w3.org/2000/01/rdf-schema#comment> \"This is a\\nlong string\\nwith\\n\\ttabs\\nand spaces.\" .",
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			stageAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				st := g.(Stage)
				graph := st.NamedGraphs()[0]
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						assert.Equal(t, "This is a\nlong string\nwith\n\ttabs\nand spaces.", string(o.(String)))
					}
					return nil
				}))
			},
		},
		{
			name: "en_string",
			args: args{
				tripleInput: "<http://example.com/graph#thing> <http://www.w3.org/2000/01/rdf-schema#comment> \"english string\"@en .",
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			stageAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				st := g.(Stage)
				graph := st.NamedGraphs()[0]
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						l := o.(LangString)
						assert.Equal(t, "english string", l.Val)
						assert.Equal(t, "en", l.LangTag)
					}
					return nil
				}))
			},
		},
		{
			name: "boolean_true",
			args: args{
				tripleInput: "<http://example.com/graph#thing> <http://www.w3.org/2000/01/rdf-schema#comment> true .",
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			stageAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				st := g.(Stage)
				graph := st.NamedGraphs()[0]
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						assert.Equal(t, true, bool(o.(Boolean)))

					}
					return nil
				}))
			},
		},
		{
			name: "boolean_false",
			args: args{
				tripleInput: "<http://example.com/graph#thing> <http://www.w3.org/2000/01/rdf-schema#comment> false .",
				readHandler: StrictHandler,
			},
			errorAssertion: assert.NoError,
			stageAssertion: func(t assert.TestingT, g interface{}, _ ...interface{}) bool {
				st := g.(Stage)
				graph := st.NamedGraphs()[0]
				assert.Equal(t, 2, graph.IRINodeCount())
				d := graph.GetIRINodeByFragment("thing")
				assert.NotNil(t, d)
				return assert.NoError(t, d.ForAll(func(_ int, s, p IBNode, o Term) error {
					if d == s {
						assert.Equal(t, false, bool(o.(Boolean)))
					}
					return nil
				}))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTripleReader(strings.NewReader(tt.args.tripleInput), RdfFormatTurtle)
			st, err := fromReaderToStage(reader, tt.args.readHandler)
			tt.errorAssertion(t, err)
			tt.stageAssertion(t, st)
		})
	}
}

func Test_Big_Literals(t *testing.T) {

}

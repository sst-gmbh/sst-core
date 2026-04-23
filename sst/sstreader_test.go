// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bufio"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadGraphImports(t *testing.T) {
	type args struct {
		r *bufio.Reader
	}
	tests := []struct {
		name           string
		args           args
		errAssertion   assert.ErrorAssertionFunc
		graphAssertion func(*testing.T, NamedGraph)
	}{
		{
			name: "emptyDefaultGraph",
			args: args{
				r: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				assert.Equal(t, "e38c8811-1028-4442-af79-2cce833feb4e", graph.ID().String())
				assert.Len(t, graph.DirectImports(), 0)
			},
		},
		{
			name: "defaultWithNodesAndImports",
			args: args{
				r: bb("SST-1.0\x00\x2durn:uuid:39c59163-2526-4fc4-8c56-644d1b3b81d4" +
					"\x02\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1" +
					"\x2durn:uuid:c9137479-b4bf-4870-8e16-be6936513d58\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x00\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				assert.Equal(t, 1, graph.IRINodeCount())
				assert.Len(t, graph.DirectImports(), 2)
			},
		},
		// {
		// 	name: "uriGraphWithImport",
		// 	args: args{
		// 		r: bb("SST-1.0\x00\x1ehttp://www.w3.org/2002/07/owl#" +
		// 			"\x01\x25http://www.w3.org/2000/01/rdf-schema#\x00\x00\x00\x00"),
		// 	},
		// 	errAssertion: assert.NoError,
		// 	graphAssertion: func(t *testing.T, graph NamedGraph) {
		// 		assert.Equal(t, "aa33b132-c8e7-58cd-8b03-40433de08ce9", graph.ID().String())
		// 		assert.Equal(t, 1, graph.IRINodeCount())
		// 		if assert.Len(t, graph.DirectImports(), 1) {
		// 			assert.Equal(
		// 				t,
		// 				"a0f79a94-ba29-5d80-b116-6f6bc02eeb2c",
		// 				graph.DirectImports()[0].ID().String(),
		// 			)
		// 		}
		// 	},
		// },
		{
			name: "defaultAndImportedGraphs",
			args: args{
				r: bb("SST-1.0\x00" +
					"\x2durn:uuid:43e199ee-ac39-46c6-852f-50704fdccaef" +
					"\x02\x2durn:uuid:5184e8b3-0649-493d-8b61-a2a2b42c4f24\x2durn:uuid:d7bb18e3-b830-42dc-97cc-3f3a14317caf" +
					"\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x00\x03sA1\x03\x03sA2\x01\x01\x03sB1\x01\x01\x03sC1\x01" +
					"\x04\x02p1\x02\x02p2\x02\x02t1\x01\x02t2\x01" +
					"\x03\x04\x06\x05\x02\x05\x03\x00\x01\x04\x07\x00"),
			},
			errAssertion: assert.NoError,
			graphAssertion: func(t *testing.T, graph NamedGraph) {
				assert.Equal(t, "43e199ee-ac39-46c6-852f-50704fdccaef", graph.ID().String())
				directImports := graph.DirectImports()
				if assert.Len(t, directImports, 2) {
					i0, i1 := directImports[0], directImports[1]
					if i0.ID().String() > i1.ID().String() {
						i0, i1 = i1, i0
					}
					assert.Equal(t, "5184e8b3-0649-493d-8b61-a2a2b42c4f24", i0.ID().String())
					assert.Equal(t, "d7bb18e3-b830-42dc-97cc-3f3a14317caf", i1.ID().String())
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sstReadGraphImports(tt.args.r)
			if tt.errAssertion(t, err) {
				if tt.graphAssertion != nil {
					tt.graphAssertion(t, got)
				}
			}
		})
	}
}

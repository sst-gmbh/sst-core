// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SortAndCountTriples(t *testing.T) {
	l1 := literal{
		typedResource: typedResource{&literalTypeString.ibNode},
		value:         "val1",
	}
	type fields struct {
		ibNode *ibNode
	}
	type args struct {
		incImportedRefCount func(t IBNode)
		incExternalRefCount func(t IBNode)
	}
	type test struct {
		fields              fields
		args                args
		wantCountsAssertion func(t *testing.T, c ibNodeWriteCounts)
	}
	tests := []struct {
		name string
		test
	}{
		// {
		// 	name: "referenced_literal_triple_with_tracked_predicate",
		// 	test: func() test {
		// 		var s1c1, s2c1, p1c1 externIBNode
		// 		c1 := namedGraph{
		// 			triplexStorage: []triplex{
		// 				{p: &p1c1.ibNode, t: &s2c1.typedResource},
		// 				{},
		// 				{p: &p1c1.ibNode, t: &l1.typedResource},
		// 				{p: &p1c1.ibNode, t: &s1c1.typedResource},
		// 				{p: &s1c1.ibNode, t: &s2c1.typedResource},
		// 				{p: &s1c1.ibNode, t: &l1.typedResource},
		// 				{p: &s2c1.ibNode, t: &s1c1.typedResource},
		// 			},
		// 			triplexKinds: []uint{0b_10_10_10_00_00_00_01},
		// 		}
		// 		s1c1 = externIBNode{
		// 			ibNode: ibNode{
		// 				ng:           &c1,
		// 				triplexStart: 0,
		// 				triplexEnd:   3,
		// 				flags:        ibNodeFlagExtern,
		// 			},
		// 			fragment: "s1",
		// 		}
		// 		s2c1 = externIBNode{
		// 			ibNode: ibNode{
		// 				ng:           &c1,
		// 				triplexStart: 3,
		// 				triplexEnd:   4,
		// 				flags:        ibNodeFlagExtern,
		// 			},
		// 			fragment: "p1",
		// 		}
		// 		p1c1 = externIBNode{
		// 			ibNode: ibNode{
		// 				ng:           &c1,
		// 				triplexStart: 4,
		// 				triplexEnd:   7,
		// 				flags:        ibNodeFlagExtern,
		// 			},
		// 			fragment: "p1",
		// 		}
		// 		var refCount int
		// 		return test{
		// 			fields: fields{
		// 				ibNode: &s1c1.ibNode,
		// 			},
		// 			args: args{
		// 				incImportedRefCount: func(t IBNode) { refCount++ },
		// 				incExternalRefCount: func(t IBNode) { refCount++ },
		// 			},
		// 			wantCountsAssertion: func(t *testing.T, c ibNodeWriteCounts) {
		// 				assert.Equal(t, 0, c.MemberSubCount)
		// 				assert.Equal(t, 0, c.PredSubCount)
		// 				assert.Equal(t, 0, c.MemberLiteralCount)
		// 				assert.Equal(t, 1, c.PredLiteralCount)
		// 				assert.Equal(t, 0, c.PredLiteralCollectionCount)
		// 				assert.Equal(t, 1, c.ExtraCount)
		// 				assert.Equal(t, 0, refCount)
		// 			},
		// 		}
		// 	}(),
		// },
		{
			name: "node_triple_with_tracked_predicate",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				c1 := namedGraph{
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
						{p: &s2c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_10_00_00_00_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 4,
						triplexEnd:   7,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				var refCount int
				return test{
					fields: fields{
						ibNode: &s2c1.ibNode,
					},
					args: args{
						incImportedRefCount: func(t IBNode) { refCount++ },
						incExternalRefCount: func(t IBNode) { refCount++ },
					},
					wantCountsAssertion: func(t *testing.T, c ibNodeWriteCounts) {
						assert.Equal(t, 0, c.termCollectionMemberCount)
						assert.Equal(t, 1, c.nonTermCollectionIBNodeTripleCount)
						assert.Equal(t, 0, c.termCollectionLiteralMemberCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralTripleCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralCollectionCount)
						assert.Equal(t, 0, c.extraCount)
						// assert.Equal(t, 0, refCount)
					},
				}
			}(),
		},
		{
			name: "triples_for_tracked_predicate",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				c1 := namedGraph{
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
						{p: &s2c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_10_00_00_00_01},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 4,
						triplexEnd:   7,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				var refCount int
				return test{
					fields: fields{
						ibNode: &p1c1.ibNode,
					},
					args: args{
						incImportedRefCount: func(t IBNode) { refCount++ },
						incExternalRefCount: func(t IBNode) { refCount++ },
					},
					wantCountsAssertion: func(t *testing.T, c ibNodeWriteCounts) {
						assert.Equal(t, 0, c.termCollectionMemberCount)
						assert.Equal(t, 0, c.nonTermCollectionIBNodeTripleCount)
						assert.Equal(t, 0, c.termCollectionLiteralMemberCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralTripleCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralCollectionCount)
						assert.Equal(t, 3, c.extraCount)
						assert.Equal(t, 0, refCount)
					},
				}
			}(),
		},
		{
			name: "referenced_literal_triple_with_imported_tracked_predicate",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				c1 := namedGraph{
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_00_00_00_01},
				}
				c2 := namedGraph{
					triplexStorage: []triplex{
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
						{p: &s2c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_10},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				var refCount int
				return test{
					fields: fields{
						ibNode: &s1c1.ibNode,
					},
					args: args{
						incImportedRefCount: func(t IBNode) { refCount++ },
						incExternalRefCount: func(t IBNode) { refCount++ },
					},
					wantCountsAssertion: func(t *testing.T, c ibNodeWriteCounts) {
						assert.Equal(t, 0, c.termCollectionMemberCount)
						assert.Equal(t, 0, c.nonTermCollectionIBNodeTripleCount)
						assert.Equal(t, 0, c.termCollectionLiteralMemberCount)
						assert.Equal(t, 1, c.nonTermCollectionLiteralTripleCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralCollectionCount)
						assert.Equal(t, 1, c.extraCount)
						assert.Equal(t, 1, refCount)
					},
				}
			}(),
		},
		{
			name: "triples_for_imported_tracked_predicate",
			test: func() test {
				var s1c1, s2c1, p1c1 ibNodeString
				c1 := namedGraph{
					triplexStorage: []triplex{
						{p: &p1c1.ibNode, t: &s2c1.typedResource},
						{},
						{p: &p1c1.ibNode, t: &l1.typedResource},
						{p: &p1c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_00_00_00_01},
				}
				c2 := namedGraph{
					triplexStorage: []triplex{
						{p: &s1c1.ibNode, t: &s2c1.typedResource},
						{p: &s1c1.ibNode, t: &l1.typedResource},
						{p: &s2c1.ibNode, t: &s1c1.typedResource},
					},
					triplexKinds: []uint{0b_10_10_10},
				}
				s1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "s1",
				}
				s2c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c1,
						triplexStart: 3,
						triplexEnd:   4,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				p1c1 = ibNodeString{
					ibNode: ibNode{
						ng:           &c2,
						triplexStart: 0,
						triplexEnd:   3,
						flags:        iriNodeType | stringNode,
					},
					fragment: "p1",
				}
				var refCount int
				return test{
					fields: fields{
						ibNode: &p1c1.ibNode,
					},
					args: args{
						incImportedRefCount: func(t IBNode) { refCount++ },
						incExternalRefCount: func(t IBNode) { refCount++ },
					},
					wantCountsAssertion: func(t *testing.T, c ibNodeWriteCounts) {
						assert.Equal(t, 0, c.termCollectionMemberCount)
						assert.Equal(t, 0, c.nonTermCollectionIBNodeTripleCount)
						assert.Equal(t, 0, c.termCollectionLiteralMemberCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralTripleCount)
						assert.Equal(t, 0, c.nonTermCollectionLiteralCollectionCount)
						assert.Equal(t, 0, c.extraCount)
						assert.Equal(t, 0, refCount)
					},
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := tt.fields.ibNode
			tt.wantCountsAssertion(t, tr.sortAndCountTriples(tt.args.incImportedRefCount, tt.args.incExternalRefCount))
		})
	}
}

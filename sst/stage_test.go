// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStage_NamedGraphIDs(t *testing.T) {
	type fields struct {
		testStage stage
	}
	u := uuid.MustParse
	tests := []struct {
		name   string
		fields fields
		want   []uuid.UUID
	}{
		{
			name: "no imports",
			fields: fields{
				testStage: stage{
					localGraphs: map[uuid.UUID]*namedGraph{
						uuid.MustParse("e00b52a1-9831-4b0f-be50-fd121187708d"): {
							id: uuid.MustParse("e00b52a1-9831-4b0f-be50-fd121187708d"),
						},
					},
					referencedGraphs: map[string]*namedGraph{},
				},
			},
			want: []uuid.UUID{u("e00b52a1-9831-4b0f-be50-fd121187708d")},
		},
		{
			name: "single import",
			fields: fields{
				testStage: stage{
					localGraphs: map[uuid.UUID]*namedGraph{
						uuid.MustParse("c8a6c627-a886-439a-8f0c-dd78e1c1bbd8"): {
							id: uuid.MustParse("c8a6c627-a886-439a-8f0c-dd78e1c1bbd8"),
						},
						uuid.MustParse("fa5741c3-fef3-4d5d-a7e1-ed853a535dea"): {
							id: uuid.MustParse("fa5741c3-fef3-4d5d-a7e1-ed853a535dea"),
						},
					},
					referencedGraphs: map[string]*namedGraph{},
				},
			},
			want: []uuid.UUID{u("c8a6c627-a886-439a-8f0c-dd78e1c1bbd8"), u("fa5741c3-fef3-4d5d-a7e1-ed853a535dea")},
		},
		{
			name: "2 nesting levels",
			fields: fields{
				testStage: stage{
					localGraphs: map[uuid.UUID]*namedGraph{
						uuid.MustParse("6a353684-175e-4f83-bec9-259f7b39ab24"): {
							id: uuid.MustParse("6a353684-175e-4f83-bec9-259f7b39ab24"),
						},
						uuid.MustParse("193d7489-b725-4a18-9094-be296e15872d"): {
							id: uuid.MustParse("193d7489-b725-4a18-9094-be296e15872d"),
						},
						uuid.MustParse("e0fd0b19-3cd7-414a-945a-f9cd899d732f"): {
							id: uuid.MustParse("e0fd0b19-3cd7-414a-945a-f9cd899d732f"),
						},
					},
					referencedGraphs: map[string]*namedGraph{},
				},
			},
			want: []uuid.UUID{u("6a353684-175e-4f83-bec9-259f7b39ab24"), u("193d7489-b725-4a18-9094-be296e15872d"), u("e0fd0b19-3cd7-414a-945a-f9cd899d732f")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.fields.testStage
			var ids []uuid.UUID
			for _, v := range s.NamedGraphs() {
				ids = append(ids, v.ID())
			}
			assert.True(t, compareUUIDSlicesUnordered(tt.want, ids))
		})
	}
}
func compareUUIDSlicesUnordered(slice1, slice2 []uuid.UUID) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	// Use maps to count occurrences of each UUID
	countMap1 := make(map[uuid.UUID]int)
	countMap2 := make(map[uuid.UUID]int)

	for _, u := range slice1 {
		countMap1[u]++
	}
	for _, u := range slice2 {
		countMap2[u]++
	}

	// Compare the two maps
	for key, count1 := range countMap1 {
		if count2, found := countMap2[key]; !found || count1 != count2 {
			return false
		}
	}

	return true
}

type noError struct{ assert.TestingT }

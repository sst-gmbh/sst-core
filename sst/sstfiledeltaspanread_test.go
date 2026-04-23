// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func bb(str string) *bufio.Reader {
	return bufio.NewReader(bytes.NewBuffer([]byte(str)))
}

func Test_deltaNodeSpanT_countDeltas_readEntries(t *testing.T) {
	type args struct {
		rBase     *bufio.Reader
		rDiffs    []*bufio.Reader
		nodeSpans *uint
		low       int
		high      int
	}
	type posPred struct {
		pos  int
		pred uint
	}
	type test struct {
		name          string
		args          args
		countFunc     func(*deltaNodeSpanT, *bufio.Reader, *bufio.Reader, *uint, int) (int, error)
		wantCount     int
		wantPosPreds  []posPred
		assertion     assert.ErrorAssertionFunc
		postAssertion func(t *testing.T, tt *test)
	}
	tests := []test{
		{
			name: "span_over_nodes",

			args: args{
				rBase:     bb("\x01\x0a"),
				rDiffs:    []*bufio.Reader{bb("\x02")},
				nodeSpans: func() *uint { v := uint(0); return &v }(),
				low:       -1,
				high:      1,
			},
			countFunc:    (*deltaNodeSpanT).countDeltas,
			wantCount:    1,
			wantPosPreds: []posPred{{0, undefPred}, {-1, 10}, {0, undefPred}},
			assertion:    assert.NoError,
			postAssertion: func(t *testing.T, tt *test) {
				// assert.Equal(t, []uint{1}, tt.args.nodeSpans)
			},
		},
		{
			name: "span_over_literals",
			args: args{
				rBase:     bb("\x01\x0b"),
				rDiffs:    []*bufio.Reader{bb("")},
				nodeSpans: func() *uint { v := uint(1); return &v }(),
				low:       -1,
				high:      1,
			},
			countFunc:    (*deltaNodeSpanT).countLiteralDeltas,
			wantCount:    1,
			wantPosPreds: []posPred{{0, undefPred}, {-1, 11}, {0, undefPred}},
			assertion:    assert.NoError,
			postAssertion: func(t *testing.T, tt *test) {
				// assert.Equal(t, []uint{1}, tt.args.nodeSpans)
			},
		},
		{
			name: "prev_span_over_nodes",
			args: args{
				rBase:     bb("\x01\x0a"),
				rDiffs:    []*bufio.Reader{bb("")},
				nodeSpans: func() *uint { v := uint(2); return &v }(),
				low:       -1,
				high:      1,
			},
			countFunc:    (*deltaNodeSpanT).countDeltas,
			wantCount:    1,
			wantPosPreds: []posPred{{0, undefPred}, {-1, 10}, {0, undefPred}},
			assertion:    assert.NoError,
			postAssertion: func(t *testing.T, tt *test) {
				// assert.Equal(t, []uint{1}, tt.args.nodeSpans)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := deltaNodeSpanT{}
			count, err := tt.countFunc(&c, tt.args.rBase, tt.args.rDiffs[0], tt.args.nodeSpans, tt.args.low)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, count)
			var posPreds []posPred
			if tt.assertion(t, c.readEntries(tt.args.rBase, tt.args.rDiffs[0], tt.args.nodeSpans, tt.args.low,
				func(p int, r *bufio.Reader, f diffEntryWithFlags, pred uint) error {
					switch f.diffEntry() {
					case diffEntrySame, diffEntryTripleModified:
						posPreds = append(posPreds, posPred{p, pred})
					case diffEntryRemoved:
					case diffEntryAdded:
						posPreds = append(posPreds, posPred{p, pred})
					}
					return nil
				})) {
				assert.Equal(t, tt.wantPosPreds, posPreds)
				tt.postAssertion(t, &tt)
			}
		})
	}
}

// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_lineNumberReader_Read(t *testing.T) {
	type fields struct {
		br sliceReader
		l  int
	}
	type args struct {
		p []byte
	}
	assertEOF := func(t assert.TestingT, err error, i ...interface{}) bool {
		return assert.ErrorIs(t, err, io.EOF, i)
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantN     []int
		wantLine  []int
		assertion []assert.ErrorAssertionFunc
	}{
		{
			name: "read_with_nl",
			fields: fields{
				br: bufio.NewReader(bytes.NewBufferString("one\n\ntwo.\n")),
				l:  0,
			},
			args: args{
				p: make([]byte, 64),
			},
			wantN:    []int{4, 1, 5, 0},
			wantLine: []int{1, 2, 3, 3},
			assertion: []assert.ErrorAssertionFunc{
				assert.NoError, assert.NoError, assert.NoError, assertEOF,
			},
		},
		{
			name: "read_without_nl",
			fields: fields{
				br: bufio.NewReader(bytes.NewBufferString("one\n\ntwo.")),
				l:  0,
			},
			args: args{
				p: make([]byte, 64),
			},
			wantN:    []int{4, 1, 4, 0},
			wantLine: []int{1, 2, 3, 3},
			assertion: []assert.ErrorAssertionFunc{
				assert.NoError, assert.NoError, assert.NoError, assertEOF,
			},
		},
		{
			name: "read_without_nl_small_buf",
			fields: fields{
				br: bufio.NewReader(bytes.NewBufferString("one\n\ntwo")),
				l:  0,
			},
			args: args{
				p: make([]byte, 2),
			},
			wantN:    []int{2, 2, 1, 2, 1, 0},
			wantLine: []int{1, 1, 2, 3, 3, 3},
			assertion: []assert.ErrorAssertionFunc{
				assert.NoError, assert.NoError, assert.NoError, assert.NoError, assert.NoError, assertEOF,
			},
		},
		{
			name: "read_with_nl_overflow_buf",
			fields: fields{
				br: bufio.NewReaderSize(bytes.NewBufferString("0123456789abcdefg\n\ntwo.\n"), 16),
				l:  0,
			},
			args: args{
				p: make([]byte, 64),
			},
			wantN:    []int{16, 2, 1, 5, 0},
			wantLine: []int{1, 1, 2, 3, 3},
			assertion: []assert.ErrorAssertionFunc{
				assert.NoError, assert.NoError, assert.NoError, assert.NoError, assertEOF,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &lineNumberReader{
				reader: tt.fields.br,
				l:      tt.fields.l,
			}
			for i, wantN := range tt.wantN {
				gotN, err := r.Read(tt.args.p)
				tt.assertion[i](t, err, "want loop index %d", i)
				assert.Equal(t, wantN, gotN, "want loop index %d", i)
				assert.Equal(t, tt.wantLine[i], r.line(), "want loop index %d", i)
			}
		})
	}
}

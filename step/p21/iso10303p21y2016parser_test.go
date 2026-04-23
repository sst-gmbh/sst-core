// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package p21

import (
	"bufio"
	"os"
	"testing"

	"git.semanticstep.net/x/sst/sst_test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_yyParserImpl_Parse(t *testing.T) {
	type args struct {
		src *bufio.Reader
	}
	tests := []struct {
		name      string
		args      args
		assertion func(t testing.TB, yyrcvr yyParser, l *lexer)
		want      int
	}{
		{
			name: "AP214miniPart",
			args: func() args {
				f, err := os.Open("testdata/AP214miniPart.stp")
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, f.Close())
				})
				return args{
					src: bufio.NewReader(f),
				}
			}(),
			want: 0,
			assertion: func(t testing.TB, _ yyParser, l *lexer) {
				testutil.DetailLogf(t, "Errors: ", l.errs)
				if testutil.DetailLogEnabled() {
					dumpParserResult(l.parserResult)
				}
			},
		},
		{
			name: "AP214miniPartExtended",
			args: func() args {
				f, err := os.Open("testdata/AP214miniPartExtended.stp")
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, f.Close())
				})
				return args{
					src: bufio.NewReader(f),
				}
			}(),
			want: 0,
			assertion: func(t testing.TB, _ yyParser, l *lexer) {
				testutil.DetailLogf(t, "Errors: ", l.errs)
				if testutil.DetailLogEnabled() {
					dumpParserResult(l.parserResult)
				}
			},
		},
		{
			name: "AP242_DOM",
			args: func() args {
				f, err := os.Open("testdata/dictionaries/AP242_DOM.p21")
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, f.Close())
				})
				return args{
					src: bufio.NewReader(f),
				}
			}(),
			want: 0,
			assertion: func(t testing.TB, _ yyParser, l *lexer) {
				testutil.DetailLogf(t, "Errors: ", l.errs)
			},
		},
		{
			name: "AP242_MIMLF",
			args: func() args {
				f, err := os.Open("testdata/dictionaries/AP242_MIMLF.p21")
				require.NoError(t, err)
				t.Cleanup(func() {
					require.NoError(t, f.Close())
				})
				return args{
					src: bufio.NewReader(f),
				}
			}(),
			want: 0,
			assertion: func(t testing.TB, _ yyParser, l *lexer) {
				testutil.DetailLogf(t, "Errors: ", l.errs)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			savedStdout := os.Stdout
			t.Cleanup(func() { os.Stdout = savedStdout })
			os.Stdout = nil
			l := newLexer(tt.args.src, newTestSstData(t))
			yyrcvr := yyNewParser()
			// yyDebug = 4
			assert.Equal(t, tt.want, yyrcvr.Parse(l))
			os.Stdout = savedStdout
			if tt.assertion != nil {
				tt.assertion(t, yyrcvr, l)
			}
		})
	}
}

func TestMain(m *testing.M) {
	testutil.DetailLogMain(m)
}

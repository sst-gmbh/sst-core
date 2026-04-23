// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"bufio"
	"bytes"
	"testing"

	"git.semanticstep.net/x/sst/sst"
	"github.com/stretchr/testify/assert"
)

func TestWriteDiff(t *testing.T) {
	type args struct {
		rFrom *bufio.Reader
		rTo   *bufio.Reader
	}
	tests := []struct {
		name      string
		args      args
		wantW     string
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "empty graphs",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
				rTo:   bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
			},
			wantW:     "\x00\x00\x00\x00\x00\x00\x00\x00",
			assertion: assert.NoError,
		},
		{
			name: "empty graph to graph with nodes no imports",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x01" +
					"\x1fhttp://semanticstep.net/schema#\x02\x01\x02s1\x01\x02s2\x02\x01\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
			},
			wantW: "\x00\x00\x00\x01\x02\x1fhttp://semanticstep.net/schema#" +
				"\x00\x02\x00\x01\x02\x02s1\x02\x02\x02s2\x04\x02\x02\x00\x02\x02\x02p1\x02\x02\x02p2\x02" +
				"\x01\x01\x0e\x01\x01\x00\x01\x01\x12\x02\x01\x00\x02",
			assertion: assert.NoError,
		},
		{
			name: "graph with nodes no imports to empty graph",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x01" +
					"\x1fhttp://semanticstep.net/schema#\x02\x01\x02s1\x01\x02s2\x02\x01\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x00\x00\x00"),
			},
			wantW: "\x00\x00\x01\x00\x01\x1fhttp://semanticstep.net/schema#" +
				"\x02\x00\x01\x00\x01\x02s1\x01\x01\x02s2\x03\x01\x01\x02\x00\x01\x02p1\x01\x01\x02p2\x01" +
				"\x03\x00\x0d\x01\x01\x00\x03\x00\x11\x02\x01\x00\x02",
			assertion: assert.NoError,
		},
		{
			name: "graph with nodes no imports to graph with imports",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e\x00\x01" +
					"\x1fhttp://semanticstep.net/schema#\x02\x01\x02s1\x01\x02s2\x02\x01\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e" +
					"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
			},
			wantW: "\x00\x01\x02-urn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1" +
				"\x00\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x02\x04\x02",
			assertion: assert.NoError,
		},
		{
			name: "graph with imports to graph with literals",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:d270203d-2598-4a71-80b1-50576a1fda84" +
					"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:d270203d-2598-4a71-80b1-50576a1fda84\x00\x01" +
					"\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x03\x03\x02p1\x01\x02p2\x01\x02p3\x01\x00" +
					"\x03\x01\x00\x04str1\x02\x04\x85\xd2\xa9\x9a\x21\x03\x05?\xf3333333"),
			},
			wantW: "\x01\x00\x01-urn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1" +
				"\x00\x00\x00\x01\x01\x00\x01\x00\x03\x04\x01\x02s2\x03\x01\x01\x00\x00\x00\x01\x00\x02\x02\x02p3\x02" +
				"\x03\x00\x0d\x01\x01\x03\x0e\x00\x04str1" +
				"\x12\x04\x85\xd2\xa9\x9a\x21\x16\x05?\xf3333333\x03\x00\x11\x02\x01\x00\x02",
			assertion: assert.NoError,
		},
		{
			name: "graph with lesser literal value modification",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x14"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x16"),
			},
			wantW: "\x00\x00\x00\x00\x00" +
				"\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x01\x00\x03\x01\x05\x04\x14\x06\x04\x16",
			assertion: assert.NoError,
		},
		{
			name: "graph with greater literal value modification",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x14"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x12"),
			},
			wantW: "\x00\x00\x00\x00\x00" +
				"\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x01\x01\x00\x03\x01\x06\x04\x12\x05\x04\x14",
			assertion: assert.NoError,
		},
		{
			name: "graph with literal to graph with literal and literal collection",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x01\x01\x02p1\x01\x00\x01\x01\x04\x14"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x02\x02\x02p1\x01\x02p2\x01\x00" +
					"\x02\x01\x04\x14\x02\x7f\x02\x00\x03cl1\x00\x03cl2"),
			},
			wantW: "\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x03\x02\x00\x01\x00\x01\x02\x02p2\x02\x01" +
				"\x00\x01\x01\x04\x0a\x7f\x02\x00\x03cl1\x00\x03cl2",
			assertion: assert.NoError,
		},
		{
			name: "graph with literal collection modification",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x02\x02\x02p1\x01\x02p2\x01\x00" +
					"\x02\x01\x04\x14\x02\x7f\x02\x00\x03cl0\x00\x03cl1"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:2a49b2d9-ddbd-49bc-ba58-2fbea7cf6792" +
					"\x00\x01\x1fhttp://semanticstep.net/schema#\x01\x00\x02s1\x02\x02\x02p1\x01\x02p2\x01\x00" +
					"\x02\x01\x04\x14\x02\x7f\x02\x00\x03cl2\x00\x03cl3"),
			},
			wantW: "\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x01\x00\x03\x01\x04\x09" +
				"\x7f\x02\x00\x03cl0\x00\x03cl1\x0a\x7f\x02\x00\x03cl2\x00\x03cl3",
			assertion: assert.NoError,
		},
		{
			name: "graph with imports to graph with imported graphs",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:d270203d-2598-4a71-80b1-50576a1fda84" +
					"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:d270203d-2598-4a71-80b1-50576a1fda84" +
					"\x02\x2durn:uuid:5184e8b3-0649-493d-8b61-a2a2b42c4f24\x2durn:uuid:d7bb18e3-b830-42dc-97cc-3f3a14317caf" +
					"\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x00\x03sA1\x03\x03sA2\x01\x01\x03sB1\x01\x01\x03sC1\x01" +
					"\x04\x02p1\x02\x02p2\x02\x02t1\x01\x02t2\x01" +
					"\x03\x04\x06\x05\x02\x05\x03\x00\x01\x04\x07\x00"),
			},
			wantW: "\x01\x02\x02-urn:uuid:5184e8b3-0649-493d-8b61-a2a2b42c4f24" +
				"\x01-urn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1" +
				"\x02-urn:uuid:d7bb18e3-b830-42dc-97cc-3f3a14317caf" +
				"\x00\x00\x00\x01\x02\x02\x01\x00\x01\x02s1\x01\x01\x02s2\x03\x02\x03sA1\x06\x02\x03sA2\x02" +
				"\x01\x01\x00\x01\x02\x03sB1\x02\x00\x00\x00\x01\x02\x03sC1\x02" +
				"\x00\x02\x03\x02\x03\x02\x02\x02t1\x02\x02\x02t2\x02" +
				"\x03\x00\x1d\x01\x01\x00\x03\x00\x21\x04\x01\x00\x01\x03\x1e\x09\x22\x05\x22\x06\x01\x00" +
				"\x01\x01\x1e\x0a\x01\x00\x02",
			assertion: assert.NoError,
		},
		{
			name: "identical graphs with imports",
			args: args{
				rFrom: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e" +
					"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
				rTo: bb("SST-1.0\x00\x2durn:uuid:e38c8811-1028-4442-af79-2cce833feb4e" +
					"\x01\x2durn:uuid:91bedd7d-8f59-48e8-98fa-c06ec328e4a1\x01\x1fhttp://semanticstep.net/schema#" +
					"\x02\x01\x02s1\x01\x02s2\x02\x01\x00\x02\x02p1\x01\x02p2\x01" +
					"\x01\x03\x01\x00\x01\x04\x02\x00\x00\x00"),
			},
			wantW:     "\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x02\x04\x02",
			assertion: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			_, err := sst.SstWriteDiff(tt.args.rFrom, tt.args.rTo, w, true)
			tt.assertion(t, err)
			assert.Equal(t, tt.wantW, w.String())
		})
	}
}

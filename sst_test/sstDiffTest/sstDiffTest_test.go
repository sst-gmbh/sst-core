// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sstDiffTest

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/semanticstep/sst-core/sst"
	"github.com/stretchr/testify/require"
)

// Utility test: generates .sst files from .ttl input for manual inspection or further testing.
// Not intended to verify correctness of logic, but will fail if conversion fails.
func TestReadTTLWriteToSST_AW10_AW20(t *testing.T) {
	basePath := "./testdata"

	tests := []struct {
		name      string
		ttlFile   string
		outputSST string
	}{
		{
			name:      "aw10",
			ttlFile:   filepath.Join(basePath, "aw10.ttl"),
			outputSST: filepath.Join(basePath, "aw10.sst"),
		},
		{
			name:      "aw20",
			ttlFile:   filepath.Join(basePath, "aw20.ttl"),
			outputSST: filepath.Join(basePath, "aw20.sst"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.ttlFile)
			require.NoError(t, err, "Failed to open TTL file")
			defer file.Close()

			ng, err := sst.RdfRead(bufio.NewReader(file), sst.RdfFormatTurtle, sst.StrictHandler, sst.DefaultTriplexMode)
			require.NoError(t, err, "Failed to parse TTL file")

			out, err := os.Create(tt.outputSST)
			require.NoError(t, err, "Failed to create SST file")
			defer func() {
				out.Close()
				_ = os.Remove(tt.outputSST)
			}()

			err = ng.NamedGraphs()[0].SstWrite(out)
			require.NoError(t, err, "Failed to write SST")
		})
	}
}

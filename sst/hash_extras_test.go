// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

//go:build extras

package sst

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParseShort_min_byte_coverage is a brute force test that finds short
// hashes that result in a range of smallest bit overlap.
//
// Notice that test timeout has to be considerably increased for this test.
func TestParseShort_min_byte_coverage(t *testing.T) {
	minSameBytes := 44
	for i1 := 0; i1 < 58; i1++ {
		for i2 := 0; i2 < 58; i2++ {
			for i3 := 0; i3 < 58; i3++ {
				for i4 := 0; i4 < 58; i4++ {
					for i5 := 0; i5 < 58; i5++ {
						for i6 := 0; i6 < 58; i6++ {
							const btcAlphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
							shortHash := string(btcAlphabet[i1]) +
								string(btcAlphabet[i2]) +
								string(btcAlphabet[i3]) +
								string(btcAlphabet[i4]) +
								string(btcAlphabet[i5]) +
								string(btcAlphabet[i6])
							minHash, maxHash, err := hashBase58ParseShort(shortHash)
							assert.NoError(t, err)
							for i := range maxHash {
								if !bytes.HasPrefix(minHash[:], maxHash[:i]) {
									if minSameBytes > i {
										minSameBytes = i
										t.Log(shortHash, minSameBytes)
										break
									} else if i <= 1 {
										t.Log(shortHash, i)
									}
								}
							}
						}
					}
				}
			}
		}
	}
	t.SkipNow()
}

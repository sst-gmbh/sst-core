// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"crypto/rand"
	"testing"

	// btcb58 "github.com/btcsuite/btcutil/base58".
	mrb58 "github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
)

type testValues struct {
	dec []byte
	enc string
}

var (
	n         = 5000000
	testPairs = make([]testValues, 0, n)
)

func initTestPairs() {
	if len(testPairs) > 0 {
		return
	}
	// pre-make the test pairs, so it doesn't take up benchmark time...
	for i := 0; i < n; i++ {
		data := make([]byte, 32)
		_, err := rand.Read(data)
		if err != nil {
			panic(err)
		}
		testPairs = append(testPairs, testValues{dec: data, enc: mrb58.EncodeAlphabet(data, mrb58.BTCAlphabet)})
	}
}

func BenchmarkBase58Encode(b *testing.B) {
	initTestPairs()
	// b.Run("github.com/btcsuite/btcutil/base58", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		btcb58.Encode(testPairs[i].dec)
	// 	}
	// })
	b.Run("github.com/mr-tron/base58", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mrb58.EncodeAlphabet(testPairs[i].dec, mrb58.BTCAlphabet)
		}
	})
}

func BenchmarkBase58Decode(b *testing.B) {
	initTestPairs()

	// b.Run("github.com/btcsuite/btcutil/base58", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		btcb58.Decode(testPairs[i].enc)
	// 	}
	// })
	b.Run("github.com/mr-tron/base58", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := mrb58.DecodeAlphabet(testPairs[i].enc, mrb58.BTCAlphabet)
			assert.NoError(b, err)
		}
	})
}

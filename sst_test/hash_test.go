// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/u6du/go-rfc1924/base85"
)

func Test_Different_Hash(t *testing.T) {
	randomBytes := make([]byte, 32)
	_, err := rand.Read(randomBytes)
	if err != nil {
		fmt.Println("random failed:", err)
		return
	}

	hash := sha256.Sum256(randomBytes)

	fmt.Printf("Original SHA-256 hash ((%d characters)):\n%x\n\n", len(hash)*2, hash)

	// Base58
	base58Encoded := base58.Encode(hash[:])
	fmt.Printf("Base58 encoding result (%d characters):\n%s\n\n", len(base58Encoded), base58Encoded)

	// Base85
	base85Encoded := base85.EncodeToString(hash[:])
	fmt.Printf("Base85 encoding result (%d characters):\n%s\n\n", len(base85Encoded), base85Encoded)

	// compare the lengths of the two encodings
	fmt.Printf("Encoding efficiency comparison (number of characters): Base58 (%d) vs Base85 (%d)\n",
		len(base58Encoded), len(base85Encoded))
}

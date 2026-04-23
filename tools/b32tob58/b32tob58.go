// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// This is a simple tool to convert a base32 encoded string into a base58 encoded string.
package main

import (
	"encoding/base32"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mr-tron/base58"
)

func main() {
	b32, err := b64tob32(os.Args[1])
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(b32)
}

func b64tob32(b32 string) (b58 string, _ error) {
	hashBytes, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(b32))
	if err != nil {
		return "", err
	}

	return base58.EncodeAlphabet(hashBytes, base58.BTCAlphabet), nil
}

// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// This is a simple tool to convert a hex encoded string into a base58 encoded string.
package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/mr-tron/base58"
)

func main() {
	b58, err := hextob58(os.Args[1])
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(b58)
}

func hextob58(h string) (b58 string, _ error) {
	hashBytes, err := hex.DecodeString(h)
	if err != nil {
		return "", err
	}

	return base58.EncodeAlphabet(hashBytes, base58.BTCAlphabet), nil
}

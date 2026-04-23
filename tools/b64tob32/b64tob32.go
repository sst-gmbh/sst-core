// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// This is a simple tool to convert a base64 encoded string into a base32 encoded string.
package main

import (
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"log"
	"os"
)

func main() {
	b32, err := b64tob32(os.Args[1])
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(b32)
}

func b64tob32(b64 string) (b32 string, _ error) {
	hashBytes, err := base64.RawURLEncoding.DecodeString(b64)
	if err != nil {
		return "", err
	}

	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hashBytes), nil
}

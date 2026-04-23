// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"crypto/sha256"
	"errors"

	"github.com/mr-tron/base58" // See https://datatracker.ietf.org/doc/html/draft-msporny-base58
)

const (
	minBase58StringLen     = 32
	maxBase58StringLen     = 44
	base58StringPrePadding = "111111111111"
	// Assert that base58StringPrePadding has length exactly maxBase58StringLen - minBase58StringLen bytes.
	_               = uint(maxBase58StringLen - minBase58StringLen - len(base58StringPrePadding))
	_               = uint(len(base58StringPrePadding) - maxBase58StringLen + minBase58StringLen)
	minBase58Digits = "11111111111111111111111111111111111111111111"
	// Assert that minBase58Digits has length exactly maxBase58StringLen bytes.
	_               = uint(maxBase58StringLen - len(minBase58Digits))
	_               = uint(len(minBase58Digits) - maxBase58StringLen)
	maxBase58Digits = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	maxBase58Value  = "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	// Assert that maxBase58Digits has length exactly maxBase58StringLen bytes.
	_ = uint(maxBase58StringLen - len(maxBase58Digits))
	_ = uint(len(maxBase58Digits) - maxBase58StringLen)
)

// Error that indicates that a byte array has not the correct size for a SHA 256 [HASH] value.
var ErrIllegalHashLength = errors.New("illegal hash length")

var (
	zeroHash = Hash{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	// emptyHash is the SHA-256 hash of an empty string.
	emptyHash = Hash{
		0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
	}

	maxHashValue = Hash{
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}
)

// Hash is an 32 byte object containing a SHA 256 hash value.
// Hash is used throughout SST-Core to identify NamedGraph revisions, dataset revisions and commits.
// SST applications might use this functionality as well for other purposes.
type Hash [sha256.Size]byte

// BytesToHash converts a byte array into a corresponding [Hash] object.
func BytesToHash[K ~[]byte | ~string](hashBytes K) (hash Hash) {
	copy(hash[:], hashBytes)
	return
}

// StringToHash converts a string that contains 44 character long string in Base58 encoding into a SHA 256 value into a corresponding [Hash] object.
func StringToHash(str string) (h Hash, _ error) {
	if len(str) != maxBase58StringLen {
		return h, ErrIllegalHashLength
	}
	b, err := base58.DecodeAlphabet(str, base58.BTCAlphabet)
	if err != nil {
		return h, err
	}
	if len(b) > len(Hash{}) {
		bOverLen := len(b) - len(Hash{})
		for i, z := range b {
			if z != 0x00 || i == bOverLen {
				b = b[i:]
				break
			}
		}
	}
	if len(b) != len(Hash{}) {
		return h, ErrIllegalHashLength
	}
	copy(h[:], b)
	return
}

func hashBase58ParseShort(str string) (minHash, maxHash Hash, _ error) {
	if len(str) >= maxBase58StringLen {
		h, err := StringToHash(str)
		return h, h, err
	}
	minStr := str + minBase58Digits[:maxBase58StringLen-len(str)]
	minHash, err := StringToHash(minStr)
	if err != nil {
		return minHash, maxHash, err
	}
	// Special case if short hash starts as maximum possible base58 string for a Hash.
	if str == maxBase58Value[:len(str)] {
		return minHash, maxHashValue, nil
	}
	maxStr := str + maxBase58Digits[:maxBase58StringLen-len(str)]
	maxHash, err = StringToHash(maxStr)
	if err != nil {
		return minHash, maxHash, err
	}
	return
}

// String returns the Base58 encoded string representation of the Hash.
// It uses the BTCAlphabet for encoding. If the encoded string is shorter
// than maxBase58StringLen, it pads the string with pre-defined padding
// characters to reach the required length.
func (h Hash) String() string {
	hStr := base58.EncodeAlphabet(h[:], base58.BTCAlphabet)
	if len(hStr) < maxBase58StringLen {
		hStr = base58StringPrePadding[:maxBase58StringLen-len(hStr)] + hStr
	}
	return hStr
}

// IsNil returns true if the Hash is the emptyHash value.
// The emptyHash value is the SHA-256 hash of an empty string.
func (h Hash) IsNil() bool {
	return h == emptyHash
}

// HashNil returns the emptyHash value.
// The emptyHash value is the SHA-256 hash of an empty string.
func HashNil() Hash {
	return emptyHash
}

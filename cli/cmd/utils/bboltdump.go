// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

func DumpBboltFromDB(db *bbolt.DB, bucketPath string) error {
	return db.View(func(tx *bbolt.Tx) error {
		pathParts := strings.Split(bucketPath, "/")
		if len(pathParts) == 0 {
			return fmt.Errorf("empty bucket path")
		}

		current := tx.Bucket([]byte(pathParts[0]))
		if current == nil {
			return fmt.Errorf("bucket '%s' not found", pathParts[0])
		}

		for _, part := range pathParts[1:] {
			var key []byte

			if decoded, err := hex.DecodeString(part); err == nil {
				key = decoded
			} else {
				key = []byte(part)
			}

			current = current.Bucket(key)
			if current == nil {
				return fmt.Errorf("sub-bucket '%s' not found", part)
			}
		}

		fmt.Printf("Dumping Bucket: %s\n", bucketPath)
		return printBucket(current, 0, []byte(pathParts[len(pathParts)-1]))
	})
}

// printBucket print bucket recursively, and for different bucket,
// it can print in different format.
func printBucket(b *bbolt.Bucket, level int, bucketName []byte) error {
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			// when v is nil, k is a sub-bucket
			if bytes.Equal(bucketName, []byte("c")) {
				fmt.Printf("%scommit SHA:[%x] (subbucket)\n", indent, k)
			} else if bytes.Equal(bucketName, []byte("dsr")) {
				fmt.Printf("%sDS-SHA:[%x] (subbucket)\n", indent, k)
			} else if bytes.Equal(bucketName, []byte("dl")) {
				if len(k) == 8 {
					fmt.Printf("%slog event number: %x\n", indent, k)
				} else if len(k) > 0 && k[0] == '\x00' {
					fmt.Printf("%sDS-UUID: %x\n", indent, k[1:])
				} else {
					fmt.Printf("%s[%x] (subbucket:%s)\n", indent, k, bucketName)
				}
			} else {
				fmt.Printf("%s[%x] (subbucket:%s)\n", indent, k, bucketName)
			}
			subBucket := b.Bucket(k)
			if subBucket == nil {
				return fmt.Errorf("expected bucket %s not found", k)
			}
			return printBucket(subBucket, level+1, bucketName)
		} else {
			if string(bucketName) == "dsr" {
				if len(k) == 1 && k[0] == '\x00' {
					fmt.Printf("%sdefault NG-SHA: %x\n", indent, v)
				} else if len(k) > 0 && k[0] == '\x00' {
					fmt.Printf("%simported DS-UUID:%x\n", indent, k[1:])
					fmt.Printf("%simported DS-SHA :%x\n", indent, v)
				} else if len(k) > 0 && k[0] == '\x01' {
					fmt.Printf("%sNG-UUID:%x\n", indent, k[1:])
					fmt.Printf("%sNG-SHA :%x\n", indent, v)
				} else {
					fmt.Printf("%sKey: %x, Value: %x\n", indent, k[1:], v)
				}
				// } else if string(bucketName) == "ngr" {
				// 	fmt.Printf("%sNG-SHA: %x\n", indent, k)
				// 	bufferV := bytes.NewBuffer(v)
				// 	reader := bufio.NewReader(bufferV)
				// 	graph, err := SstRead(reader, DefaultTriplexMode)
				// 	if err == nil {
				// 		var bufferW bytes.Buffer
				// 		ioWriter := io.Writer(&bufferW)
				// 		writer := newTripleWriter(ioWriter, RdfFormatTurtle)
				// 		err = toWriter(graph, writer)
				// 		if err != nil {
				// 			log.Panic("ToWriter failed。")
				// 		}
				// 		data := bufferW.Bytes()
				// 		fmt.Printf("%sValue: %s\n", indent, data)
				// 	} else {
				// 		log.Panic("SST Read failed.")
				// 	}

			} else if string(bucketName) == "c" {
				if len(k) > 0 && k[0] == '\x00' {
					fmt.Printf("%scommitted _DS-UUID_: %x \n%scommitted _DS-SHA_: %x \n%sparent _Commit-SHA_: %x\n", indent, k[1:], indent+indent, v[:32], indent+indent, v[32:])
				} else if bytes.Equal(k, []byte("author")) {
					var unixSeconds int64
					buf := bytes.NewBuffer(v[len(v)-8:])
					err := binary.Read(buf, binary.BigEndian, &unixSeconds)
					timeResult := time.Unix(unixSeconds, 0).UTC()
					if err != nil {
						fmt.Println("binary.Read failed:", err)
					}
					fmt.Printf("%sKey: %s, Value: %s %s\n", indent, k, v[:len(v)-8], timeResult)
				} else if bytes.Equal(k, []byte("message")) {
					fmt.Printf("%sKey: %s, Value: %s\n", indent, k, v)
				} else if bytes.Equal(k, []byte("reason")) {
					fmt.Printf("%sKey: %s, Value: %s\n", indent, k, v)
				} else {
					fmt.Printf("%sKey: %s, Value: %x\n", indent, k, v)
				}
			} else if string(bucketName) == "dl" {
				if len(k) > 0 && k[0] == '\x00' {
					if bytes.Equal(v[len(v)-1:], []byte("\x00")) {
						fmt.Printf("%sbranch: %s, Value: %x Created.\n", indent, k[1:], v[:len(v)-1])
					} else {
						fmt.Printf("%sbranch: %s\n%scurrent _Commit_SHA: %x\n%sprevious _Commit_SHA: %x\n", indent, k[1:], indent, v[:32], indent, v[32:])
					}
				} else {
					fmt.Printf("%sKey: %s, Value: %x\n", indent, k, v)
				}
			} else if string(bucketName) == "ds" {
				if len(k) > 0 && k[0] == '\x00' {
					fmt.Printf("%sbranch: %s latest _Commit_SHA: %x\n", indent, k[1:], v)
				} else if len(k) > 0 && k[0] == '\x01' {
					fmt.Printf("%stag name: %s latest _Commit_SHA: %x\n", indent, k[1:], v)
				} else if len(k) > 0 && k[0] == '\x02' {
					fmt.Printf("%s_Commit_SHA_: %s latest _Commit_SHA: %x\n", indent, k[1:], v)
				} else {
					fmt.Printf("%sKey: %s, Value: %x\n", indent, k, v)
				}
			} else {
				fmt.Printf("%sKey: %x, Value: %x\n", indent, k, v)
			}
		}
		return nil
	})
	return err
}

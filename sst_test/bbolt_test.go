// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

/*
	bbolt (formerly known as BoltDB) is a pure key/value storage database written in Go.
	It provides a simple and efficient way to store data.
	bbolt is a single-file database that supports fully serializable transactions.
*/

func TestBbolt(t *testing.T) {
	dir := "testdata/bbolt_test.db"
	defer os.RemoveAll(dir)
	t.Run("TestOpenBbolt", func(t *testing.T) {
		db, err := bbolt.Open(dir, 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
	})

	// Buckets can be thought of as a table or directory within a database.
	// All key-value pairs are stored in a specific bucket.
	t.Run("TestUpdatePut", func(t *testing.T) {
		db, err := bbolt.Open(dir, 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
			return nil
		})

		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			err := b.Put([]byte("key"), []byte("value"))
			if err != nil {
				return fmt.Errorf("put error: %s", err)
			}
			return nil
		})
	})

	t.Run("TestView", func(t *testing.T) {
		db, err := bbolt.Open(dir, 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			v := b.Get([]byte("key"))
			fmt.Printf("The value of 'key' is: %s\n", v)
			return nil
		})
	})

	t.Run("TestUpdateDelete", func(t *testing.T) {
		db, err := bbolt.Open(dir, 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			v := b.Get([]byte("key"))
			fmt.Printf("The value of 'key' is: %s\n", v)
			return nil
		})

		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			err := b.Delete([]byte("key"))
			if err != nil {
				return fmt.Errorf("delete error: %s", err)
			}
			return nil
		})

		db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			v := b.Get([]byte("key"))
			fmt.Printf("The value of 'key' is: %s\n", v)
			return nil
		})
	})

}

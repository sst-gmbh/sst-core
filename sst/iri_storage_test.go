// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestDatasetIRIStorage(t *testing.T) {
	// Create a temporary bbolt database for testing
	db, err := bbolt.Open(t.TempDir()+"/test.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Test data
	testIRI := "http://example.com/dataset/test"
	version5UUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(testIRI))
	version4UUID := uuid.New()

	t.Run("Store and retrieve IRI for Version 5 UUID", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			// Create datasets bucket
			datasets, err := tx.CreateBucketIfNotExists(keyDatasets)
			require.NoError(t, err)

			// Create dataset bucket for Version 5 UUID
			dsBucket, err := createDatasetBucketIfNotExists(datasets, version5UUID)
			require.NoError(t, err)

			// Store IRI for Version 5 UUID
			err = putDatasetIRI(dsBucket, version5UUID, testIRI)
			require.NoError(t, err)

			// Retrieve IRI
			retrievedIRI := getDatasetIRI(dsBucket, version5UUID)
			assert.Equal(t, testIRI, retrievedIRI)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("Get URN-UUID for Version 4 UUID", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			// Create datasets bucket
			datasets, err := tx.CreateBucketIfNotExists(keyDatasets)
			require.NoError(t, err)

			// Create dataset bucket for Version 4 UUID
			dsBucket, err := createDatasetBucketIfNotExists(datasets, version4UUID)
			require.NoError(t, err)

			// Retrieve IRI (should return URN-UUID for Version 4)
			retrievedIRI := getDatasetIRI(dsBucket, version4UUID)
			assert.Equal(t, version4UUID.URN(), retrievedIRI)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("Get empty string for non-Version 4/5 UUID", func(t *testing.T) {
		// Create a nil UUID (Version 0)
		nilUUID := uuid.Nil

		err := db.Update(func(tx *bbolt.Tx) error {
			// Create datasets bucket
			datasets, err := tx.CreateBucketIfNotExists(keyDatasets)
			require.NoError(t, err)

			// Create dataset bucket for nil UUID
			dsBucket, err := createDatasetBucketIfNotExists(datasets, nilUUID)
			require.NoError(t, err)

			retrievedIRI := getDatasetIRI(dsBucket, nilUUID)
			assert.Equal(t, "urn:uuid:00000000-0000-0000-0000-000000000000", retrievedIRI)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("Store empty IRI for Version 5 UUID", func(t *testing.T) {
		// Create a different Version 5 UUID for this test
		testIRI2 := "http://example.com/dataset/test2"
		version5UUID2 := uuid.NewSHA1(uuid.NameSpaceURL, []byte(testIRI2))

		err := db.Update(func(tx *bbolt.Tx) error {
			// Create datasets bucket
			datasets, err := tx.CreateBucketIfNotExists(keyDatasets)
			require.NoError(t, err)

			// Create dataset bucket for Version 5 UUID
			dsBucket, err := createDatasetBucketIfNotExists(datasets, version5UUID2)
			require.NoError(t, err)

			// Store empty IRI
			err = putDatasetIRI(dsBucket, version5UUID2, "")
			require.NoError(t, err)

			// Retrieve IRI (should return empty string)
			retrievedIRI := getDatasetIRI(dsBucket, version5UUID2)
			assert.Equal(t, "", retrievedIRI)

			return nil
		})
		require.NoError(t, err)
	})
}

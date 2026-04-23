// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

const VaultDirName = "document_vault"

// vaultPath constructs the full file path for a given hash in the vault
func vaultPath(vaultDir string, hash Hash) string {
	subDir := fmt.Sprintf("%02X", hash[0])
	fileName := ""
	for _, b := range hash[1:] {
		fileName += fmt.Sprintf("%02x", b)
	}
	return filepath.Join(vaultDir, subDir, fileName)
}

// StoreStreamToVault stores the content from a buffer into the appropriate vault subdirectory.
func storeStreamToVault(buf *bytes.Buffer, vaultDir string, hash Hash) error {
	fullPath := vaultPath(vaultDir, hash)

	// Create subdir if needed
	if err := os.MkdirAll(filepath.Dir(fullPath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create vault subdir: %w", err)
	}

	return os.WriteFile(fullPath, buf.Bytes(), 0o644)
}

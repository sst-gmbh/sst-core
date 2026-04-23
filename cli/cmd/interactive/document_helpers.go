// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package interactive

import (
	"bufio"
	"net/http"
	"path/filepath"
	"strings"
)

// guessMimeType attempts to guess the MIME type based on file name extension.
// If the extension is unknown, falls back to content-based detection.
func guessMimeType(filename string, reader *bufio.Reader) string {
	// Try by file extension
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	case ".json":
		return "application/json"
	case ".html", ".htm":
		return "text/html"
	case ".csv":
		return "text/csv"
	}

	// Fallback: try detecting from file content
	peekBytes, err := reader.Peek(512)
	if err == nil {
		return http.DetectContentType(peekBytes)
	}

	// If all fails, return default binary MIME type
	return "application/octet-stream"
}

// extFromMime returns a file extension (with dot) for a known MIME type.
// If unknown, returns empty string.
func extFromMime(mime string) string {
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "application/pdf":
		return ".pdf"
	case "text/plain":
		return ".txt"
	case "application/json":
		return ".json"
	case "text/html":
		return ".html"
	case "text/csv":
		return ".csv"
	default:
		return ""
	}
}

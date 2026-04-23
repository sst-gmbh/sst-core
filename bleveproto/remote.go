// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package bleveproto

import (
	"github.com/goccy/go-json"

	"github.com/blevesearch/bleve/v2"
)

// Install
// - protoc compiler
// - protoc and grpc plugins:
// go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
// go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

//go:generate -command protoc protoc --plugin=protoc-gen-go-grpc=$GOPATH/bin/protoc-gen-go-grpc --plugin=protoc-gen-go=$GOPATH/bin/protoc-gen-go

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative index.proto

func NewSearchRequest(request *bleve.SearchRequest) ([]byte, error) {
	return json.Marshal(request)
}

func NewSearchResult(result *bleve.SearchResult) (*SearchResult, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &SearchResult{
		Result: encoded,
	}, nil
}

func (r *SearchResult) ToSearchResult() (*bleve.SearchResult, error) {
	var result *bleve.SearchResult
	err := json.Unmarshal(r.Result, &result)
	return result, err
}

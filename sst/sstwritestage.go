// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"bufio"
	"encoding/base64"
	"errors"

	"github.com/google/uuid"
	fs "github.com/relab/wrfs"
)

var ErrDirectoryExpectedAsBasePath = errors.New("directory expected as base path")

func (to *stage) WriteToSstFiles(stageDir fs.FS) (err error) {
	return to.writeStageToSstFilesMatched(stageDir, func(uuid.UUID) bool { return true })
}

// encodeBaseURL encodes a base URL to a safe filename using base64 URL encoding.
// Uses RawURLEncoding to avoid padding '=' characters in the filename.
func encodeBaseURL(baseURL string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(baseURL))
}

// WriteToSstFilesWithBaseURL writes this Stage data to the specified directory using the graph's base URL as filename.
// Unlike WriteToSstFiles which uses UUIDs as filenames, this function uses a base64-encoded version of the base URL
// portion of each graph's IRI. This ensures the filename is filesystem-safe while being reversible.
//
// Parameters:
//   - stageDir: The filesystem directory where the stage data will be written.
//
// Returns:
//   - err: An error if the write operation fails, otherwise nil.
func (to *stage) WriteToSstFilesWithBaseURL(stageDir fs.FS) (err error) {
	return to.writeStageToSstFilesWithBaseURLMatched(stageDir)
}

func (to *stage) writeStageToSstFilesWithBaseURLMatched(stageFS fs.FS) (err error) {
	var sfs stageSstFS
	if temp, ok := stageFS.(stageSstFS); ok {
		sfs = temp
	} else {
		sfs = stageDirFS{stageFS}
	}

	stagePathInfo, err := fs.Stat(stageFS, ".")
	if err != nil {
		panic(err)
	}
	if !stagePathInfo.IsDir() {
		return ErrDirectoryExpectedAsBasePath
	}

	graphIRIs := make(map[IRI]struct{})
	for _, ng := range to.NamedGraphs() {
		graphIRIs[ng.IRI()] = struct{}{}
	}

	for graphIRI := range graphIRIs {
		graph := to.NamedGraph(graphIRI)

		if _, ok := to.repo.(*remoteRepository); !ok {
			// not modified, skip
			if !graph.(*namedGraph).flags.modified {
				continue
			}
		}

		// Get base URL from graph IRI and encode it as filename
		baseURL, _ := graph.IRI().Split()
		filename := encodeBaseURL(baseURL)

		err = func() (err error) {
			var graphW fs.WriteFile
			graphW, err = fs.Create(sfs, filename)
			if err != nil {
				panic(err)
			}
			bufW := bufio.NewWriter(graphW)
			defer func() {
				e := bufW.Flush()
				if e != nil {
					if err == nil {
						err = e
					}
					return
				}
				e = graphW.Close()
				if e != nil {
					if err == nil {
						err = e
					}
					return
				}
			}()
			return graph.SstWrite(bufW)
		}()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (to *stage) writeStageToSstFilesMatched(stageFS fs.FS, match func(ngID uuid.UUID) bool) (err error) {
	var sfs stageSstFS
	if temp, ok := stageFS.(stageSstFS); ok {
		sfs = temp
	} else {
		sfs = stageDirFS{stageFS}
	}

	stagePathInfo, err := fs.Stat(stageFS, ".")
	if err != nil {
		panic(err)
	}
	if !stagePathInfo.IsDir() {
		return ErrDirectoryExpectedAsBasePath
	}

	graphIDs := map[uuid.UUID]IRI{}
	for _, ng := range to.NamedGraphs() {
		graphIDs[ng.ID()] = ng.IRI()
	}

	for graphID := range graphIDs {
		if !match(graphID) {
			continue
		}

		graph := to.namedGraphByUUID(graphID)

		if _, ok := to.repo.(*remoteRepository); !ok {
			// not modified, skip
			if !graph.(*namedGraph).flags.modified {
				continue
			}
		}

		err = func() (err error) {
			var graphW fs.WriteFile
			graphW, err = fs.Create(sfs, graphID.String())
			if err != nil {
				panic(err)
			}
			bufW := bufio.NewWriter(graphW)
			defer func() {
				e := bufW.Flush()
				if e != nil {
					if err == nil {
						err = e
					}
					return
				}
				e = graphW.Close()
				if e != nil {
					if err == nil {
						err = e
					}
					return
				}
			}()
			return graph.SstWrite(bufW)
		}()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// func sstAddNamedGraphImportsRecursively(gi ImportDetails, graphIDs map[uuid.UUID]struct{}) {
// 	graphIDs[gi.ID()] = struct{}{}
// 	for _, di := range gi.Direct() {
// 		sstAddNamedGraphImportsRecursively(di, graphIDs)
// 	}
// }

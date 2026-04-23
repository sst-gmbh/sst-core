// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type sliceReader interface {
	// ReadSlice reads until the first occurrence of delim in the input,
	// returning a slice pointing at the bytes in the buffer.
	// The bytes stop being valid at the next read.
	// If ReadSlice encounters an error before finding a delimiter,
	// it returns all the data in the buffer and the error itself (often io.EOF).
	// ReadSlice fails with error bufio.ErrBufferFull if the buffer fills without a delim.
	// Data returned from ReadSlice will be overwritten  by the next ReadSlice operation.
	// ReadSlice returns err != nil if and only if line does not end in delim.
	ReadSlice(delim byte) (line []byte, err error)
}

type lineNumberReader struct {
	reader      sliceReader
	lastSlice   []byte
	startedLine bool
	l           int
}

func (r *lineNumberReader) Read(p []byte) (n int, err error) {
	if len(r.lastSlice) > 0 {
		if !r.startedLine {
			r.l++
			r.startedLine = true
		}
		n = copy(p, r.lastSlice)
		if len(p) <= len(r.lastSlice) {
			r.lastSlice = r.lastSlice[len(p):]
		} else {
			r.lastSlice = nil
		}
		if n > 0 && p[n-1] == '\n' {
			r.startedLine = false
		}
		return
	}
	r.lastSlice, err = r.reader.ReadSlice('\n')
	n = copy(p, r.lastSlice)
	if n > 0 {
		if !r.startedLine {
			r.l++
			r.startedLine = true
		}
		if p[n-1] == '\n' {
			r.startedLine = false
		}
		err = nil
	}
	if len(p) <= len(r.lastSlice) {
		r.lastSlice = r.lastSlice[len(p):]
	} else {
		r.lastSlice = nil
	}
	return
}

func (r *lineNumberReader) line() int {
	return r.l
}

type fileLineError struct {
	error
}

func newFileLineError(fileName string, line int, err error) fileLineError {
	errMsg := err.Error()
	msgComponents := strings.SplitN(errMsg, " ", 5)
	const unexpectedAsMinComponents, unexpectedAsMaxComponents = 3, 4
	if len(msgComponents) > unexpectedAsMinComponents {
		var tokenType uint
		var tokenTypePos int
		if msgComponents[0] == "unexpected" && msgComponents[2] == "as" {
			t, err := strconv.ParseUint(msgComponents[1], 10, 0)
			if err == nil {
				tokenType = uint(t)
				tokenTypePos = 1
			}
		} else if len(msgComponents) > unexpectedAsMaxComponents && msgComponents[1] == "unexpected" && msgComponents[3] == "as" {
			t, err := strconv.ParseUint(msgComponents[2], 10, 0)
			if err == nil {
				tokenType = uint(t)
				tokenTypePos = 2
			}
		}
		tokenTypes := [...]string{
			"EOF",
			"EOL",
			"Error",
			"IRIAbs",
			"IRIRel",
			"BNode",
			"Literal",
			"Literal3",
			"LiteralInteger",
			"LiteralDouble",
			"LiteralDecimal",
			"LiteralBoolean",
			"LangMarker",
			"Lang",
			"DataTypeMarker",
			"Dot",
			"Semicolon",
			"Comma",
			"RDFType",
			"Prefix",
			"PrefixLabel",
			"IRISuffix",
			"Base",
			"SparqlPrefix",
			"SparqlBase",
			"AnonBNode",
			"PropertyListStart",
			"PropertyListEnd",
			"CollectionStart",
			"CollectionEnd",
		}
		if tokenTypePos > 0 && tokenType < uint(len(tokenTypes)) {
			msgComponents[tokenTypePos] = tokenTypes[tokenType]
			errMsg = strings.Join(msgComponents, " ")
			err = errors.New(errMsg)
		}
	}
	colonPos := strings.IndexByte(errMsg, ':')
	if colonPos > 0 {
		_, e := strconv.ParseUint(errMsg[0:colonPos], 10, 0)
		if e == nil {
			subErrMsg := errMsg[colonPos+1:]
			colonPos = strings.IndexByte(subErrMsg, ':')
			if colonPos > 0 {
				_, e := strconv.ParseUint(subErrMsg[0:colonPos], 10, 0)
				if e == nil {
					return fileLineError{fmt.Errorf("%s:%w", fileName, err)}
				}
			}
		}
	}
	return fileLineError{fmt.Errorf("%s:%d: %w", fileName, line, err)}
}

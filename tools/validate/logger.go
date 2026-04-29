// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package validate

import (
	"fmt"

	"github.com/semanticstep/sst-core/sst"
)

type LogLevel int

const (
	_ = LogLevel(iota)
	InfoLevel
	InfoEnterLevel
	InfoLeaveLevel
	WarnLevel
	ErrorLevel
)

type LogTarget interface {
	validateTarget()
}

type Logger interface {
	Log(level LogLevel, t sst.IBNode, v ...any) error
	LogForGraph(level LogLevel, t sst.NamedGraph, v ...any) error
	Logf(level LogLevel, t sst.IBNode, format string, v ...any) error
	LogfForGraph(level LogLevel, t sst.NamedGraph, format string, v ...any) error
}

type TripleStringFormatter interface {
	FormatTripleString(s, p, o string) string
}

type tripleFormatterError struct {
	message, format string
}

func (e *tripleFormatterError) Error() string {
	return e.message
}

func (e *tripleFormatterError) FormatTripleString(s, p, o string) string {
	var sLen, pLen, oLen int
	if s != "" {
		sLen = len(s) + 1
	}
	if p != "" {
		pLen = len(p) + 1
	}
	if o != "" {
		oLen = len(o) + 1
	}
	return fmt.Sprintf(e.format, sLen, s, pLen, p, oLen, o)
}

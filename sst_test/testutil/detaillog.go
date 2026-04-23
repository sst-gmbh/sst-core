// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package testutil

import (
	"flag"
	"log"
	"os"
	"strings"
	"testing"
)

var detailLogEnabled bool

func DetailLogf(
	t interface {
		Helper()
		Logf(format string, args ...interface{})
	},
	format string, args ...interface{},
) {
	t.Helper()
	if detailLogEnabled {
		t.Logf(format, args...)
	}
}

func DetailLogEnabled() bool { return detailLogEnabled }

func ParseDetailLogFlags() {
	err := flag.Set("test.v", "true")
	if err != nil {
		log.Panic(err)
	}
	flag.Parse()
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "test.run" && !strings.Contains(f.Value.String(), "TestMain") {
			detailLogEnabled = true
		}
	})
}

func DetailLogMain(m *testing.M) {
	ParseDetailLogFlags()
	os.Exit(m.Run())
}

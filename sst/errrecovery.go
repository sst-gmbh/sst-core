// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
)

var tracebackAll = getTracebackAll()

type recoveredError struct {
	Value         interface{}
	Traceback     string
	inclTraceback bool
}

func newRecoveredError(value interface{}) recoveredError {
	var tracebackBuf []byte
	if tracebackAll >= 0 {
		initBufSize := 10
		if tracebackAll > 0 {
			initBufSize = 20
		}
		tracebackBuf = make([]byte, 1<<initBufSize)
		for {
			n := runtime.Stack(tracebackBuf, tracebackAll > 0)
			if n < len(tracebackBuf) {
				tracebackBuf = tracebackBuf[:n]
				break
			}
			if len(tracebackBuf) >= 1<<26 {
				break
			}
			tracebackBuf = make([]byte, len(tracebackBuf)<<1)
		}
	}

	return recoveredError{Value: value, Traceback: string(tracebackBuf)}
}

func (e recoveredError) Error() string {
	if e.inclTraceback {
		return fmt.Sprintf("panic: %v\n%s", e.Value, e.Traceback)
	}
	return fmt.Sprintf("panic: %v", e.Value)
}

func (e recoveredError) IncludingTraceback() (err recoveredError) {
	err = e
	err.inclTraceback = true
	return
}

func (e recoveredError) ExcludingTraceback() (err recoveredError) {
	err = e
	err.inclTraceback = false
	return
}

func getTracebackAll() int {
	level := os.Getenv("GOTRACEBACK")
	switch level {
	case "none":
		return -1
	case "single", "":
		return 0
	case "all", "system", "crash":
		return 1
	default:
		if n, err := strconv.Atoi(level); err == nil && n == int(uint32(n)) {
			return n - 1
		}
	}
	return 0
}

func handleWithRecovery(ctx context.Context, resultChan chan error, handler func(ctx context.Context) error) {
	panicked := true

	defer func() {
		if r := recover(); r != nil || panicked {
			resultChan <- newRecoveredError(r)
		}
	}()

	resultChan <- handler(ctx)
	panicked = false
}

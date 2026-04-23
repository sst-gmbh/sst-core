// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package utils

import (
	"fmt"
	"io"
	"log"
	"os"
)

var originalLogOutput io.Writer = log.Default().Writer() // Store the original log output
var nullDevice, _ = os.Open(os.DevNull)                  // Get `/dev/null` as io.Writer

// MuteLog redirects log output to a null device, effectively silencing it.
func MuteLog() {
	log.SetOutput(nullDevice) // Redirect log output to `NUL` (Windows) or `/dev/null` (Linux/Mac)
}

// RestoreLog restores the log output to its original destination.
func RestoreLog() {
	log.SetOutput(originalLogOutput) // Restore the original log output
}

var (
	originalStdout *os.File = os.Stdout // Store the original stdout
	mutedStdout    *os.File             // Store the muted stdout file handle
)

// MuteStdout redirects stdout to a null device, effectively silencing fmt.Printf and similar output.
func MuteStdout() {
	// Close previous muted stdout if exists (defensive programming)
	if mutedStdout != nil {
		mutedStdout.Close()
		mutedStdout = nil
	}
	// Open DevNull in write mode to allow writing
	var err error
	mutedStdout, err = os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		// If opening fails, panic as this is critical for functionality
		// In practice, this should never fail on any OS
		panic(fmt.Sprintf("Failed to open null device: %v", err))
	}
	os.Stdout = mutedStdout
}

// RestoreStdout restores stdout to its original destination.
func RestoreStdout() {
	if mutedStdout != nil {
		mutedStdout.Close()
		mutedStdout = nil
	}
	os.Stdout = originalStdout
}

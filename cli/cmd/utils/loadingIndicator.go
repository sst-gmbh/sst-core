// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package utils

import (
	"fmt"
	"time"
)

// ShowLoadingIndicator runs a loading message or spinner while executing a given function.
// - `message`: The message to display while loading.
// - `fn`: The function that takes time to execute.
func ShowLoadingIndicator(message string, fn func()) {
	done := make(chan bool)

	// Start a goroutine for the spinner
	go func() {
		spinner := []string{"|", "/", "-", "\\"}
		i := 0
		select {
		case <-done:
			return // Operation finished quickly, no need to show spinner
		case <-time.After(200 * time.Millisecond):
			for {
				select {
				case <-done:
					fmt.Print("\r") // Clear spinner when done
					return
				default:
					fmt.Printf("\r%s %s", message, spinner[i%len(spinner)])
					time.Sleep(100 * time.Millisecond) // Adjust speed
					i++
				}
			}
		}
	}()

	// Execute the provided function
	fn()

	// Stop the spinner
	done <- true
	close(done)
	fmt.Print("\r") // Ensure the spinner is cleared
}

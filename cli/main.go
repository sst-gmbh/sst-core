// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

// The SST Core “Command Line Interface” tool CLI is a low level tool
// for debugging and testing the SST-Core and -Repositories.
package main

import cli "git.semanticstep.net/x/sst/cli/cmd"

// main function to execute the CLI commands
// go build -o ./cli/sst ./cli/main.go
func main() {
	cli.Execute()
}

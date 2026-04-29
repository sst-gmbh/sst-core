// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package cli

import (
	"fmt"
	"io"

	"github.com/semanticstep/sst-core/cli/cmd/interactive"
	"github.com/spf13/cobra"
)

// rootCmd is the main entry point for the CLI
var RootCmd = &cobra.Command{
	Use:   "sst",
	Short: "SST Command Line Interface",
	Long:  "SST CLI provides commands to interact with SST repositories and datasets.",
	Run: func(cmd *cobra.Command, args []string) {
		// Default behavior: start interactive mode
		fmt.Println("Entering SST CLI tool in interactive mode. Type 'q' to quit, 'help' to see available commands.")
		interactive.StartInteractiveMode()
	},
}

// Execute starts the CLI execution
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}

func init() {
	origHelp := RootCmd.HelpFunc()

	RootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		origHelp(cmd, args)

		if cmd == RootCmd {
			printInteractiveFooter(cmd.OutOrStdout())
		}
	})

	RootCmd.AddCommand(interactive.InteractiveCmd)
	RootCmd.AddCommand(versionCmd)
}

func printInteractiveFooter(w io.Writer) {
	fmt.Fprintln(w, "\nInteractive mode:")
	fmt.Fprintln(w, "  Start:  sst (or sst interactive)")
	fmt.Fprintln(w, "  Inside: type 'help' to see all interactive commands")
}

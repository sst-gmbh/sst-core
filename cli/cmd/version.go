// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package cli

import (
	"strings"

	"github.com/spf13/cobra"
)

// go build -ldflags "-X 'git.semanticstep.net/x/sst/cli/cmd.version=v1.0.0' -X 'git.semanticstep.net/x/sst/cli/cmd.buildTime=20260409080125' -X 'git.semanticstep.net/x/sst/cli/cmd.commit=d716e85dd842'" -o cli/sst ./cli
// go build -ldflags "-X 'git.semanticstep.net/x/sst/cli/cmd.version=$(git describe --tags --abbrev=0)' -X 'git.semanticstep.net/x/sst/cli/cmd.buildTime=$(date -u +%Y%m%d%H%M%S)' -X 'git.semanticstep.net/x/sst/cli/cmd.commit=$(git rev-parse --short=12 HEAD)'" -o cli/sst ./cli

var version = "dev"
var buildTime = ""
var commit = ""

func versionString() string {
	v := strings.TrimSpace(version)
	if v == "" {
		v = "dev"
	}

	bt := strings.TrimSpace(buildTime)
	c := strings.TrimSpace(commit)

	parts := []string{v}

	if bt != "" {
		parts = append(parts, bt)
	}
	if c != "" {
		parts = append(parts, c)
	}

	return strings.Join(parts, "-")
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println(versionString())
	},
}

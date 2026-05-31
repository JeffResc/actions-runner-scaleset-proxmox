// scaleset is the orchestrator binary. It runs an actions/scaleset
// listener backed by Proxmox VMs as ephemeral GitHub Actions runners.
//
// Subcommands:
//
//	scaleset run [--config=path]
//	scaleset version
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/app"
)

// version is overridden via -ldflags at build time.
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "scaleset",
		Short:         "Run GitHub Actions jobs as ephemeral Proxmox VMs",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	var configPath string

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run the orchestrator until SIGINT/SIGTERM.",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			return app.Run(ctx, app.Options{
				ConfigPath: configPath,
				Version:    version,
			})
		},
	}
	runCmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "Path to config YAML.")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the build version and exit.",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(version)
		},
	}

	root.AddCommand(runCmd, versionCmd)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "scaleset: %v\n", err)
		os.Exit(1)
	}
}

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "foam-postprocess [run file]",
	Short: "A program for working with  OpenFOAM functionObject output files",
	Long:  `A program for working with  OpenFOAM functionObject output files`,
	Args: cobra.MatchAll(
		cobra.MaximumNArgs(1),
	),
	RunE: run,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.foam-postprocess.yaml)")
	rootCmd.PersistentFlags().BoolVarP(
		&verbose,
		"verbose",
		"v",
		false,
		"more verbose log output",
	)
	rootCmd.PersistentFlags().BoolVarP(
		&quiet,
		"quiet",
		"q",
		false,
		"suppress all log output",
	)
	rootCmd.MarkFlagsMutuallyExclusive("verbose", "quiet")

	rootCmd.PersistentFlags().BoolVar(
		&dryRun,
		"dry-run",
		false,
		"read the config and exit",
	)
	rootCmd.PersistentFlags().BoolVar(
		&noProcess,
		"no-process",
		false,
		"don't process the input data",
	)
	rootCmd.PersistentFlags().BoolVar(
		&noWriteCSV,
		"no-write-csv",
		false,
		"don't write data to csv",
	)
	rootCmd.PersistentFlags().BoolVar(
		&noWriteGraphs,
		"no-write-graphs",
		false,
		"don't write graph files",
	)
	rootCmd.PersistentFlags().BoolVar(
		&noGenerateGraphs,
		"no-generate-graphs",
		false,
		"don't generate graphs",
	)

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

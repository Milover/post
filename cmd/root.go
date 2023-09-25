package cmd

import (
	"log"

	"github.com/Milover/post/internal/common"
	"github.com/spf13/cobra"
)

var (
	// rootCmd represents the base command when called without any subcommands
	rootCmd = &cobra.Command{
		Use:           "post [run file]",
		Short:         "post is a program for processing structured data files in bulk",
		Long:          `post is a program for processing structured data files in bulk`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: cobra.MatchAll(
			cobra.MinimumNArgs(1),
		),
		RunE: run,
	}
)

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.post.yaml)")
	rootCmd.PersistentFlags().BoolVarP(
		&common.Verbose,
		"verbose",
		"v",
		false,
		"verbose log output",
	)

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.Flags().BoolVar(
		&dryRun,
		"dry-run",
		false,
		"check runfile syntax and exit",
	)
	rootCmd.Flags().StringSliceVar(
		&skipIDs,
		"skip",
		[]string{},
		"a list of pipeline IDs to be skipped during processing",
	)
	rootCmd.Flags().BoolVar(
		&onlyGraphs,
		"only-graphs",
		false,
		"only write and generate graphs, skip input, processing and output",
	)
	rootCmd.Flags().BoolVar(
		&noProcess,
		"no-process",
		false,
		"don't process data",
	)
	rootCmd.Flags().BoolVar(
		&noOutput,
		"no-output",
		false,
		"don't output data",
	)
	rootCmd.Flags().BoolVar(
		&noGraph,
		"no-graph",
		false,
		"don't write or generate graphs",
	)
	rootCmd.Flags().BoolVar(
		&noGraphWrite,
		"no-graph-write",
		false,
		"don't write graph files",
	)
	rootCmd.Flags().BoolVar(
		&noGraphGenerate,
		"no-graph-generate",
		false,
		"don't generate graphs",
	)
	rootCmd.Flags().BoolVar(
		&logMem,
		"log-mem",
		false,
		"log memory usage at the end of each pipeline",
	)

	rootCmd.AddCommand(writeConfigTemplateCmd)
	rootCmd.AddCommand(writeGraphTemplateCmd)
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

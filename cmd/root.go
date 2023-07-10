package cmd

import (
	"log"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// configFile is the default file name of the config file, it is used
	// if no config file is supplied as a command line argument.
	configFile string = "config.yaml"

	logLevel logrus.Level
)

var (
	// rootCmd represents the base command when called without any subcommands
	rootCmd = &cobra.Command{
		Use:   "fp [run file]",
		Short: "A program for working with  OpenFOAM functionObject output files",
		Long:  `A program for working with  OpenFOAM functionObject output files`,
		Args: cobra.MatchAll(
			cobra.MaximumNArgs(1),
		),
		RunE: run,
	}
	writeConfigTemplateCmd = &cobra.Command{
		Use:   "runfile",
		Short: "Generate a run file stub",
		Long:  `Generate a run file stub`,
		Args: cobra.MatchAll(
			cobra.ExactArgs(0),
		),
		RunE: writeConfigTemplate,
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	rootCmd.AddCommand(writeConfigTemplateCmd)
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.foam-postprocess.yaml)")
	rootCmd.PersistentFlags().CountVarP(
		(*int)(unsafe.Pointer(&logLevel)), // XXX: unsafe, logLevel = uint32
		"verbose",
		"v",
		"verbose log output",
	)
	logLevel = logrus.WarnLevel

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.Flags().BoolVar(
		&dryRun,
		"dry-run",
		false,
		"read the config and exit",
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
		&noWriteCSV,
		"no-write-csv",
		false,
		"don't write data to csv",
	)
	rootCmd.Flags().BoolVar(
		&noWriteGraphs,
		"no-write-graphs",
		false,
		"don't write graph files",
	)
	rootCmd.Flags().BoolVar(
		&noGenerateGraphs,
		"no-generate-graphs",
		false,
		"don't generate graphs",
	)
}

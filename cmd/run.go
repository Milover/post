package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/Milover/foam-postprocess/internal/input"
	"github.com/Milover/foam-postprocess/internal/output"
	"github.com/Milover/foam-postprocess/internal/process"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	// configFile is the default file name of the config file, it is used
	// if no config file is supplied as a command line argument.
	configFile string = "config.yaml"

	logLevel logrus.Level = logrus.FatalLevel

	dryRun           bool
	noProcess        bool
	noWriteCSV       bool
	noWriteGraphs    bool
	noGenerateGraphs bool
)

type Config struct {
	Input   input.Config     `yaml:"input"`
	Process []process.Config `yaml:"process"`
	Output  output.Config    `yaml:"output"`

	Log *logrus.Logger `yaml:"-"`
}

func logError(err error, log *logrus.Logger) error {
	if err != nil {
		log.Error(err)
	}
	return err
}

func run(cmd *cobra.Command, args []string) error {
	var config Config
	config.Log = logrus.StandardLogger()
	config.Log.SetLevel(logLevel)

	// read in config
	if len(args) != 0 {
		configFile = args[0]
	}
	f, err := os.Open(configFile)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(raw, &config); err != nil {
		return err
	}

	if dryRun {
		return nil
	}

	// propagate logger
	config.Input.Log = config.Log
	for i := range config.Process {
		config.Process[i].Log = config.Log
	}

	df, err := input.CreateDataFrame(&config.Input)
	if err != nil {
		return logError(fmt.Errorf("error creating data frame: %w", err), config.Log)
	}
	if !noProcess {
		if err = process.Process(df, config.Process); err != nil {
			return logError(fmt.Errorf("error processing data frame: %w", err), config.Log)
		}
	}
	if !noWriteCSV {
		if err := output.WriteCSV(df, &config.Output); err != nil {
			return logError(fmt.Errorf("output error: %w", err), config.Log)
		}
	}
	if !noWriteGraphs {
		if err := output.WriteGraphFiles(&config.Output); err != nil {
			return logError(fmt.Errorf("output error: %w", err), config.Log)
		}
	}
	if !noGenerateGraphs {
		if err := output.GenerateGraphs(&config.Output); err != nil {
			return logError(fmt.Errorf("output error: %w", err), config.Log)
		}
	}

	return nil
}

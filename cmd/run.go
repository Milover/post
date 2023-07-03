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

func (c *Config) propagateLogger(log *logrus.Logger) {
	c.Log = log
	c.Input.Log = log
	for i := range c.Process {
		c.Process[i].Log = log
	}
	c.Output.Log = log
}

func (c *Config) logError(err error) error {
	if err != nil {
		c.Log.Error(err)
	}
	return err
}

func run(cmd *cobra.Command, args []string) error {
	logger := logrus.StandardLogger()
	logger.SetLevel(logLevel)

	// read in configs
	var configs []Config
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
	if err = yaml.Unmarshal(raw, &configs); err != nil {
		return err
	}
	if dryRun {
		return nil
	}

	// work
	for i := range configs {
		c := &configs[i]
		c.propagateLogger(logger)

		df, err := input.CreateDataFrame(&c.Input)
		if err != nil {
			return c.logError(fmt.Errorf("error creating data frame: %w", err))
		}
		if !noProcess {
			if err = process.Process(df, c.Process); err != nil {
				return c.logError(fmt.Errorf("error processing data frame: %w", err))
			}
		}
		if !noWriteCSV {
			if err := output.WriteCSV(df, &c.Output); err != nil {
				return c.logError(fmt.Errorf("output error: %w", err))
			}
		}
		if !noWriteGraphs {
			if err := output.WriteGraphFiles(&c.Output); err != nil {
				return c.logError(fmt.Errorf("output error: %w", err))
			}
		}
		if !noGenerateGraphs {
			if err := output.GenerateGraphs(&c.Output); err != nil {
				return c.logError(fmt.Errorf("output error: %w", err))
			}
		}
	}

	return nil
}

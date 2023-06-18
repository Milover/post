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
	verbose    bool   = false
	quiet      bool   = false
	configFile string = "config.yaml"
)

type Config struct {
	Input   input.Config     `yaml:"input"`
	Process []process.Config `yaml:"process"`
	Output  output.Config    `yaml:"output"`

	Log *logrus.Logger `yaml:"-"`
}

func run(cmd *cobra.Command, args []string) error {
	var config Config
	config.Log = logrus.StandardLogger()
	if verbose {
		config.Log.SetLevel(logrus.DebugLevel)
	} else if quiet {
		config.Log.SetLevel(logrus.ErrorLevel)
	}

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

	// propagate logger
	config.Input.Log = config.Log
	for i := range config.Process {
		config.Process[i].Log = config.Log
	}

	df, err := input.CreateDataFrame(&config.Input)
	if err != nil {
		return fmt.Errorf("error creating data frame: %w", err)
	}
	if err = process.Process(df, config.Process); err != nil {
		return fmt.Errorf("error processing data frame: %w", err)
	}
	if err = output.Output(df, &config.Output); err != nil {
		return fmt.Errorf("output error: %w", err)
	}
	return nil
}

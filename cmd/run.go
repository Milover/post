package cmd

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/Milover/foam-postprocess/internal/graph"
	"github.com/Milover/foam-postprocess/internal/input"
	"github.com/Milover/foam-postprocess/internal/output"
	"github.com/Milover/foam-postprocess/internal/process"
	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

var (
	dryRun          bool
	onlyGraphs      bool
	noProcess       bool
	noOutput        bool
	noGraph         bool
	noGraphWrite    bool
	noGraphGenerate bool

	skipIDs []string
)

type Config struct {
	ID      string           `yaml:"id"`
	Input   input.Config     `yaml:"input"`
	Process []process.Config `yaml:"process"`
	Output  []output.Config  `yaml:"output"`
	Graph   graph.Config     `yaml:"graph"`

	Log *logrus.Logger `yaml:"-"`
}

func (c *Config) propagateLogger(log *logrus.Logger) {
	c.Log = log
	c.Input.Log = log
	for i := range c.Process {
		c.Process[i].Log = log
	}
	for i := range c.Output {
		c.Output[i].Log = log
	}
	c.Graph.Log = log
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
		if len(c.ID) == 0 {
			c.ID = strconv.Itoa(i)
		}
		if slices.Contains(skipIDs, c.ID) {
			c.Log.WithFields(logrus.Fields{
				"id": c.ID,
			}).Info("skipping pipeline")
			continue
		}

		var df *dataframe.DataFrame
		if !onlyGraphs {
			df, err = input.CreateDataFrame(&c.Input)
			if err != nil {
				return c.logError(fmt.Errorf("error creating data frame: %w", err))
			}
			if !noProcess {
				if err = process.Process(df, c.Process); err != nil {
					return c.logError(fmt.Errorf("error processing data frame: %w", err))
				}
			}
			if !noOutput {
				if err = output.Output(df, c.Output); err != nil {
					return c.logError(fmt.Errorf("output error: %w", err))
				}
			}
		}
		if !noGraph {
			if !noGraphWrite {
				if err := graph.Write(df, &c.Graph); err != nil {
					return c.logError(fmt.Errorf("error writing graphs: %w", err))
				}
			}
			if !noGraphGenerate {
				if err := graph.Generate(df, &c.Graph); err != nil {
					return c.logError(fmt.Errorf("error generating graphs: %w", err))
				}
			}
		}
	}

	return nil
}

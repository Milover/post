package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/Milover/post/internal/common"
	"github.com/Milover/post/internal/graph"
	"github.com/Milover/post/internal/process"
	"github.com/Milover/post/internal/rw"
	"github.com/go-gota/gota/dataframe"
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
	Input   rw.Config        `yaml:"input"`
	Process []process.Config `yaml:"process"`
	Output  []rw.Config      `yaml:"output"`
	Graph   graph.Config     `yaml:"graph"`
}

func run(cmd *cobra.Command, args []string) error {
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
		if len(c.ID) == 0 {
			c.ID = strconv.Itoa(i)
		}
		if slices.Contains(skipIDs, c.ID) {
			if common.Verbose {
				log.Printf("skipping pipeline: %v", c.ID)
			}
			continue
		}

		var df *dataframe.DataFrame
		if !onlyGraphs {
			df, err = rw.Read(&c.Input)
			if err != nil {
				return fmt.Errorf("error creating data frame: %w", err)
			}
			if !noProcess {
				if err = process.Process(df, c.Process); err != nil {
					return fmt.Errorf("error processing data frame: %w", err)
				}
			}
			if !noOutput {
				if err = rw.Write(df, c.Output); err != nil {
					return fmt.Errorf("output error: %w", err)
				}
			}
		}
		if !noGraph {
			if !noGraphWrite {
				if err := graph.Write(df, &c.Graph); err != nil {
					return fmt.Errorf("error writing graphs: %w", err)
				}
			}
			if !noGraphGenerate {
				if err := graph.Generate(df, &c.Graph); err != nil {
					return fmt.Errorf("error generating graphs: %w", err)
				}
			}
		}
	}

	return nil
}

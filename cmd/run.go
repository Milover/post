package cmd

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/Milover/post/internal/format"
	"github.com/Milover/post/internal/graph"
	"github.com/Milover/post/internal/process"
	"github.com/Milover/post/internal/rw"
	"github.com/Milover/post/internal/template"
	"github.com/go-gota/gota/dataframe"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	dryRun          bool
	logMem          bool
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
	readers := make([]io.Reader, len(args))
	for i, arg := range args {
		f, err := os.Open(arg)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := os.Chdir(path.Dir(f.Name())); err != nil {
			return fmt.Errorf("could not change directory: %w", err)
		}
		readers[i] = f
	}
	raw, err := io.ReadAll(io.MultiReader(readers...))
	if err != nil {
		return err
	}

	var n yaml.Node
	if err = yaml.Unmarshal(raw, &n); err != nil {
		return err
	}
	if err := template.Process(&n); err != nil {
		return err
	}
	var configs []Config
	if err := n.Decode(&configs); err != nil {
		return err
	}
	if dryRun {
		return nil
	}

	// work
	var mem runtime.MemStats
	for i := range configs {
		c := &configs[i]
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
					return fmt.Errorf("error writing graph: %w", err)
				}
			}
			if !noGraphGenerate {
				if err := graph.Generate(df, &c.Graph); err != nil {
					return fmt.Errorf("error generating graph: %w", err)
				}
			}
		}

		if logMem {
			runtime.ReadMemStats(&mem)
			log.Printf("memory usage: alloc = %v\tsys = %v\tn_allocs = %v\tid = %q\n",
				format.Byte(mem.Alloc),
				format.Byte(mem.Sys),
				mem.Mallocs,
				c.ID)
		}
	}
	return nil
}

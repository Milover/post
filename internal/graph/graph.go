package graph

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	ErrBadGrapher = fmt.Errorf(
		"bad grapher, available graphers are: %q",
		common.MapKeys(GrapherFactories))
)

type Grapher interface {
	Write() error
	Generate() error
}

type GrapherFactory func(*yaml.Node, *Config) (Grapher, error)

// GrapherFactories maps Format type tags to FormatReaders.
var GrapherFactories = map[string]GrapherFactory{
	"tex": newTeXGrapher,
}

// WriteGraphFiles writes graph files, e.g., TeX files,
// using options from the config.
func Write(_ *dataframe.DataFrame, config *Config) error {
	return graphExecute(config, Grapher.Write, "writing")
}

// GenerateGraphs generates the graphs, e.g., PDFs from TeX files.
func Generate(_ *dataframe.DataFrame, config *Config) error {
	return graphExecute(config, Grapher.Generate, "generating")
}

func graphExecute(config *Config, exec func(Grapher) error, action string) error {
	if len(config.Graphs) == 0 {
		return nil
	}
	factory, found := GrapherFactories[strings.ToLower(config.GrapherType)]
	if !found {
		return fmt.Errorf("graph: %w, got: %q", ErrBadGrapher, config.GrapherType)
	}
	if common.Verbose {
		log.Printf("%v: %v graph", strings.ToLower(config.GrapherType), action)
	}
	var err error
	for i := range config.Graphs {
		c := &config.Graphs[i]
		grapher, e := factory(c, config)
		if e != nil {
			err = errors.Join(err, e)
			continue
		}
		err = errors.Join(err, exec(grapher))
	}
	return err
}

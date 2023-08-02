package graph

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

var (
	ErrInvalidGrapher = fmt.Errorf(
		"bad input grapher, available graphers are: %q",
		maps.Keys(GrapherFactories))
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

// WriteGraphFiles writes graph files, using options from the config.
func Write(_ *dataframe.DataFrame, config *Config) error {
	return graphExecute(config, Grapher.Write, "writing")
}

// GenerateGraphs generates the actual graphs, e.g., PDFs from TeX files.
func Generate(_ *dataframe.DataFrame, config *Config) error {
	return graphExecute(config, Grapher.Generate, "writing")
}

func graphExecute(config *Config, exec func(Grapher) error, action string) error {
	if len(config.Graphs) == 0 {
		return nil
	}
	factory, found := GrapherFactories[strings.ToLower(config.GrapherType)]
	if !found {
		return ErrInvalidGrapher
	}
	config.Log.WithFields(logrus.Fields{
		"grapher": config.GrapherType,
	}).Debug(action + " graph")
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

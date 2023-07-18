package output

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
	Write(*GraphOutputer) error
	Generate(*GraphOutputer) error
}

type GrapherFactory func(*yaml.Node, *GraphOutputer) (Grapher, error)

// GrapherFactories maps Format type tags to FormatReaders.
var GrapherFactories = map[string]GrapherFactory{
	"tex": newTeXGrapher,
}

type GraphOutputer struct {
	// Directory is an output directory for all data. If it is an empty string,
	// the current working directory is used. The path is created recursively
	// if it does not exist.
	Directory string `yaml:"directory"`
	// Graphing is the graphing program name.
	Grapher string `yaml:"grapher"`
	// Graphs is list of graph YAML specifications.
	Graphs []yaml.Node `yaml:"graphs"`

	Factory GrapherFactory `yaml:"-"`
	Log     *logrus.Logger `yaml:"-"`
}

func NewGraphOutputer(n *yaml.Node, config *Config) (Outputer, error) {
	o := GraphOutputer{}
	o.Log = config.Log
	if err := n.Decode(&o); err != nil {
		return nil, err
	}
	var found bool
	if o.Factory, found = GrapherFactories[strings.ToLower(o.Grapher)]; !found {
		return nil, ErrInvalidGrapher
	}
	return &o, nil
}

// WriteGraphFiles writes graph files, using options from the config.
// GenerateGraphs generates the actual graphs, e.g., PDFs from TeX files.
func (o *GraphOutputer) Output(_ *dataframe.DataFrame) error {
	if err := o.graphExecute(Grapher.Write, "writing"); err != nil {
		return err
	}
	return o.graphExecute(Grapher.Generate, "writing")
}

type execFunc func(Grapher, *GraphOutputer) error

func (o *GraphOutputer) graphExecute(exec execFunc, action string) error {
	if len(o.Graphs) == 0 {
		return nil
	}
	o.Log.WithFields(logrus.Fields{
		"grapher": o.Grapher,
	}).Debug(action + " graph")
	var err error
	for i := range o.Graphs {
		g, e := o.Factory(&o.Graphs[i], o)
		if e != nil {
			err = errors.Join(err, e)
		} else {
			err = errors.Join(err, exec(g, o))
		}
	}
	return err
}

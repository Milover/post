package output

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	Write(*Config) error
	Generate(*Config) error
}

type GrapherFactory func(*yaml.Node) (Grapher, error)

// GrapherFactories maps Format type tags to FormatReaders.
var GrapherFactories = map[string]GrapherFactory{
	"tex": NewTeXGrapher,
}

var factory GrapherFactory // :(

func OutDir(config *Config) (string, error) {
	if err := os.MkdirAll(filepath.Clean(config.Directory), 0755); err != nil {
		return "", err
	}
	return config.Directory, nil
}

// WriteCSV writes df to a CSV file, using options from the config.
// FIXME: LaTeX has an upper size limit for CSV files that it can handle
// so the output should be decimated down to this size if it's too large.
func WriteCSV(df *dataframe.DataFrame, config *Config) error {
	csv, err := OutDir(config)
	if err != nil {
		return err
	}
	if len(config.TableFile) == 0 {
		return err
	}
	csv = filepath.Join(csv, config.TableFile)
	config.Log.WithFields(logrus.Fields{
		"file": csv,
	}).Debug("writing csv")
	w, err := os.Create(csv)
	if err != nil {
		return err
	}
	if err := df.WriteCSV(w, dataframe.WriteHeader(true)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

type execFunc func(Grapher, *Config) error

func graphExecute(config *Config, exec execFunc, action string) error {
	if len(config.Graphs) == 0 {
		return nil
	}
	var found bool
	if factory, found = GrapherFactories[strings.ToLower(config.Grapher)]; !found {
		return ErrInvalidGrapher
	}
	config.Log.WithFields(logrus.Fields{
		"directory":  config.Directory,
		"table-file": config.TableFile,
		"grapher":    config.Grapher,
	}).Debug(action + " graph")
	var err error
	for i := range config.Graphs {
		g, e := factory(&config.Graphs[i])
		if e != nil {
			err = errors.Join(err, e)
		} else {
			err = errors.Join(err, exec(g, config))
		}
	}
	return err
}

// WriteGraphFiles writes graph files, using options from the config.
func WriteGraphFiles(config *Config) error {
	return graphExecute(config, Grapher.Write, "writing")
}

// GenerateGraphs generates the actual graphs, e.g., PDFs from TeX files.
func GenerateGraphs(config *Config) error {
	return graphExecute(config, Grapher.Generate, "generating")
}

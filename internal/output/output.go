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
	ErrInvalidOutputer = fmt.Errorf(
		"bad output type, available types are: %q",
		maps.Keys(OutputerFactories))
)

type OutputerFactory func(*yaml.Node, *Config) (Outputer, error)

var OutputerFactories = map[string]OutputerFactory{
	"csv":   NewCSVOutputer,
	"graph": NewGraphOutputer,
}

type Outputer interface {
	Output(*dataframe.DataFrame) error
}

func OutDir(dirPath string) (string, error) {
	if len(dirPath) == 0 {
		return dirPath, nil
	}
	if err := os.MkdirAll(filepath.Clean(dirPath), 0755); err != nil {
		return "", err
	}
	return dirPath, nil
}

// Process applies all Processors as defined in the config
// to the dataframe.DataFrame.
// Each Processor is applied sequentially, in the order they appear in
// the config, and the result of one Processor is passed as the input to the
// next one.
//
// An error is returned if any of the Processors return an error.
// If an error is returned, the dataframe.DataFrame state is unknown.
func Output(df *dataframe.DataFrame, configs []Config) error {
	var err error
	for i := range configs {
		err = errors.Join(err, output(df, &configs[i]))
	}
	return err
}

// process applies a single Processor as defined in the config
// to the dataframe.DataFrame.
func output(df *dataframe.DataFrame, config *Config) error {
	of, found := OutputerFactories[strings.ToLower(config.Type)]
	if !found {
		return ErrInvalidOutputer
	}
	config.Log.WithFields(logrus.Fields{
		"type": strings.ToLower(config.Type),
	}).Debug("outputting")
	o, err := of(&config.TypeSpec, config)
	if err != nil {
		return err
	}
	return o.Output(df)
}

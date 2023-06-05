package process

import (
	"errors"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
)

var (
	ErrInvalidType = errors.New("invalid process type")
)

// Processor is a function which applies processing on a dataframe.DataFrame
// based on the configuration.
type Processor func(*dataframe.DataFrame, *Config) error

// ProcessorMap maps Processor type tags to Processors.
var ProcessorMap = map[string]Processor{
	"dummy":  dummyProcessor,
	"filter": filterProcessor,
}

// validType represents the supported series.Series types (a dataframe.DataFrame
// is composed of []series.Series).
type validType interface {
	string | int | float64 | bool
}

// Process applies all Processors as defined in the config
// to the dataframe.DataFrame.
// Each Processor is applied sequentially, in the order they appear in
// the config, and the result of one Processor is passed as the input to the
// next one.
//
// An error is returned if any of the Processors return an error.
// If an error is returned, the dataframe.DataFrame state is unknown.
func Process(df *dataframe.DataFrame, configs []Config) error {
	for i := range configs {
		if err := process(df, &configs[i]); err != nil {
			return err
		}
	}
	return nil
}

// process applies a single Processor as defined in the config
// to the dataframe.DataFrame.
func process(df *dataframe.DataFrame, config *Config) error {
	p, found := ProcessorMap[strings.ToLower(config.Type)]
	if !found {
		return ErrInvalidType
	}
	config.Log.WithFields(logrus.Fields{
		"processor": strings.ToLower(config.Type),
	}).Debug("applying processor")
	if err := p(df, config); err != nil {
		return err
	}
	return nil
}

// dummyProcessor is a do-nothing processor used for testing purposes.
func dummyProcessor(_ *dataframe.DataFrame, config *Config) error {
	return nil
}

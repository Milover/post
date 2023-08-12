package process

import (
	"fmt"
	"log"
	"strings"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var (
	ErrBadProcessor = fmt.Errorf(
		"bad processor type, available types are: %q",
		common.MapKeys(ProcessorTypes))
)

// Processor is a function which applies processing on a dataframe.DataFrame
// based on the configuration.
type Processor func(*dataframe.DataFrame, *Config) error

// ProcessorTypes maps Processor type tags to Processors.
var ProcessorTypes = map[string]Processor{
	"average-cycle": averageCycleProcessor,
	"dummy":         dummyProcessor,
	"expression":    expressionProcessor,
	"filter":        filterProcessor,
	"select":        selectProcessor,
	"resample":      resampleProcessor,
}

// validType represents the supported series.Series types (a dataframe.DataFrame
// is composed of []series.Series).
type validType interface {
	string | int | float64 | bool
}

// Process applies all Processors defined in the config to df.
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

// process applies a single Processor to df, as defined in the config.
func process(df *dataframe.DataFrame, config *Config) error {
	p, found := ProcessorTypes[strings.ToLower(config.Type)]
	if !found {
		return fmt.Errorf("process: %w, got: %q", ErrBadProcessor, config.Type)
	}
	if common.Verbose {
		log.Printf("applying processor: %q", strings.ToLower(config.Type))
		log.Printf("fields before: %v", df.Names())
	}
	if err := p(df, config); err != nil {
		return err
	}
	if common.Verbose {
		log.Printf("fields after: %v", df.Names())
	}
	return nil
}

// dummyProcessor is a do-nothing processor used for testing purposes.
func dummyProcessor(_ *dataframe.DataFrame, _ *Config) error {
	return nil
}

// selectNumFields is a function that selects only numeric (int, float) fields
// in a dataframe.DataFrame, and removes all other fields.
func selectNumFields(df *dataframe.DataFrame) error {
	keep := make([]int, 0, df.Ncol())
	for i, typ := range df.Types() {
		if typ == series.Int || typ == series.Float {
			keep = append(keep, i)
		}
	}
	*df = df.Select(keep)
	return df.Error()
}

// intsToFloats is a function that converts int fields to float fields.
func intsToFloats(df *dataframe.DataFrame) error {
	f := func(s series.Series) series.Series {
		if s.Type() != series.Int {
			return s
		}
		return series.New(s.Float(), series.Float, s.Name)
	}
	*df = df.Capply(f)
	return df.Error()
}

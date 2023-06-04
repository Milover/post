package process

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

var (
	ErrInvalidType = errors.New("invalid process type")

	ErrFilterField     = errors.New("filter field does not exist")
	ErrFilterValue     = errors.New("filter value undefined")
	ErrFilterFieldType = errors.New("field-filter value type mismatch")
)

// Processor is a function which applies processing on a dataframe.DataFrame
// based on the configuration.
type Processor func(*dataframe.DataFrame, *Config) error

// ProcessorMap maps Processor type tags to Processors.
var ProcessorMap = map[string]Processor{
	"dummy":  dummyProcessor,
	"filter": filterProcessor,
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

// dfType represents the supported series.Series types (a dataframe.DataFrame
// is composed of []series.Series).
type dfType interface {
	string | int | float64 | bool
}

// filterSpec contains data needed for defining a filter Processor.
type filterSpec struct {
	Field string            `yaml:"field,omitempty"`
	Op    series.Comparator `yaml:"op,omitempty"`
	Value string            `yaml:"value,omitempty"`

	Log *logrus.Logger `yaml:"-"`
}

// filterOnType mutates df by applying the filter as defined in the spec.
func filterOnType[T dfType](df *dataframe.DataFrame, spec *filterSpec, val T) error {
	temp := df.Filter(dataframe.F{
		Colname:    spec.Field,
		Comparator: spec.Op,
		Comparando: val,
	})
	err := errors.Join(df.Error(), temp.Error())
	spec.Log.WithFields(logrus.Fields{
		"type":  fmt.Sprintf("%T", val),
		"field": spec.Field,
		"op":    spec.Op,
		"value": val,
		"error": err,
	}).Debug("filtering")

	*df = temp
	return err
}

// filterProcessor mutates the dataframe.DataFrame by applying a row filter
// based on the field, comparison operator and value, as defined in the config.
func filterProcessor(df *dataframe.DataFrame, config *Config) error {
	var spec filterSpec
	spec.Log = config.Log
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return err
	}
	if !slices.Contains(df.Names(), spec.Field) {
		return ErrFilterField
	}
	if len(spec.Value) == 0 {
		return ErrFilterValue
	}

	switch df.Select(spec.Field).Types()[0] {
	case series.String:
		return filterOnType(df, &spec, spec.Value)
	case series.Int:
		val, err := strconv.Atoi(spec.Value)
		if err != nil {
			return err
		}
		return filterOnType(df, &spec, val)
	case series.Float:
		val, err := strconv.ParseFloat(spec.Value, 64)
		if err != nil {
			return err
		}
		return filterOnType(df, &spec, val)
	case series.Bool:
		val, err := strconv.ParseBool(spec.Value)
		if err != nil {
			return err
		}
		return filterOnType(df, &spec, val)
	default:
		return ErrFilterFieldType
	}
}

package process

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	ErrFilterField       = errors.New("filter: field does not exist")
	ErrFilterValue       = errors.New("filter: value undefined")
	ErrFilterFieldType   = errors.New("filter: field value type mismatch")
	ErrFilterAggregation = fmt.Errorf(
		"filter: bad aggregation mode, available modes are: %q",
		maps.Keys(filterAggregations))
)

var filterAggregations = map[string]dataframe.Aggregation{
	"or":  dataframe.Or,
	"and": dataframe.And,
}

// filterSetSpec contains data needed for defining a filter-set Processor.
type filterSetSpec struct {
	Aggregation string       `yaml:"aggregation"`
	Filters     []filterSpec `yaml:"filters"`

	Log *logrus.Logger `yaml:"-"`
}

// filterSpec contains data needed for defining a filter Processor.
type filterSpec struct {
	Field string            `yaml:"field"`
	Op    series.Comparator `yaml:"op"`
	Value string            `yaml:"value"`

	Log *logrus.Logger `yaml:"-"`
}

// defaultFilterSetSpec returns a filterSetSpec with 'sensible' default values.
func defaultFilterSetSpec() filterSetSpec {
	return filterSetSpec{Aggregation: "or"}
}

// createFilter creates a dataframe.F from an input filterSpec
// and a filter (comparison) value.
func createFilter[T validType](spec *filterSpec, val T) dataframe.F {
	spec.Log.WithFields(logrus.Fields{
		"type":  fmt.Sprintf("%T", val),
		"field": spec.Field,
		"op":    spec.Op,
		"value": val,
	}).Debug("creating filter")
	return dataframe.F{
		Colname:    spec.Field,
		Comparator: spec.Op,
		Comparando: val,
	}
}

// filterProcessor mutates the dataframe.DataFrame by applying a set of row
// filters as defined in the config.
//
// The filter behaviour is described by providing the field (name) on which
// to apply a filter, the comparison operator and a comparison value.
// Rows satisfying the comparison are kept, while others are discarded.
//
// All defined filters are applied at the same time. The way in which they
// are aggregated is controlled by setting the 'aggregation' field in the spec,
// 'and' or 'or' aggregation modes are available.
// The 'or' mode is the default if the 'aggregation' field is unset.
func filterProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := defaultFilterSetSpec()
	spec.Log = config.Log
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return err
	}
	if len(spec.Filters) == 0 {
		return nil
	}
	aggr, found := filterAggregations[strings.ToLower(spec.Aggregation)]
	if !found {
		return ErrFilterAggregation
	}

	filters := make([]dataframe.F, len(spec.Filters))
	for i := range spec.Filters {
		fs := &spec.Filters[i]
		fs.Log = config.Log
		if !slices.Contains(df.Names(), fs.Field) {
			return ErrFilterField
		}
		if len(fs.Value) == 0 {
			return ErrFilterValue
		}
		switch df.Select(fs.Field).Types()[0] {
		case series.String:
			filters[i] = createFilter(fs, fs.Value)
		case series.Int:
			val, err := strconv.Atoi(fs.Value)
			if err != nil {
				return err
			}
			filters[i] = createFilter(fs, val)
		case series.Float:
			val, err := strconv.ParseFloat(fs.Value, 64)
			if err != nil {
				return err
			}
			filters[i] = createFilter(fs, val)
		case series.Bool:
			val, err := strconv.ParseBool(fs.Value)
			if err != nil {
				return err
			}
			filters[i] = createFilter(fs, val)
		default:
			return ErrFilterFieldType
		}
	}

	spec.Log.WithFields(logrus.Fields{"aggregation": aggr}).
		Debug("applying filters")
	temp := df.FilterAggregation(aggr, filters...)
	err := errors.Join(df.Error(), temp.Error()) // which one errors?
	*df = temp
	return err
}

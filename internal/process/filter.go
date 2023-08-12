package process

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var (
	ErrBadFilterAggregation = fmt.Errorf(
		"bad aggregation mode, available modes are: %q",
		common.MapKeys(filterAggregations))
)

var filterAggregations = map[string]dataframe.Aggregation{
	"or":  dataframe.Or,
	"and": dataframe.And,
}

// filterSetSpec contains data needed for defining a filter-set Processor.
type filterSetSpec struct {
	// Aggregation defines how filters in the set are combined,
	// i.e., the aggregation mode.
	Aggregation string `yaml:"aggregation"`
	// Filters is a list of filter specifications.
	Filters []filterSpec `yaml:"filters"`
}

// filterSpec contains data needed for defining a filter Processor.
type filterSpec struct {
	// Field is the field name to which the filter is applied.
	Field string `yaml:"field"`
	// Op is the filtering (comparison) operation.
	Op series.Comparator `yaml:"op"`
	// Value is the comparison value.
	Value string `yaml:"value"`
}

// DefaultFilterSetSpec returns a filterSetSpec with 'sensible' default values.
func DefaultFilterSetSpec() filterSetSpec {
	return filterSetSpec{Aggregation: "or"}
}

// createFilter creates a dataframe.F from an input filterSpec
// and a filter (comparison) value.
func createFilter[T validType](spec *filterSpec, val T) dataframe.F {
	if common.Verbose {
		log.Printf("filter: creating: %q %v %v",
			spec.Field, spec.Op, val)
	}
	return dataframe.F{
		Colname:    spec.Field,
		Comparator: spec.Op,
		Comparando: val,
	}
}

// filterProcessor mutates df by applying a set of row filters
// as defined in the config.
// The filter behaviour is described by providing the field name ('field')
// to which the filter is applied, the comparison operator ('op') and
// a comparison value ('value'). Rows satisfying the comparison are kept,
// while others are discarded.
//
// All defined filters are applied at the same time. The way in which they
// are aggregated is controlled by setting the 'aggregation' field in the spec,
// 'and' and 'or' aggregation modes are available.
// The 'or' mode is the default if the 'aggregation' field is unset.
func filterProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultFilterSetSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("filter: %w", err)
	}
	if len(spec.Filters) == 0 {
		return nil
	}
	aggr, found := filterAggregations[strings.ToLower(spec.Aggregation)]
	if !found {
		return fmt.Errorf("filter: %w, got: %q",
			ErrBadFilterAggregation, spec.Aggregation)
	}
	filters := make([]dataframe.F, len(spec.Filters))
	for i := range spec.Filters {
		fs := &spec.Filters[i]
		if !slices.Contains(df.Names(), fs.Field) {
			return fmt.Errorf("filter: %w: %q", common.ErrBadField, fs.Field)
		}
		if fs.Value == "" {
			return fmt.Errorf("filter: %w: %q: %q",
				common.ErrBadFieldValue, "value", fs.Value)
		}
		switch typ := df.Select(fs.Field).Types()[0]; typ {
		case series.String:
			filters[i] = createFilter(fs, fs.Value)
		case series.Int:
			val, err := strconv.Atoi(fs.Value)
			if err != nil {
				return fmt.Errorf("filter: %w", err)
			}
			filters[i] = createFilter(fs, val)
		case series.Float:
			val, err := strconv.ParseFloat(fs.Value, 64)
			if err != nil {
				return fmt.Errorf("filter: %w", err)
			}
			filters[i] = createFilter(fs, val)
		case series.Bool:
			val, err := strconv.ParseBool(fs.Value)
			if err != nil {
				return fmt.Errorf("filter: %w", err)
			}
			filters[i] = createFilter(fs, val)
		default:
			return fmt.Errorf("filter: %w: %v", common.ErrBadFieldType, typ)
		}
	}
	if common.Verbose {
		log.Printf("filter: applying with aggregation: %q", aggr)
	}
	temp := df.FilterAggregation(aggr, filters...)
	err := errors.Join(df.Error(), temp.Error()) // which one errors?
	*df = temp
	if err != nil {
		return fmt.Errorf("filter: %w", err)
	}
	return nil
}

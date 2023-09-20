package process

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
)

// sortSpec contains data needed for defining a single sorting operation.
type sortSpec struct {
	// Field is the field which will be sorted.
	Field string `yaml:"field"`
	// Descending is a flag which controls whether to sort in ascending or
	// descending order.
	Descending bool `yaml:"descending"`
}

// ToOrder converts a sortSpec to a dataframe.Order.
func (s sortSpec) ToOrder() dataframe.Order {
	return dataframe.Order{Colname: s.Field, Reverse: s.Descending}
}

// DefaultSortSpec returns a sortSpec with 'sensible' default values.
func DefaultSortSpec() sortSpec {
	return sortSpec{}
}

// sortProcessor sorts df by 'field' in ascending or descending,
// if 'descending' == true, order. The processor takes a list of fields and
// orderings and applies them in sequence. The order in which the fields
// are listed defines the sorting precedence.
func sortProcessor(df *dataframe.DataFrame, config *Config) error {
	var spec []sortSpec
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("sort: %w", err)
	}
	if len(spec) == 0 {
		return nil
	}
	names := df.Names()
	for i := range spec {
		id := slices.Index(names, spec[i].Field)
		if id == -1 {
			return fmt.Errorf("sort: %w: %q", common.ErrBadField, spec[i].Field)
		}
	}

	f := func(s []sortSpec) []dataframe.Order {
		o := make([]dataframe.Order, 0, len(s))
		for i := range s {
			o = append(o, s[i].ToOrder())
		}
		return o
	}
	temp := df.Arrange(f(spec)...)
	err := errors.Join(df.Error(), temp.Error()) // which one errors?
	*df = temp
	if err != nil {
		return fmt.Errorf("sort: %w", err)
	}
	return nil
}

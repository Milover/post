package process

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
)

// selectSpec contains data needed for defining a select Processor.
type selectSpec struct {
	// Fields is a list of field names to be extracted.
	Fields []string `yaml:"fields"`
	// Remove toggles whether to keep or remove the selected fields.
	Remove bool `yaml:"remove"`
}

// DefaultSelectSpec returns a selectSpec with 'sensible' default values.
func DefaultSelectSpec() selectSpec {
	return selectSpec{}
}

// selectProcessor mutates df by keeping or removing 'fields'.
// If 'remove' is true, the fields are removed, otherwise only the selected
// fields are kept in the order specified.
func selectProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultSelectSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("select: %w", err)
	}
	if len(spec.Fields) == 0 {
		return nil
	}
	ids := make([]int, len(spec.Fields))
	names := df.Names()
	for i := range spec.Fields {
		id := slices.Index(names, spec.Fields[i])
		if id == -1 {
			return fmt.Errorf("select: %w: %q", common.ErrBadField, spec.Fields[i])
		}
		ids[i] = id
	}
	if spec.Remove {
		// invert index selection
		t := make([]int, 0, len(names)-len(ids))
		for i := range names {
			if !slices.Contains(ids, i) {
				t = append(t, i)
			}
		}
		ids = t
	}
	temp := df.Select(ids)
	err := errors.Join(df.Error(), temp.Error()) // which one errors?
	*df = temp
	if err != nil {
		return fmt.Errorf("select: %w", err)
	}
	return nil
}

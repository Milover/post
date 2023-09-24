package process

import (
	"fmt"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
)

// renameSpec contains data needed for defining a select Processor.
type renameSpec struct {
	// Fields are key-value pairs which are used to rename
	// field(s) 'key(s)' to 'value(s)'.
	Fields map[string]string `yaml:"fields"`
}

// DefaultRenameSpec returns a renameSpec with 'sensible' default values.
func DefaultRenameSpec() renameSpec {
	return renameSpec{}
}

// renameProcessor mutates df by renaming fields (columns).
func renameProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultRenameSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	if len(spec.Fields) == 0 {
		return nil
	}
	names := df.Names()
	for _, field := range common.MapKeys(spec.Fields) {
		if !slices.Contains(names, field) {
			return fmt.Errorf("rename: %w: %q", common.ErrBadField, field)
		}
	}

	newNames := make([]string, len(names))
	for i, name := range names {
		if v, found := spec.Fields[name]; found {
			newNames[i] = v
		} else {
			newNames[i] = name
		}
	}
	if err := df.SetNames(newNames...); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

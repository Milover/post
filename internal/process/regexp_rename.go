package process

import (
	"fmt"
	"regexp"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
)

// regexpRenameSpec contains data needed for defining a regexp-rename Processor.
type regexpRenameSpec struct {
	// Src is a regular expression which is used to perform matching
	// on field names.
	Src string `yaml:"src"`
	// Repl is the replacement string. All matches of Src are replaced by Repl.
	// Inside Repl, $ signs are interpreted as in regexp.Expand, so for
	// instance $1 represents the text of the first submatch
	Repl string `yaml:"repl"`
}

// DefaultRegexpRenameSpec returns a regexpRenameSpec with 'sensible' default values.
func DefaultRegexpRenameSpec() regexpRenameSpec {
	return regexpRenameSpec{}
}

// regexpRenameProcessor mutates df by replacing field names which
// match the regular expression src with repl.
// See https://golang.org/s/re2syntax for the regexp syntax description.
func regexpRenameProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultRegexpRenameSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("regexp-rename: %w", err)
	}
	if spec.Src == "" {
		return fmt.Errorf("regexp-rename: %w: %q: %q",
			common.ErrBadFieldValue, "src", spec.Src)
	}
	if spec.Repl == "" {
		return fmt.Errorf("regexp-rename: %w: %q: %q",
			common.ErrBadFieldValue, "repl", spec.Repl)
	}
	re, err := regexp.Compile(spec.Src)
	if err != nil {
		return fmt.Errorf("regexp-rename: %w", err)
	}

	names := df.Names()
	newNames := make([]string, len(names))
	for i, name := range names {
		newNames[i] = re.ReplaceAllString(name, spec.Repl)
	}
	if err := df.SetNames(newNames...); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

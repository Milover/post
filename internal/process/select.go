package process

import (
	"errors"
	"fmt"

	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

var (
	ErrSelectField = errors.New("select: field does not exist")
)

// selectSpec contains data needed for defining a select Processor.
type selectSpec struct {
	Fields []string `yaml:"fields"`

	Log *logrus.Logger `yaml:"-"`
}

// DefaultSelectSpec returns a selectSpec with 'sensible' default values.
func DefaultSelectSpec() selectSpec {
	return selectSpec{}
}

// selectProcessor mutates the dataframe.DataFrame by extracting columns
// specified in the config.
func selectProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultSelectSpec()
	spec.Log = config.Log
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return err
	}
	if len(spec.Fields) == 0 {
		return nil
	}

	ids := make([]int, len(spec.Fields))
	names := df.Names()
	for i := range spec.Fields {
		id := slices.Index(names, spec.Fields[i])
		if id == -1 {
			return fmt.Errorf("%w: %v", ErrSelectField, spec.Fields[i])
		}
		ids[i] = id
	}

	spec.Log.WithFields(logrus.Fields{"fields": spec.Fields}).
		Debug("selecting fields")
	temp := df.Select(ids)
	err := errors.Join(df.Error(), temp.Error()) // which one errors?
	*df = temp
	return err
}

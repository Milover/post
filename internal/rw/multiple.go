package rw

import (
	"errors"
	"fmt"

	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	ErrMultipleEmpty = errors.New("multple: no inputs defined")
)

type multiple struct {
	// FormatSpecs is a list of configs for specifying each input type.
	FormatSpecs []Config `yaml:"format_specs"`
}

func defaultMultiple() *multiple {
	return &multiple{}
}

func NewMultiple(n *yaml.Node) (*multiple, error) {
	rw := defaultMultiple()
	if err := n.Decode(rw); err != nil {
		return nil, err
	}
	if len(rw.FormatSpecs) == 0 {
		return nil, ErrMultipleEmpty
	}
	return rw, nil
}

func (rw *multiple) Read() (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame
	for i := range rw.FormatSpecs {
		temp, err := Read(&rw.FormatSpecs[i])
		if err != nil {
			return nil, err
		}
		// concatonate the new dataframe
		if df == nil {
			df = temp
		} else {
			*df = df.CBind(*temp)
			if df.Error() != nil {
				return nil, fmt.Errorf("multiple: %w", df.Error())
			}
		}
	}
	return df, nil
}

package process

import (
	"fmt"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// binSpec contains data needed for defining a bin Processor.
type binSpec struct {
	// NBins is the number of bins into which the field will be divided.
	NBins int `yaml:"n_bins"`
}

// DefaultBinSpec returns a binSpec with 'sensible' default values.
func DefaultBinSpec() binSpec {
	return binSpec{}
}

// binProcessor mutates df by dividing all numeric fields into 'n_bins'
// and setting the field values to bin-mean-values.
//
// WARNING: each bin MUST contain the same number of field values, i.e.,
// len(field) % 'n_fields' == 0.
// This might change in the future.
// FIXME: should fix at some point by interpolating 'missing' field values or
// something to that effect.
func binProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultBinSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("bin: %w", err)
	}
	if spec.NBins <= 0 || df.Nrow()%spec.NBins != 0 {
		return fmt.Errorf("bin: %w: %q: %v",
			common.ErrBadFieldValue, "n_bins", spec.NBins)
	}
	if err := selectNumFields(df); err != nil {
		return fmt.Errorf("bin: %w", err)
	}
	if err := intsToFloats(df); err != nil {
		return fmt.Errorf("bin: %w", err)
	}
	ss := make([]series.Series, 0, len(df.Names()))

	vals := make([]float64, spec.NBins)
	nPerBin := df.Nrow() / spec.NBins
	for _, name := range df.Names() {
		x := df.Col(name).Float()
		for i := range vals {
			var sum, c, t, y float64
			for j := 0; j < nPerBin; j++ {
				y = x[i*nPerBin+j] - c
				t = sum + y
				c = (t - sum) - y
				sum = t
			}
			vals[i] = sum / float64(nPerBin)
		}
		ss = append(ss, series.New(vals, series.Float, name))
	}
	*df = dataframe.New(ss...)
	if df.Error() != nil {
		return fmt.Errorf("bin: %w", df.Error())
	}
	return nil
}

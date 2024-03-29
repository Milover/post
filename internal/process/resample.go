package process

import (
	"fmt"
	"math"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// resampleSpec contains data needed for defining a resample Processor.
type resampleSpec struct {
	// NPoints is the number of resampling points.
	NPoints int `yaml:"n_points"`
	// X is the name of the independent variable field.
	X string `yaml:"x_field"`
}

// DefaultResampleSpec returns a selectSpec with 'sensible' default values.
func DefaultResampleSpec() resampleSpec {
	return resampleSpec{}
}

type interp struct {
	i0, i1 int     // upper and lower bound indexes
	delta  float64 // interpolation coefficient
}

// interpolate performs linear interpolation using
// stored indices and coefficients.
func (i interp) interpolate(y []float64) float64 {
	return y[i.i0] + (y[i.i1]-y[i.i0])*i.delta
}

// newInterpolation creates interpolation coefficients
// for the mapping f(xOld) -> f(x0).
func newInterpolation(x, xOld []float64) []interp {
	itp := make([]interp, 0, len(x))
	for i := range x {
		// indices of the range of old x values in which new x falls
		high := slices.IndexFunc(xOld, func(a float64) bool { return x[i] < a })
		if high == -1 {
			high = len(xOld) - 1
		} else if high == 0 {
			high = 1
		}
		low := high - 1

		itp = append(itp, interp{
			i0:    low,
			i1:    high,
			delta: (x[i] - xOld[low]) / (xOld[high] - xOld[low]),
		})
	}
	return itp
}

// resampleProcessor mutates df by linearly interpolating all numeric fields,
// such that the resulting fields have 'n_points' values, at uniformly
// distributed values of the field 'x_field'.
// If 'x_field' is not set, a uniform resampling is performed, i.e., as if
// the values of each field were given at a uniformly distributed x,
// where x ∈ [0,1].
// The first and last values of a field are preserved in the resampled field.
func resampleProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultResampleSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("resample: %w", err)
	}
	if spec.NPoints <= 1 {
		return fmt.Errorf("resample: %w: %q: %q",
			common.ErrBadFieldValue, "n_points", spec.NPoints)
	}
	if err := selectNumFields(df); err != nil {
		return fmt.Errorf("resample: %w", err)
	}
	if err := intsToFloats(df); err != nil {
		return fmt.Errorf("resample: %w", err)
	}
	ss := make([]series.Series, 0, len(df.Names()))

	var itp []interp
	distr := func(low, high float64, n int) []float64 {
		x := make([]float64, n)
		d := high - low
		for i := range x {
			t := float64(i) / float64(len(x)-1)
			x[i] = math.FMA(t, d, low)
		}
		return x
	}
	if spec.X != "" { // non-uniform resample
		if found := slices.Index(df.Names(), spec.X); found == -1 {
			return fmt.Errorf("resample: %w: %q", common.ErrBadField, spec.X)
		}
		xOld := df.Col(spec.X).Float()
		x := distr(xOld[0], xOld[len(xOld)-1], spec.NPoints)
		itp = newInterpolation(x, xOld)
		ss = append(ss, series.New(x, series.Float, spec.X))
	} else { // uniform resample
		xOld := distr(0, 1, df.Nrow())
		x := distr(0, 1, spec.NPoints)
		itp = newInterpolation(x, xOld)
	}

	y := make([]float64, spec.NPoints)
	for _, name := range df.Names() {
		if spec.X != "" && name == spec.X {
			continue
		}
		y_o := df.Col(name).Float()
		for i := range itp {
			y[i] = itp[i].interpolate(y_o)
		}
		ss = append(ss, series.New(y, series.Float, name))
	}
	*df = dataframe.New(ss...)
	if df.Error() != nil {
		return fmt.Errorf("resample: %w", df.Error())
	}
	return nil
}

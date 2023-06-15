package process

import (
	"errors"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

var (
	ErrAverageCycleField     = errors.New("average-cycle: bad field")
	ErrAverageCycleFieldType = errors.New("average-cycle: bad field type, must be float64")
	ErrAverageCycleNCycles   = errors.New("average-cycle: bad number of cycles, nCycles % nRows != 0")
	ErrAverageCycleNCycles0  = errors.New("average-cycle: bad number of cycles, nCycles <= 0")
)

// averageCycleSpec contains data needed for defining an averaging Processor.
type averageCycleSpec struct {
	Field   string `yaml:"field"`
	NCycles int    `yaml:"n_cycles"`

	Log *logrus.Logger `yaml:"-"`
}

// defaultAverageCycleSpec returns a averageCycleSpec
// with 'sensible' default values.
func defaultAverageCycleSpec() averageCycleSpec {
	return averageCycleSpec{}
}

// averageCycle computes the enesemble average of a cycle as specified in the
// spec, and sets df to the result.
func averageCycle(df *dataframe.DataFrame, spec *averageCycleSpec) error {
	nRows := df.Nrow()
	if nRows%spec.NCycles != 0 {
		return ErrAverageCycleNCycles
	}
	period := nRows / spec.NCycles
	col := slices.Index(df.Names(), spec.Field)
	avg := make([]float64, period)
	for i := range avg {
		// Khan summation
		var c, t, y float64
		for j := 0; j < spec.NCycles; j++ {
			y = df.Elem(i+j*period, col).Float() - c
			t = avg[i] + y
			c = (t - avg[i]) - y
			avg[i] = t
		}
		avg[i] /= float64(spec.NCycles)
	}
	*df = dataframe.New(series.New(avg, series.Float, spec.Field))
	return df.Error()
}

// averageCycleProcessor computes the enesemble average of a cycle for a single
// field as specified in the config, and sets df to the result.
// The ensemble average is computed as:
//
//	Φ(ωt) = 1/N Σ ϕ[ω(t+j)T], j = 0...N-1
//
// where ϕ is the slice of values to be averaged, ω the angular velocity,
// t the time and T the period.
//
// If an error occurs, the state of df is unknown.
func averageCycleProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := defaultAverageCycleSpec()
	spec.Log = config.Log
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return err
	}
	if len(spec.Field) == 0 || !slices.Contains(df.Names(), spec.Field) {
		return ErrAverageCycleField
	}
	if df.Select(spec.Field).Types()[0] != series.Float {
		return ErrAverageCycleFieldType
	}
	if spec.NCycles <= 0 {
		return ErrAverageCycleNCycles0
	}
	spec.Log.WithFields(logrus.Fields{
		"field":  spec.Field,
		"cycles": spec.NCycles}).
		Debug("averaging cycle")
	return averageCycle(df, &spec)
}

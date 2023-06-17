package process

import (
	"errors"
	"math"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

var (
	ErrAverageCycleField          = errors.New("average-cycle: bad field")
	ErrAverageCycleFieldType      = errors.New("average-cycle: bad field type, must be float64")
	ErrAverageCycleNCycles        = errors.New("average-cycle: bad number of cycles, nCycles % nRows != 0")
	ErrAverageCycleNCycles0       = errors.New("average-cycle: bad number of cycles, nCycles <= 0")
	ErrAverageCycleTimeMismatch   = errors.New("average-cycle: cycle time mismatch")
	ErrAverageCycleNonuniformTime = errors.New("average-cycle: non-uniform cycle time")
)

// averageCycleSpec contains data needed for defining an averaging Processor.
type averageCycleSpec struct {
	Field         string  `yaml:"field"`
	NCycles       int     `yaml:"n_cycles"`
	TimeField     string  `yaml:"time_field"`
	TimePrecision float64 `yaml:"time_precision"`

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
		var c, t, y float64 // Khan summation
		for j := 0; j < spec.NCycles; j++ {
			y = df.Elem(i+j*period, col).Float() - c
			t = avg[i] + y
			c = (t - avg[i]) - y
			avg[i] = t
		}
		avg[i] /= float64(spec.NCycles)
	}
	// build time series
	time := make([]float64, period)
	if len(spec.TimeField) == 0 {
		spec.TimeField = "time"
		tStep := 1.0 / float64(period)
		for i := range time {
			time[i] = tStep * float64(1+i)
		}
	} else {
		// time matching
		spec.Log.WithFields(logrus.Fields{
			"time-field":     spec.TimeField,
			"time-precision": spec.TimePrecision}).
			Debug("matching times")
		col := slices.Index(df.Names(), spec.TimeField)
		tPeriod := df.Elem(period, col).Float() - df.Elem(0, col).Float()
		tStep := tPeriod / float64(period)
		var sum, c, t, y float64 // Khan summation
		for i := range time {
			if i != 0 {
				sum = time[i-1]
			}
			y = tStep - c // ensures uniform time step
			t = sum + y
			c = (t - sum) - y
			time[i] = t

			for j := 0; j < spec.NCycles; j++ {
				target := df.Elem(i+j*period, col).Float() - tPeriod*float64(j)
				var match bool
				if spec.TimePrecision == 0 {
					match = time[i] == target
				} else {
					match = math.Abs(time[i]-target) < spec.TimePrecision
				}
				if !match && j == 0 {
					return ErrAverageCycleNonuniformTime
				} else if !match {
					return ErrAverageCycleTimeMismatch
				}
			}
		}
	}
	*df = dataframe.New(
		series.New(avg, series.Float, spec.Field),
		series.New(time, series.Float, spec.TimeField))
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
// Time matching can be optionally specified, as well as the match precision.
// This checks wheather the time (step) is uniform and weather there is a
// mismatch between the expected time of the averaged value, as per the number
// of cycles defined in the config and the supplied data, and the read time.
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

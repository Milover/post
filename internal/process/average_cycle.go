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
	ErrAverageCycleField         = errors.New("average-cycle: bad field")
	ErrAverageCycleFieldType     = errors.New("average-cycle: bad field type, must be float64")
	ErrAverageCycleNCycles       = errors.New("average-cycle: bad number of cycles, nCycles % nRows != 0")
	ErrAverageCycleNCycles0      = errors.New("average-cycle: bad number of cycles, nCycles <= 0")
	ErrAverageCycleTimeField     = errors.New("average-cycle: bad time field")
	ErrAverageCycleTimePrecision = errors.New("average-cycle: bad time precision, must be >= 0")
	ErrAverageCycleTimeMismatch  = errors.New("average-cycle: cycle time mismatch")
)

// averageCycleSpec contains data needed for defining an averaging Processor.
type averageCycleSpec struct {
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
	vals := make([]float64, period)
	ss := make([]series.Series, 0, df.Ncol()+1)
	// compute the cycle average for each field using Khan sumation
	for col, name := range df.Names() {
		// don't average the time field
		if name == spec.TimeField {
			continue
		}
		if col != 0 {
			for i := range vals {
				vals[i] = 0
			}
		}
		for i := range vals {
			var c, t, y float64
			for j := 0; j < spec.NCycles; j++ {
				y = df.Elem(i+j*period, col).Float() - c
				t = vals[i] + y
				c = (t - vals[i]) - y
				vals[i] = t
			}
			vals[i] /= float64(spec.NCycles)
		}
		ss = append(ss, series.New(vals, series.Float, name))
	}
	// build time series
	for i := range vals {
		vals[i] = float64(i+1) / float64(period)
	}
	if len(spec.TimeField) == 0 {
		spec.TimeField = "time"
	} else {
		// time matching
		spec.Log.WithFields(logrus.Fields{
			"time-field":     spec.TimeField,
			"time-precision": spec.TimePrecision}).
			Debug("matching times")
		col := slices.Index(df.Names(), spec.TimeField)
		tPeriod := df.Elem(period, col).Float() - df.Elem(0, col).Float()
		tStep := tPeriod / float64(period)
		matchTime := make([]float64, period)
		// build the time series by summing the time step using Khan summation
		var sum, c, t, y float64
		for i := range matchTime {
			y = tStep - c // ensures uniform time step
			t = sum + y
			c = (t - sum) - y
			sum, matchTime[i] = t, t

			// check whether the times, at the same points in the cycle,
			// are spaced exactly N periods apart, up to the specified precision
			for j := 0; j < spec.NCycles; j++ {
				target := df.Elem(i+j*period, col).Float() - tPeriod*float64(j)
				var match bool
				if spec.TimePrecision == 0 {
					match = matchTime[i] == target
				} else {
					match = math.Abs(matchTime[i]-target) < spec.TimePrecision
				}
				if !match {
					return ErrAverageCycleTimeMismatch
				}
			}
		}
	}
	ss = append(ss, series.New(vals, series.Float, spec.TimeField))

	*df = dataframe.New(ss...)
	return df.Error()
}

// averageCycleProcessor computes the enesemble average of a cycle
// for all numeric fields as specified in the config, and sets df to the result.
// The ensemble average is computed as:
//
//	Φ(ωt) = 1/N Σ ϕ[ω(t+j)T], j = 0...N-1
//
// where ϕ is the slice of values to be averaged, ω the angular velocity,
// t the time and T the period.
//
// The resulting dataframe.DataFrame will contain the cycle average of
// all numeric fields and a time field (named 'time'),
// containing times for each row of cycle average data, in the range (0, T].
//
// Time matching can be optionally specified, as well as the match precision,
// by setting 'time_field' and 'time_precision' respectively in the config.
// This checks whether the time (step) is uniform and weather there is a
// mismatch between the expected time of the averaged value, as per the number
// of cycles defined in the config and the supplied data, and the read time.
// For example, if there are two cycles, with a period of 1, and a time step
// of 0.25, the expected input is as follows:
//
//	 time |  x  | ...
//	------|-----|-----
//	 0.25 | ... |
//	 0.5  | ... |
//	 0.75 | ... |
//	 1    | ... |
//	 1.25 | ... |
//	 1.5  | ... |
//	 1.75 | ... |
//	 2    | ... |
//
// NOTE: In this case the output time field will be named after 'time_field',
// i.e., the time field name will remain unchanged.
//
// If an error occurs, the state of df is unknown.
func averageCycleProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := defaultAverageCycleSpec()
	spec.Log = config.Log
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return err
	}
	if spec.NCycles <= 0 {
		return ErrAverageCycleNCycles0
	}
	if len(spec.TimeField) != 0 && !slices.Contains(df.Names(), spec.TimeField) {
		return ErrAverageCycleTimeField
	}
	if spec.TimePrecision < 0 {
		return ErrAverageCycleTimePrecision
	}
	// prepare data for averaging
	if err := selectNumFields(df); err != nil {
		return err
	}
	if err := intsToFloats(df); err != nil {
		return err
	}
	spec.Log.WithFields(logrus.Fields{"cycles": spec.NCycles}).
		Debug("averaging cycle")
	return averageCycle(df, &spec)
}

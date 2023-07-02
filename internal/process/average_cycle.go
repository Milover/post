package process

import (
	"errors"

	"github.com/Milover/foam-postprocess/internal/numeric"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

var (
	ErrAverageCycleField         = errors.New("average-cycle: bad field")
	ErrAverageCycleFieldType     = errors.New("average-cycle: bad field type, must be float64")
	ErrAverageCycleNCycles       = errors.New("average-cycle: bad number of cycles")
	ErrAverageCycleNCycles0      = errors.New("average-cycle: bad number of cycles, nCycles <= 0")
	ErrAverageCycleNRowsPerTime  = errors.New("average-cycle: bad number of rows per time")
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
	return averageCycleSpec{
		TimePrecision: numeric.Eps,
	}
}

// entriesPerTimeStep is a function which returns the number of df rows
// associated with a single time step.
func entriesPerTimeStep(df *dataframe.DataFrame, spec *averageCycleSpec) int {
	// if the time field is not specified, there can only be
	// one entry per time step
	if len(spec.TimeField) == 0 {
		return 1
	}
	// XXX: we expect that a field named spec.TimeField exists, and that
	// it is of type series.Float (float64)
	time := df.Col(spec.TimeField).Float() // XXX: does this allocate a new slice?
	var nEntries int
	for _, t := range time {
		if !numeric.EqualEps(time[0], t, spec.TimePrecision) {
			break
		}
		nEntries++
	}
	return nEntries
}

// averageCycle computes the enesemble average of a cycle as specified in the
// spec, and sets df to the result.
// A time field, named 'time', is added to df if 'TimeField' is not set.
// NOTE: the time field, even if 'TimeField' is set, will be the last
// field in df if no error occurs. The order of other fields is preserved.
func averageCycle(df *dataframe.DataFrame, spec *averageCycleSpec) error {
	nRows := df.Nrow()
	if nRows%spec.NCycles != 0 {
		return ErrAverageCycleNCycles
	}
	period := nRows / spec.NCycles
	nPerTime := entriesPerTimeStep(df, spec)
	if period%nPerTime != 0 {
		return ErrAverageCycleNRowsPerTime
	}
	vals := make([]float64, period)
	ss := make([]series.Series, 0, df.Ncol()+1)

	// compute the cycle average for each field using Khan sumation
	for col, name := range df.Names() {
		// don't average the time field
		if name == spec.TimeField {
			continue
		}
		if col != 0 { // reset the sum
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

	// build the time series
	var tCurrent float64
	for i := range vals {
		if i%nPerTime == 0 {
			tCurrent = float64(i/nPerTime+1) / float64(period/nPerTime)
		}
		vals[i] = tCurrent
	}
	// match times
	if len(spec.TimeField) == 0 {
		spec.TimeField = "time"
	} else {
		spec.Log.WithFields(logrus.Fields{
			"time-field":     spec.TimeField,
			"time-precision": spec.TimePrecision}).
			Debug("matching times")
		readT := df.Col(spec.TimeField).Float() // XXX: does this allocate?
		deltaT := readT[nPerTime] - readT[0]
		cycleT := deltaT + readT[period-1] - readT[0]
		offsetT := readT[0] - deltaT

		// check whether the times, at the same points in the cycle,
		// are spaced exactly N periods apart, up to the specified precision
		for i := range vals {
			for j := 0; j < spec.NCycles; j++ {
				computed := offsetT + cycleT*(vals[i]+float64(j))
				read := readT[i+j*period]
				if !numeric.EqualEps(computed, read, spec.TimePrecision) {
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

package process

import (
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/Milover/post/internal/numeric"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var (
	ErrAverageCycleNRowsPerTime = errors.New("average-cycle: bad number of rows per time")
	ErrAverageCycleTimeMismatch = errors.New("average-cycle: cycle time mismatch")
)

// averageCycleSpec contains data needed for
// defining a cycle averaging Processor.
type averageCycleSpec struct {
	// NCycles is the number of cycles in a periodic data set.
	NCycles int `yaml:"n_cycles"`
	// TimeField is the name of the time field.
	// If defined time-matching is turned on.
	TimeField string `yaml:"time_field"`
	// TimePrecision is the time-matching precision.
	TimePrecision float64 `yaml:"time_precision"`
}

// DefaultAverageCycleSpec returns a averageCycleSpec
// with 'sensible' default values.
func DefaultAverageCycleSpec() averageCycleSpec {
	return averageCycleSpec{
		TimePrecision: numeric.Eps,
	}
}

// entriesPerTimeStep is a function which returns the number of df rows
// associated with a single time step.
func entriesPerTimeStep(df *dataframe.DataFrame, spec *averageCycleSpec) int {
	// if the time field is not specified, there can only be
	// one entry per time step
	if spec.TimeField == "" {
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
// The time field will be the last field in df if no error occurs, while
// the order of the other fields is preserved.
//
// Time matching can be optionally specified, as well as the match precision,
// by setting 'time_field' and 'time_precision' respectively in the config.
// This checks whether the time (step) is uniform and whether there is a
// mismatch between the expected time of the averaged value, as per the number
// of cycles defined in the config and the supplied data, and the read time.
// The read time is the one read from the field named 'time_field'.
// For example, if there are 2 cycles with a period of 1, a time step of 0.25,
// and the 'time_field' set to 'time', the expected input and output are:
//
//	      input                     output
//	------------------        ------------------
//	 time |  x  | ...          time |  x  | ...
//	------|-----|-----        ------|-----|-----
//	 0.25 | 0.5 | ...          0.25 | 1.0 | ...
//	 0.5  | 0.5 | ...          0.5  | 1.0 | ...
//	 0.75 | 0.5 | ...          0.75 | 1.0 | ...
//	 1    | 0.5 | ...          1    | 1.0 | ...
//	 1.25 | 1.5 | ...
//	 1.5  | 1.5 | ...
//	 1.75 | 1.5 | ...
//	 2    | 1.5 | ...
//
// NOTE: In this case the output time field will be named after 'time_field',
// i.e., the time field name will remain unchanged.
//
// If an error occurs, the state of df is unknown.
func averageCycleProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultAverageCycleSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("average-cycle: %w", err)
	}
	if spec.NCycles <= 0 {
		return fmt.Errorf("average-cycle: %w: %q: %v",
			common.ErrBadFieldValue, "n_cycles", spec.NCycles)
	}
	if spec.TimeField != "" && !slices.Contains(df.Names(), spec.TimeField) {
		return fmt.Errorf("average-cycle: %w: %q", common.ErrBadField, "time_field")
	}
	if spec.TimePrecision < 0 {
		return fmt.Errorf("average-cycle: %w: %q: %v",
			common.ErrBadFieldValue, "time_precision", spec.TimePrecision)
	}
	// prepare data for averaging
	if err := selectNumFields(df); err != nil {
		return fmt.Errorf("average-cycle: %w", err)
	}
	if err := intsToFloats(df); err != nil {
		return fmt.Errorf("average-cycle: %w", err)
	}

	// compute average
	nRows := df.Nrow()
	if nRows%spec.NCycles != 0 {
		return fmt.Errorf("average-cycle: %w: %q: %v",
			common.ErrBadFieldValue, "n_cycles", spec.NCycles)
	}
	period := nRows / spec.NCycles
	nPerTime := entriesPerTimeStep(df, &spec)
	if period%nPerTime != 0 {
		return ErrAverageCycleNRowsPerTime
	}
	vals := make([]float64, period)
	ss := make([]series.Series, 0, df.Ncol()+1)

	// compute the cycle average for each field using Khan sumation
	for _, name := range df.Names() {
		// don't average the time field
		if name == spec.TimeField {
			continue
		}
		x := df.Col(name).Float()
		for i := range vals {
			var sum, c, t, y float64
			for j := 0; j < spec.NCycles; j++ {
				y = x[i+j*period] - c
				t = sum + y
				c = (t - sum) - y
				sum = t
			}
			vals[i] = sum / float64(spec.NCycles)
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
	if spec.TimeField == "" {
		spec.TimeField = "time"
	} else {
		if common.Verbose {
			log.Printf("average-cycle: matching times with %q to within %v",
				spec.TimeField, spec.TimePrecision)
		}
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
					return fmt.Errorf("%w: computed %v, but read %v",
						ErrAverageCycleTimeMismatch, computed, read)
				}
			}
		}
	}
	ss = append(ss, series.New(vals, series.Float, spec.TimeField))

	*df = dataframe.New(ss...)
	if df.Error() != nil {
		return fmt.Errorf("average-cycle: %w", df.Error())
	}
	return nil
}

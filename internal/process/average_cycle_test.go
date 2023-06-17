package process

import (
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type averageCycleTest struct {
	Name   string
	Config Config
	Spec   string
	Input  dataframe.DataFrame
	Output dataframe.DataFrame
	Error  error
}

// rng is a random number generator used during testing.
var rng *rand.Rand

// addNoise adds (mutates) random noise, up to amplitude amp, to a slice
// of values.
func addNoise(v []float64, amp float64) []float64 {
	if rng == nil {
		rng = rand.New(rand.NewSource(0))
	}
	for i := range v {
		v[i] += amp * rng.Float64()
	}
	return v
}

// multiply performs an elementwise multiply with a constant.
func multiply(v []float64, c float64) []float64 {
	for i := range v {
		v[i] *= c
	}
	return v
}

var averageCycleTests = []averageCycleTest{
	{
		Name: "good",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 4
`,
		Input: dataframe.New(series.New(
			[]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				0.5, 1.5, 2.5, 1.5, 0.5, -0.5,
			}, series.Float, "x")),
		Output: dataframe.New(
			series.New([]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x"),
			series.New(multiply([]float64{1, 2, 3, 4, 5, 6}, 1.0/6.0), series.Float, "time"),
		),
		Error: nil,
	},
	{
		Name: "bad-spec",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: [CRASH ME BBY!]
`,
		Input:  dataframe.New(series.New([]float64{1.0, 2.0}, series.Float, "x")),
		Output: dataframe.New(series.New([]float64{1.0, 2.0}, series.Float, "x")),
		Error: &yaml.TypeError{
			Errors: []string{"line 4: cannot unmarshal !!seq into int"},
		},
	},
	{
		Name: "bad-cycle-field",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: CRASH ME BBY!
  n_cycles: 1
`,
		Input: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Output: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Error: ErrAverageCycleField,
	},
	{
		Name: "bad-empty-cycle-field",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  n_cycles: 1
`,
		Input: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Output: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Error: ErrAverageCycleField,
	},
	{
		Name: "bad-cycle-field-type",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 1
`,
		Input: dataframe.New(series.New(
			[]int{1, 2, 3, 2, 1, 0}, series.Int, "x")),
		Output: dataframe.New(series.New(
			[]int{1, 2, 3, 2, 1, 0}, series.Int, "x")),
		Error: ErrAverageCycleFieldType,
	},
	{
		Name: "bad-n-cycles-0",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 0
`,
		Input: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Output: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Error: ErrAverageCycleNCycles0,
	},
	{
		Name: "bad-n-cycles-negative",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: -1
`,
		Input: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Output: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x")),
		Error: ErrAverageCycleNCycles0,
	},
	{
		Name: "bad-n-cycles",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 2
`,
		Input: dataframe.New(series.New(
			[]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5,
			}, series.Float, "x")),
		Output: dataframe.New(series.New(
			[]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5,
			}, series.Float, "x")),
		Error: ErrAverageCycleNCycles,
	},
	// time matching
	{
		Name: "good-time-matching",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 4
  time_field: t
`,
		Input: dataframe.New(
			series.New([]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				0.5, 1.5, 2.5, 1.5, 0.5, -0.5,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
				7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
				13.0, 14.0, 15.0, 16.0, 17.0, 18.0,
				19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
			}, series.Float, "t"),
		),
		Output: dataframe.New(
			series.New([]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x"),
			series.New([]float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, series.Float, "t"),
		),
		Error: nil,
	},
	{
		Name: "good-time-matching-precision",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 4
  time_field: t
  time_precision: 0.1
`,
		Input: dataframe.New(
			series.New([]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				0.5, 1.5, 2.5, 1.5, 0.5, -0.5,
			}, series.Float, "x"),
			series.New(addNoise([]float64{
				1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
				7.0, 8.0, 9.0, 10.0, 11.0, 12.0,
				13.0, 14.0, 15.0, 16.0, 17.0, 18.0,
				19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
			}, 0.01), series.Float, "t"),
		),
		Output: dataframe.New(
			series.New([]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0}, series.Float, "x"),
			series.New([]float64{
				0.9987454041006346,
				1.9974908082012692,
				2.9962362123019037,
				3.9949816164025385,
				4.993727020503173,
				5.9924724246038075,
			}, series.Float, "t"),
		),
		Error: nil,
	},
	{
		Name: "bad-time-mismatch",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 2
  time_field: t
`,
		Input: dataframe.New(
			series.New([]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
				7.0, 8.0, 9.0, 10.5, 11.0, 12.0,
			}, series.Float, "t"),
		),
		Output: dataframe.New(
			series.New([]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
				7.0, 8.0, 9.0, 10.5, 11.0, 12.0,
			}, series.Float, "t"),
		),
		Error: ErrAverageCycleTimeMismatch,
	},
	{
		Name: "bad-time-nonuniform",
		Config: Config{
			Type: "average-cycle",
		},
		Spec: `
type_spec:
  field: x
  n_cycles: 2
  time_field: t
`,
		Input: dataframe.New(
			series.New([]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 2.0, 3.5, 4.0, 5.5, 6.0,
				7.0, 8.0, 9.5, 10.0, 11.5, 12.0,
			}, series.Float, "t"),
		),
		Output: dataframe.New(
			series.New([]float64{
				1.0, 2.0, 3.0, 2.0, 1.0, 0.0,
				1.5, 2.5, 3.5, 2.5, 1.5, 0.5,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 2.0, 3.5, 4.0, 5.5, 6.0,
				7.0, 8.0, 9.5, 10.0, 11.5, 12.0,
			}, series.Float, "t"),
		),
		Error: ErrAverageCycleNonuniformTime,
	},
}

// TestAverageCycleProcessor tests weather the cycle-average is computed
// correctly.
func TestAverageCycleProcessor(t *testing.T) {
	rand.Seed(0)
	for _, tt := range averageCycleTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.Spec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = averageCycleProcessor(&tt.Input, &tt.Config)

			assert.Equal(tt.Error, err)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

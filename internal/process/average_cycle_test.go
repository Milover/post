package process

import (
	"io"
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
		Output: dataframe.New(series.New(
			[]float64{1.0, 2.0, 3.0, 2.0, 1.0, 0.0},
			series.Float, "x")),
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
}

// TestAverageCycleProcessor tests weather the cycle-average is computed
// correctly.
func TestAverageCycleProcessor(t *testing.T) {
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

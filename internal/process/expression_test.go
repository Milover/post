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

type expressionTest struct {
	Name   string
	Config Config
	Spec   string
	Input  dataframe.DataFrame
	Output dataframe.DataFrame
	Error  error
}

var expressionTests = []expressionTest{
	// const tests
	{
		Name: "good-left-add-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x + 1.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-right-add-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: '1.0 + x'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-sub-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x - 1.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{-1, 0}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-mul-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x * 2.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-mul-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x * 2.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-div-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x * 2.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-pow-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x ** 2.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{1, 4}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-const",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: '(1+2*x**2-2)/2'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{0.5, 3.5}, series.Float, "res"),
		),
		Error: nil,
	},
	// series tests
	{
		Name: "good-add-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x + y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
			series.New([]float64{0, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-add-series-reuse",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x + x'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-add-series-multi-reuse",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x + x + x + x'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 4}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-sub-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x - y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
			series.New([]float64{0, 0}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-mul-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x * y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 2}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 2}, series.Float, "y"),
			series.New([]float64{0, 2}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-div-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x / y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "y"),
			series.New([]float64{0, 0.5}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-pow-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x ** y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 2}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 2}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "y"),
			series.New([]float64{0, 4}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x*(x+2*y-x**y)/y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{2, 1}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{2, 1}, series.Float, "y"),
			series.New([]float64{2, 4}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-unused-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x*(x+2*y-x**y)/y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{2, 1}, series.Float, "y"),
			series.New([]float64{2, 1}, series.Float, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{2, 1}, series.Float, "y"),
			series.New([]float64{2, 1}, series.Float, "z"),
			series.New([]float64{2, 4}, series.Float, "res"),
		),
		Error: nil,
	},
	{
		Name: "good-unused-string-series",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x*(x+2*y-x**y)/y'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{2, 1}, series.Float, "y"),
			series.New([]string{"1", "2"}, series.String, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
			series.New([]float64{2, 1}, series.Float, "y"),
			series.New([]string{"1", "2"}, series.String, "z"),
			series.New([]float64{2, 4}, series.Float, "res"),
		),
		Error: nil,
	},
	// errors
	{
		Name: "bad-expression-undefined",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
		),
		Error: ErrExpression,
	},
	{
		Name: "bad-expression-undefined",
		Config: Config{
			Type: "expression",
		},
		Spec: `
type_spec:
  expression: 'x*(x+2*y-x**y)/y'
`,
		Input: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{1, 2}, series.Float, "x"),
		),
		Error: ErrResult,
	},
}

// TestExpressionProcessor tests weather a dataframe.DataFrame expression,
// defined in the config, is evaluated correctly.
func TestExpressionProcessor(t *testing.T) {
	for _, tt := range expressionTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.Spec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = expressionProcessor(&tt.Input, &tt.Config)

			assert.Equal(tt.Error, err)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

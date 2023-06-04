package process

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type processorTest struct {
	Name   string
	Config Config
	Input  dataframe.DataFrame
	Output dataframe.DataFrame
	Error  error
}

var processorTests = []processorTest{
	{
		Name: "good-dummy",
		Config: Config{
			Type: "dummy",
		},
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "bad-unknown",
		Config: Config{
			Type: "unknown",
		},
		Input:  dataframe.DataFrame{},
		Output: dataframe.DataFrame{},
		Error:  ErrInvalidType,
	},
}

// TestProcess tests weather a single processor is applied correctly, as
// defined in the config, to a dataframe.DataFrame.
func TestProcess(t *testing.T) {
	for _, tt := range processorTests {
		t.Run(tt.Name, func(t *testing.T) {
			// setup
			assert := assert.New(t)
			var hook *test.Hook
			tt.Config.Log, hook = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			err := process(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)

			// check the log
			if err == nil || !errors.Is(err, ErrInvalidType) {
				assert.Equal(1, len(hook.Entries))
				assert.Equal("applying processor", hook.LastEntry().Message)
				assert.Equal(tt.Config.Type, hook.LastEntry().Data["processor"])
			}
			hook.Reset()
			assert.Nil(hook.LastEntry())
		})
	}
}

type filterTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var filterTests = []filterTest{
	{
		Name: "good-int",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  field: x
  op: '>='
  value: 2
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 2, 3}, series.Int, "x"),
			series.New([]int{1, 2, 3, 4}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{2, 3}, series.Int, "x"),
			series.New([]int{3, 4}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-float",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  field: x
  op: '<'
  value: 2.1
`,
		Input: dataframe.New(
			series.New([]float64{0, 1, 2, 3}, series.Float, "x"),
			series.New([]float64{1, 2, 3, 4}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1, 2}, series.Float, "x"),
			series.New([]float64{1, 2, 3}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "bad-spec",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  value: [CRASH ME BBY!]
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: &yaml.TypeError{
			Errors: []string{"line 3: cannot unmarshal !!seq into string"},
		},
	},
	{
		Name: "bad-value",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  field: x
  op: '>='
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: ErrFilterValue,
	},
	{
		Name: "bad-value-conversion",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  field: x
  op: '=='
  value: 42
`,
		Input: dataframe.New(
			series.New([]bool{true, false}, series.Bool, "x"),
			series.New([]bool{false, true}, series.Bool, "y"),
		),
		Output: dataframe.New(
			series.New([]bool{true, false}, series.Bool, "x"),
			series.New([]bool{false, true}, series.Bool, "y"),
		),
		Error: &strconv.NumError{
			Func: "ParseBool",
			Num:  "42",
			Err:  strconv.ErrSyntax,
		},
	},
	{
		Name: "bad-field",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  field: CRASH ME BABY
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: ErrFilterField,
	},
	//	{ // TODO: this one is a bitch to trigger
	//		Name: "bad-type",
	//		Config: Config{
	//			Type: "filter",
	//		},
	//		TypeSpec: "",
	//		Input: dataframe.DataFrame{},
	//		Output: dataframe.DataFrame{},
	//		Error: ErrFilterFieldType,
	//	},
}

// TestFilterProcessor tests weather a single filter is applied correctly, as
// defined in the config, to a dataframe.DataFrame.
func TestFilterProcessor(t *testing.T) {
	for _, tt := range filterTests {
		t.Run(tt.Name, func(t *testing.T) {
			// setup
			assert := assert.New(t)
			var hook *test.Hook
			tt.Config.Log, hook = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err)
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err)

			err = filterProcessor(&tt.Input, &tt.Config)

			assert.Equal(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)

			// check the log
			if err == nil {
				assert.Equal(1, len(hook.Entries))
				assert.Equal("filtering", hook.LastEntry().Message)
				assert.Equal(nil, hook.LastEntry().Data["error"])
			}
			hook.Reset()
			assert.Nil(hook.LastEntry())
		})
	}
}

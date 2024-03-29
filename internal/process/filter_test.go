package process

import (
	"io"
	"strings"
	"testing"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

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
  filters:
    - field: x
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
  filters:
    - field: x
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
		Name: "good-int-set",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  aggregation: or
  filters:
    - field: x
      op: '>='
      value: 2
    - field: y
      op: '<='
      value: 1
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 2, 3}, series.Int, "x"),
			series.New([]int{1, 2, 3, 4}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 2, 3}, series.Int, "x"),
			series.New([]int{1, 3, 4}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-mixed-set",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  aggregation: and
  filters:
    - field: x
      op: '<'
      value: 3.1
    - field: y
      op: '<='
      value: 6
    - field: z
      op: '=='
      value: a
`,
		Input: dataframe.New(
			series.New([]float64{0, 1, 2, 3, 4, 5, 6}, series.Float, "x"),
			series.New([]int{1, 2, 3, 4, 5, 6, 7}, series.Int, "y"),
			series.New([]string{"a", "b", "c", "a", "b", "c", "a"}, series.String, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 3}, series.Float, "x"),
			series.New([]int{1, 4}, series.Int, "y"),
			series.New([]string{"a", "a"}, series.String, "z"),
		),
		Error: nil,
	},
	{
		Name: "bad-aggregation",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  aggregation: CRASH ME BBY!
  filters:
    - field: x
      op: '<'
      value: 2
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: ErrBadFilterAggregation,
	},
	{
		Name: "bad-value",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  filters:
    - field: x
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
		Error: common.ErrBadFieldValue,
	},
	{
		Name: "bad-field",
		Config: Config{
			Type: "filter",
		},
		TypeSpec: `
type_spec:
  filters:
    - field: CRASH ME BBY!
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: common.ErrBadField,
	},
	//	{ // TODO: not sure how to trigger this one
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

// TestFilterProcessor tests whether filters are applied correctly, as
// defined in the config, to a dataframe.DataFrame.
func TestFilterProcessor(t *testing.T) {
	for _, tt := range filterTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = filterProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

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

type assertEqualTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Error    error
}

var assertEqualTests = []assertEqualTest{
	{
		Name: "good-int-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-int-not-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 2}, series.Int, "y"),
		),
		Error: ErrAssertEqualFail,
	},
	{
		Name: "good-string-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
`,
		Input: dataframe.New(
			series.New([]string{"a", "b"}, series.String, "x"),
			series.New([]string{"a", "b"}, series.String, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-bool-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
`,
		Input: dataframe.New(
			series.New([]bool{true, false}, series.Bool, "x"),
			series.New([]bool{true, false}, series.Bool, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-float-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-float-equal-precision",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
  precision: 0.1
`,
		Input: dataframe.New(
			series.New([]float64{0.09, 1.02}, series.Float, "x"),
			series.New([]float64{0.08, 0.96}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-float-multiple-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y, z]
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
			series.New([]float64{0, 1}, series.Float, "z"),
		),
		Error: nil,
	},
	{
		Name: "good-float-multiple-not-equal",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y, z]
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
			series.New([]float64{0, 1.01}, series.Float, "z"),
		),
		Error: ErrAssertEqualFail,
	},
	{
		Name: "bad-field",
		Config: Config{
			Type: "assert-equal",
		},
		TypeSpec: `
type_spec:
  fields: [x, y, z]
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{0, 1}, series.Float, "y"),
		),
		Error: common.ErrBadField,
	},
}

// TestAssertEqualProcessor tests whether the assertion is handled correctly,
// as defined in the config.
func TestAssertEqualProcessor(t *testing.T) {
	for _, tt := range assertEqualTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			common.Verbose = true

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			exp := tt.Input.Copy()
			err = assertEqualProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(exp, tt.Input)
		})
	}
}

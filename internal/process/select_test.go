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

type selectTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var selectTests = []selectTest{
	{
		Name: "good",
		Config: Config{
			Type: "select",
		},
		TypeSpec: `
type_spec:
  fields: [x, y]
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
			series.New([]int{0, 1}, series.Int, "z"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-reorder",
		Config: Config{
			Type: "select",
		},
		TypeSpec: `
type_spec:
  fields: [y, x]
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
			series.New([]int{0, 1}, series.Int, "z"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "y"),
			series.New([]int{0, 1}, series.Int, "x"),
		),
		Error: nil,
	},
	{
		Name: "good-empty",
		Config: Config{
			Type: "select",
		},
		TypeSpec: "",
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "bad-field",
		Config: Config{
			Type: "select",
		},
		TypeSpec: `
type_spec:
  fields: [x, no_exist]
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
}

// TestSelectProcessor tests whether column selection is applied correctly,
// as defined in the config, to a dataframe.DataFrame.
func TestSelectProcessor(t *testing.T) {
	for _, tt := range selectTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = selectProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

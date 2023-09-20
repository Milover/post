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

type sortTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var sortTests = []sortTest{
	{
		Name: "good",
		Config: Config{
			Type: "sort",
		},
		TypeSpec: `
type_spec:
  - field: x
    descending: true
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 2, 3}, series.Int, "x"),
		),
		Output: dataframe.New(
			series.New([]int{3, 2, 1, 0}, series.Int, "x"),
		),
		Error: nil,
	},
	{
		Name: "good-multiple",
		Config: Config{
			Type: "sort",
		},
		TypeSpec: `
type_spec:
  - field: x
    descending: true
  - field: y
    descending: true
  - field: z
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 1, 1}, series.Int, "x"),
			series.New([]int{1, 2, 3, 3}, series.Int, "y"),
			series.New([]int{2, 3, 4, 5}, series.Int, "z"),
		),
		Output: dataframe.New(
			series.New([]int{1, 1, 1, 0}, series.Int, "x"),
			series.New([]int{3, 3, 2, 1}, series.Int, "y"),
			series.New([]int{4, 5, 3, 2}, series.Int, "z"),
		),
		Error: nil,
	},
	{
		Name: "good-overconstrained",
		Config: Config{
			Type: "sort",
		},
		TypeSpec: `
type_spec:
  - field: x
    descending: true
  - field: y
  - field: z
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 3, 3}, series.Int, "x"),
			series.New([]int{5, 3, 4, 2}, series.Int, "y"),
			series.New([]int{0, 1, 2, 3}, series.Int, "z"),
		),
		Output: dataframe.New(
			series.New([]int{3, 3, 1, 0}, series.Int, "x"),
			series.New([]int{2, 4, 3, 5}, series.Int, "y"),
			series.New([]int{3, 2, 1, 0}, series.Int, "z"),
		),
		Error: nil,
	},
	{
		Name: "bad-empty-field",
		Config: Config{
			Type: "sort",
		},
		TypeSpec: `
type_spec:
  - descending: true
  - field: y
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 2}, series.Int, "x"),
			series.New([]int{0, 1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1, 2}, series.Int, "x"),
			series.New([]int{0, 1, 2}, series.Int, "y"),
		),
		Error: common.ErrBadField,
	},
}

// TestSortProcessor tests whether sorting is applied correctly,
// as defined in the config, to a dataframe.DataFrame.
func TestSortProcessor(t *testing.T) {
	for _, tt := range sortTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = sortProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

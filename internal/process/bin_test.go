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

type binTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var binTests = []binTest{
	{
		Name: "good-1",
		Config: Config{
			Type: "bin",
		},
		TypeSpec: `
type_spec:
  n_bins: 1
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 2}, series.Int, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{1}, series.Float, "x"),
		),
		Error: nil,
	},
	{
		Name: "good-3",
		Config: Config{
			Type: "bin",
		},
		TypeSpec: `
type_spec:
  n_bins: 3
`,
		Input: dataframe.New(
			series.New([]int{-1, 0, 1, 1, 1, 1, 4, 2, 0}, series.Int, "x"),
			series.New([]int{-1, 0, 1, 1, 1, 1, 4, 2, 0}, series.Int, "y"),
			series.New([]int{-1, 0, 1, 1, 1, 1, 4, 2, 0}, series.Int, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1, 2}, series.Float, "x"),
			series.New([]float64{0, 1, 2}, series.Float, "y"),
			series.New([]float64{0, 1, 2}, series.Float, "z"),
		),
		Error: nil,
	},
	{
		Name: "bad-nbins-0",
		Config: Config{
			Type: "bin",
		},
		TypeSpec: `
type_spec:
  n_bins: 0
`,
		Input: dataframe.New(
			series.New([]int{-1, 0, 1, 1, 1, 1, 4, 2, 0}, series.Int, "x"),
		),
		Output: dataframe.New(
			series.New([]int{-1, 0, 1, 1, 1, 1, 4, 2, 0}, series.Int, "x"),
		),
		Error: common.ErrBadFieldValue,
	},
	{
		Name: "bad-nbins-nondivisible",
		Config: Config{
			Type: "bin",
		},
		TypeSpec: `
type_spec:
  n_bins: 3
`,
		Input: dataframe.New(
			series.New([]int{0, 1, 2, 3}, series.Int, "x"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1, 2, 3}, series.Int, "x"),
		),
		Error: common.ErrBadFieldValue,
	},
}

// TestBinProcessor tests whether column selection is applied correctly,
// as defined in the config, to a dataframe.DataFrame.
func TestBinProcessor(t *testing.T) {
	for _, tt := range binTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = binProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

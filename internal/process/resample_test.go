package process

import (
	"io"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type resampleTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var resampleTests = []resampleTest{
	{
		Name: "good-uniform-refine",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 11
`,
		Input: dataframe.New(
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "y"),
			series.New([]string{
				"a", "a", "a", "a", "a",
			}, series.String, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-uniform-coarsen",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 5
`,
		Input: dataframe.New(
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "y"),
			series.New([]string{
				"a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a",
			}, series.String, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-uniform-func",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 11
`,
		Input: dataframe.New(
			series.New([]float64{
				5.0, 5.25, 5.5, 5.75, 6.0,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 1.5, 2.0, 1.5, 1.0,
			}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{
				5.0, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 6.0,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 1.2, 1.4, 1.6, 1.8, 2, 1.8, 1.6, 1.4, 1.2, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-refine",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 11
  x_field: x
`,
		Input: dataframe.New(
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "y"),
			series.New([]string{
				"a", "a", "a", "a", "a",
			}, series.String, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-coarsen",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 5
  x_field: x
`,
		Input: dataframe.New(
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "y"),
			series.New([]string{
				"a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a",
			}, series.String, "z"),
		),
		Output: dataframe.New(
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-func",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 11
  x_field: x
`,
		Input: dataframe.New(
			series.New([]float64{
				0.0, 0.25, 0.5, 0.75, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 1.5, 2.0, 1.5, 1.0,
			}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				1.0, 1.2, 1.4, 1.6, 1.8, 2, 1.8, 1.6, 1.4, 1.2, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-nonuniform",
		Config: Config{
			Type: "resample",
		},
		TypeSpec: `
type_spec:
  n_points: 11
  x_field: x
`,
		Input: dataframe.New(
			series.New([]float64{
				0.0, 0.3, 0.5, 0.8, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.3, 0.5, 0.8, 1.0,
			}, series.Float, "y"),
		),
		Output: dataframe.New(
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "x"),
			series.New([]float64{
				0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			}, series.Float, "y"),
		),
		Error: nil,
	},
}

// TestResampleProcessor tests whether resampling is applied correctly,
// as defined in the config, to a dataframe.DataFrame.
func TestResampleProcessor(t *testing.T) {
	for _, tt := range resampleTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = resampleProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

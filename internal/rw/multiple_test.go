package rw

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type multipleTest struct {
	Name   string
	Config string
	Output dataframe.DataFrame
	Error  error
}

var multipleReadTests = []multipleTest{
	{
		Name: "good-dat",
		Config: `
format_specs:
  - type: dat
    fields: [x0, y0]
    type_spec:
      file: 'testdata/foam_series.good/0.1/data.dat'
  - type: dat
    fields: [x1, y1]
    type_spec:
      file: 'testdata/foam_series.good/0.1/data.dat'
`,
		Output: dataframe.New(
			series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x0"),
			series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y0"),
			series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x1"),
			series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y1"),
		),
		Error: nil,
	},
	{
		Name: "good-csv",
		Config: `
format_specs:
  - type: csv
    fields: [x0, y0]
    type_spec:
      file: 'testdata/foam_series.good/0.1/data.csv'
      header: true
  - type: csv
    fields: [x1, y1]
    type_spec:
      file: 'testdata/foam_series.good/0.1/data.csv'
      header: true
  - type: csv
    fields: [x2, y2]
    type_spec:
      file: 'testdata/foam_series.good/0.1/data.csv'
      header: true
`,
		Output: dataframe.New(
			series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x0"),
			series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y0"),
			series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x1"),
			series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y1"),
			series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x2"),
			series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y2"),
		),
		Error: nil,
	},
	{
		Name: "good-foam-series",
		Config: `
format_specs:
  - type: foam-series
    fields: [time0, x0, y0]
    type_spec:
      directory: 'testdata/foam_series.good'
      file: 'data.dat'
      format_spec:
        type: dat
  - type: foam-series
    fields: [time1, x1, y1]
    type_spec:
      directory: 'testdata/foam_series.good'
      file: 'data.dat'
      format_spec:
        type: dat
`,
		Output: dataframe.New(
			series.New([]float64{
				0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
				0.2, 0.2, 0.2, 0.2, 0.2, 0.2,
				0.3, 0.3, 0.3, 0.3, 0.3, 0.3}, series.Float, "time0"),
			series.New([]int{
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5}, series.Int, "x0"),
			series.New([]int{
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0}, series.Int, "y0"),
			series.New([]float64{
				0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
				0.2, 0.2, 0.2, 0.2, 0.2, 0.2,
				0.3, 0.3, 0.3, 0.3, 0.3, 0.3}, series.Float, "time1"),
			series.New([]int{
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5}, series.Int, "x1"),
			series.New([]int{
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0}, series.Int, "y1"),
		),
		Error: nil,
	},
	{
		Name: "bad-row-dimensions",
		Config: `
format_specs:
  - type: foam-series
    fields: [time0, x0, y0]
    type_spec:
      directory: 'testdata/foam_series.good'
      file: 'data.dat'
      format_spec:
        type: dat
  - type: csv
    fields: [x1, y1]
    type_spec:
      file: 'testdata/foam_series.good/0.1/data.csv'
      header: true
`,
		Output: dataframe.DataFrame{},
		Error:  errors.New("arguments have different dimensions"),
	},
}

func TestMultipleRead(t *testing.T) {
	for _, tt := range multipleReadTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewMultiple(&config)
			assert.Nil(err, "unexpected NewFoamSeries() error")
			out, err := rw.Read()

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

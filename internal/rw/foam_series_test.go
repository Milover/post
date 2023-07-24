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

type foamSeriesTest struct {
	Name        string
	Config      string
	Output      dataframe.DataFrame
	SkipCompare bool
	Error       error
}

var goodSeries = dataframe.New(
	series.New([]float64{
		0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
		0.2, 0.2, 0.2, 0.2, 0.2, 0.2,
		0.3, 0.3, 0.3, 0.3, 0.3, 0.3}, series.Float, "time"),
	series.New([]int{
		0, 1, 2, 3, 4, 5,
		0, 1, 2, 3, 4, 5,
		0, 1, 2, 3, 4, 5}, series.Int, "x"),
	series.New([]int{
		0, 1, 2, 2, 1, 0,
		0, 1, 2, 2, 1, 0,
		0, 1, 2, 2, 1, 0}, series.Int, "y"),
)

var foamSeriesReadTests = []foamSeriesTest{
	{
		Name: "good-csv",
		Config: `
directory: 'testdata/foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "good-dat",
		Config: `
directory: 'testdata/foam_series.good'
file: data.dat
time_name: 'time'
format_spec:
  type: dat
`,
		Output: dataframe.New(
			series.New([]float64{
				0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
				0.2, 0.2, 0.2, 0.2, 0.2, 0.2,
				0.3, 0.3, 0.3, 0.3, 0.3, 0.3}, series.Float, "time"),
			series.Ints([]int{
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5}),
			series.Ints([]int{
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0}),
		),
		Error: nil,
	},
	{
		Name: "good-unsorted",
		Config: `
directory: 'testdata/foam_series.good_unsorted'
file: data.dat
time_name: 'time'
format_spec:
  type: dat
`,
		Output: dataframe.New(
			series.New([]float64{
				0, 0,
				1, 1,
				2, 2,
				3, 3,
				4, 4,
				5, 5,
				6, 6,
				7, 7,
				8, 8,
				9, 9,
				10, 10,
				11, 11,
				12, 12,
				13, 13,
				14, 14,
				15, 15,
				16, 16,
				17, 17,
				18, 18,
				19, 19,
				20, 20}, series.Float, "time"),
			series.Ints([]int{
				0, 0,
				0, 1,
				0, 2,
				0, 3,
				0, 4,
				0, 5,
				0, 6,
				0, 7,
				0, 8,
				0, 9,
				0, 10,
				0, 11,
				0, 12,
				0, 13,
				0, 14,
				0, 15,
				0, 16,
				0, 17,
				0, 18,
				0, 19,
				0, 20}),
		),
		Error: nil,
	},
	{
		Name: "good-empty",
		Config: `
directory: 'testdata/foam_series.good_empty'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
`,
		Output:      dataframe.DataFrame{},
		SkipCompare: true,
		Error:       nil,
	},
	{
		Name: "good-empty-times",
		Config: `
directory: 'testdata/foam_series.good_empty_times'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
`,
		Output:      dataframe.DataFrame{},
		SkipCompare: true,
		Error:       nil,
	},
	{
		Name: "good-csv-tar.xz",
		Config: `
archive: 'testdata/foam_series.good.tar.xz'
directory: 'foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "good-csv-archive",
		Config: `
archive: 'testdata/archive.tgz'
directory: 'archive/foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "bad-unequal-rows",
		Config: `
directory: 'testdata/foam_series.bad_unequal_rows'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: dataframe.DataFrame{},
		Error:  errors.New("arguments have different dimensions"),
	},
}

func TestFoamSeriesRead(t *testing.T) {
	for _, tt := range foamSeriesReadTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewFoamSeries(&config)
			assert.Nil(err, "unexpected NewFoamSeries() error")
			out, err := rw.Read()

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else if !tt.SkipCompare {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

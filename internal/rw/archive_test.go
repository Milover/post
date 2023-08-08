package rw

import (
	"io"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type archiveTest struct {
	Name   string
	Config string
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var expected = dataframe.New(
	series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x"),
	series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y"),
)
var expectedSeries = dataframe.New(
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

var archiveReadTests = []archiveTest{
	// csv
	{
		Name: "good-csv-tar",
		Config: `
file: 'testdata/data.csv.tar'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-tar.xz",
		Config: `
file: 'testdata/data.csv.tar.xz'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-txz",
		Config: `
file: 'testdata/data.csv.txz'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-tar.gz",
		Config: `
file: 'testdata/data.csv.tar.gz'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-tgz",
		Config: `
file: 'testdata/data.csv.tgz'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-tar.bzip2",
		Config: `
file: 'testdata/data.csv.tar.bz2'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-tbz",
		Config: `
file: 'testdata/data.csv.tbz'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-zip",
		Config: `
file: 'testdata/data.csv.zip'
format_spec:
  type: csv
  type_spec:
    file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-archive.tgz",
		Config: `
file: 'testdata/archive.tgz'
format_spec:
  type: csv
  type_spec:
    file: 'archive/data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-csv-archive.zip",
		Config: `
file: 'testdata/archive.zip'
format_spec:
  type: csv
  type_spec:
    file: 'archive/data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	// foam-series
	{
		Name: "good-series-tar.xz",
		Config: `
file: 'testdata/foam_series.good.tar.xz'
format_spec:
  type: foam-series
  type_spec:
    directory: 'foam_series.good'
    file: data.csv
    time_name: 'time'
    format_spec:
      type: csv
`,
		Output: expectedSeries,
		Error:  nil,
	},
	{
		Name: "good-series-zip",
		Config: `
file: 'testdata/foam_series.good.zip'
format_spec:
  type: foam-series
  type_spec:
    directory: 'foam_series.good'
    file: data.csv
    time_name: 'time'
    format_spec:
      type: csv
`,
		Output: expectedSeries,
		Error:  nil,
	},
	{
		Name: "good-series-archive.tgz",
		Config: `
file: 'testdata/archive.tgz'
format_spec:
  type: foam-series
  type_spec:
    directory: 'archive/foam_series.good'
    file: data.csv
    time_name: 'time'
    format_spec:
      type: csv
`,
		Output: expectedSeries,
		Error:  nil,
	},
	{
		Name: "good-series-archive.zip",
		Config: `
file: 'testdata/archive.zip'
format_spec:
  type: foam-series
  type_spec:
    directory: 'archive/foam_series.good'
    file: data.csv
    time_name: 'time'
    format_spec:
      type: csv
`,
		Output: expectedSeries,
		Error:  nil,
	},
}

func TestArchiveRead(t *testing.T) {
	for _, tt := range archiveReadTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewArchive(&config)
			assert.Nil(err, "unexpected NewArchive() error")
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

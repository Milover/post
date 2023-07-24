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

type csvTest struct {
	Name   string
	Config string
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var csvReadOutOfTests = []csvTest{
	{
		Name:   "good-default",
		Config: "",
		Input:  "x,y\n0,1\n1,2",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name:   "good-default-w-empty-line",
		Config: "",
		Input:  "\nx,y\n\n0,1\n1,2\n\n",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name:   "good-default-w-comment",
		Config: "",
		Input:  "#comment\nx,y\n0,1\n# comment\n1,2\n# comment",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-no-header",
		Config: `
header: false
`,
		Input: "0,1\n1,2",
		Output: dataframe.New(
			series.Ints([]int{0, 1}),
			series.Ints([]int{1, 2}),
		),
		Error: nil,
	},
	{
		Name: "good-delimiter",
		Config: `
delimiter: "\t"
`,
		Input: "x\ty\n0\t1\n1\t2",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-no-header-delimiter-w-comment",
		Config: `
header: false
delimiter: " "
comment: "§"
`,
		Input: "§comment\n\n0 1\n§ comment\n1 2\n§ comment",
		Output: dataframe.New(
			series.Ints([]int{0, 1}),
			series.Ints([]int{1, 2}),
		),
		Error: nil,
	},
}

func TestCsvReadOutOf(t *testing.T) {
	for _, tt := range csvReadOutOfTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewCsv(&config)
			assert.Nil(err, "unexpected NewCsv() error")
			out, err := rw.ReadOutOf(strings.NewReader(tt.Input))

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

// Test reading from a CSV file/archive.
var expected = dataframe.New(
	series.New([]int{0, 1, 2, 3, 4, 5}, series.Int, "x"),
	series.New([]int{0, 1, 2, 2, 1, 0}, series.Int, "y"),
)

var csvReadTests = []csvTest{
	{
		Name: "good-file",
		Config: `
file: 'testdata/data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-tar",
		Config: `
archive: 'testdata/data.csv.tar'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-tar.xz",
		Config: `
archive: 'testdata/data.csv.tar'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-txz",
		Config: `
archive: 'testdata/data.csv.txz'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-tar.gz",
		Config: `
archive: 'testdata/data.csv.tar.gz'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-tgz",
		Config: `
archive: 'testdata/data.csv.tgz'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-tar.bzip2",
		Config: `
archive: 'testdata/data.csv.tar.bz2'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-tbz",
		Config: `
archive: 'testdata/data.csv.tbz'
file: 'data.csv'
`,
		Output: expected,
		Error:  nil,
	},
	{
		Name: "good-archive",
		Config: `
archive: 'testdata/archive.tgz'
file: 'archive/data.csv'
`,
		Output: expected,
		Error:  nil,
	},
}

func TestCsvRead(t *testing.T) {
	for _, tt := range csvReadTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewCsv(&config)
			assert.Nil(err, "unexpected NewCsv() error")
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

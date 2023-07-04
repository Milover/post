package input

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

// Test weather decodeRuneOrDefault works correctly.
type decodeRuneTest struct {
	Name    string
	Input   string
	Default rune
	Output  rune
}

var decodeRuneTests = []decodeRuneTest{
	{
		Name:    "good-character-decode",
		Input:   "x",
		Default: '0',
		Output:  'x',
	},
	{
		Name:    "good-string-decode",
		Input:   "xbla",
		Default: '0',
		Output:  'x',
	},
	{
		Name:    "default-decode",
		Input:   "",
		Default: 'x',
		Output:  'x',
	},
	{
		Name:    "bad-decode",
		Input:   string(rune(-1)),
		Default: 'x',
		Output:  'x',
	},
}

func TestDecodeRuneOrDefault(t *testing.T) {
	for _, tt := range decodeRuneTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			out := decodeRuneOrDefault(tt.Input, tt.Default)

			assert.Equal(tt.Output, out)
		})
	}
}

// Test weather fromCSV correctly applies config options, and
// reads and constructs the dataframe.DataFrame.
type fromCSVTest struct {
	Name       string
	Config     Config
	FormatSpec string
	Input      string
	Output     dataframe.DataFrame
	Error      error
}

var fromCSVTests = []fromCSVTest{
	{
		Name:       "good-default",
		Config:     Config{},
		FormatSpec: "",
		Input:      "x,y\n0,1\n1,2",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name:       "good-default-w-empty-line",
		Config:     Config{},
		FormatSpec: "",
		Input:      "\nx,y\n\n0,1\n1,2\n\n",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name:       "good-default-w-comment",
		Config:     Config{},
		FormatSpec: "",
		Input:      "#comment\nx,y\n0,1\n# comment\n1,2\n# comment",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name:   "good-no-header",
		Config: Config{},
		FormatSpec: `
format_spec:
  has_header: false
`,
		Input: "0,1\n1,2",
		Output: dataframe.New(
			series.Ints([]int{0, 1}),
			series.Ints([]int{1, 2}),
		),
		Error: nil,
	},
	{
		Name:   "good-delimiter",
		Config: Config{},
		FormatSpec: `
format_spec:
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
		Name:   "good-no-header-delimiter-w-comment",
		Config: Config{},
		FormatSpec: `
format_spec:
  has_header: false
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
	{
		Name:   "bad-config",
		Config: Config{},
		FormatSpec: `
format_spec:
  has_header: CRASH ME BBY!
`,
		Input:  "",
		Output: dataframe.DataFrame{},
		Error: &yaml.TypeError{
			Errors: []string{"line 3: cannot unmarshal !!str `CRASH M...` into bool"},
		},
	},
}

func TestFromCSV(t *testing.T) {
	for _, tt := range fromCSVTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			raw, err := io.ReadAll(strings.NewReader(tt.FormatSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			out, err := fromCSV(strings.NewReader(tt.Input), &tt.Config)

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

// Test weather fromDAT correctly applies config options, and
// reads and constructs the dataframe.DataFrame.
type fromDATTest struct {
	Name   string
	Config Config
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var fromDATTests = []fromDATTest{
	{
		Name:   "good-default",
		Config: Config{},
		Input:  "# comment\n\n0 \t1\t(0 0 0)\n# comment\n1 \t2\t(1 1 1)\n",
		Output: dataframe.New(
			series.Ints([]int{0, 1}),
			series.Ints([]int{1, 2}),
			series.Ints([]int{0, 1}),
			series.Ints([]int{0, 1}),
			series.Ints([]int{0, 1}),
		),
		Error: nil,
	},
	{
		Name:   "empty-dat",
		Config: Config{},
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  errors.New("load records: empty DataFrame"),
	},
	//	{ // TODO: not sure how to trigger this one
	//		Name:   "bad-dat-read",
	//		Config: Config{},
	//		Input:  "",
	//		Output: dataframe.DataFrame{},
	//		Error:  nil,
	//	},
}

func TestFromDAT(t *testing.T) {
	for _, tt := range fromDATTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			out, err := fromDAT(strings.NewReader(tt.Input), &tt.Config)

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

// Test weather CreateDataFrame correctly applies config options, and
// reads and constructs the dataframe.DataFrame.
type readDataFrameTest struct {
	Name   string
	Config Config
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var readDataFrameTests = []readDataFrameTest{
	{
		Name: "good-dat",
		Config: Config{
			Format: "dat",
			Fields: []string{"a", "b", "c", "d", "e"},
		},
		Input: "# comment\n\n0 \t1\t(0 0 0)\n# comment\n1 \t2\t(1 1 1)\n",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "a"),
			series.New([]int{1, 2}, series.Int, "b"),
			series.New([]int{0, 1}, series.Int, "c"),
			series.New([]int{0, 1}, series.Int, "d"),
			series.New([]int{0, 1}, series.Int, "e"),
		),
		Error: nil,
	},
	{
		Name: "good-csv",
		Config: Config{
			Format: "csv",
		},
		Input: "x,y\n0,1\n1,2",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "bad-format",
		Config: Config{
			Format: "",
		},
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  ErrInvalidFormat,
	},
	{
		Name: "bad-dat",
		Config: Config{
			Format: "dat",
		},
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  errors.New("load records: empty DataFrame"),
	},
	{
		Name: "bad-csv",
		Config: Config{
			Format: "csv",
		},
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  errors.New("load records: empty DataFrame"),
	},
	{
		Name: "bad-fields",
		Config: Config{
			Format: "csv",
			Fields: []string{"a", "b", "c", "d", "e"},
		},
		Input:  "x,y\n0,1\n1,2",
		Output: dataframe.DataFrame{},
		Error:  errors.New("setting names: wrong dimensions"),
	},
}

func TestReadDataFrame(t *testing.T) {
	for _, tt := range readDataFrameTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			out, err := ReadDataFrame(strings.NewReader(tt.Input), &tt.Config)

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

// Test weather ReadSeries correctly applies config options, and
// reads and constructs the dataframe.DataFrame.
type readSeriesTest struct {
	Name        string
	Config      Config
	SeriesSpec  string
	Output      dataframe.DataFrame
	SkipCompare bool
	Error       error
}

var readSeriesTests = []readSeriesTest{
	{
		Name: "good-csv",
		Config: Config{
			Format: "csv",
			Fields: []string{"x", "y"},
		},
		SeriesSpec: `
series_spec:
  series_directory: 'testdata/foam_series.good'
  series_file: 'data.csv'
  series_time_name: 'time'
`,
		Output: dataframe.New(
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
		),
		Error: nil,
	},
	{
		Name: "good-dat",
		Config: Config{
			Format: "dat",
			Fields: []string{"x", "y"},
		},
		SeriesSpec: `
series_spec:
  series_directory: 'testdata/foam_series.good'
  series_file: 'data.dat'
  series_time_name: 'time'
`,
		Output: dataframe.New(
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
		),
		Error: nil,
	},
	{
		Name: "good-unsorted",
		Config: Config{
			Format: "dat",
			Fields: []string{"x"},
		},
		SeriesSpec: `
series_spec:
  series_directory: 'testdata/foam_series.good.unsorted'
  series_file: 'data.dat'
  series_time_name: 'time'
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
			series.New([]int{
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
				0, 20}, series.Int, "x"),
		),
		Error: nil,
	},
	{
		Name: "good-empty",
		Config: Config{
			Format: "csv",
			Fields: []string{"x", "y"},
		},
		SeriesSpec: `
series_spec:
  series_directory: 'testdata/foam_series.good_empty'
  series_file: 'data.csv'
  series_time_name: 'time'
`,
		Output:      dataframe.DataFrame{},
		SkipCompare: true,
		Error:       nil,
	},
	{
		Name: "good-empty-times",
		Config: Config{
			Format: "csv",
			Fields: []string{"x", "y"},
		},
		SeriesSpec: `
series_spec:
  series_directory: 'testdata/foam_series.good_empty'
  series_file: 'data.csv'
  series_time_name: 'time'
`,
		Output:      dataframe.DataFrame{},
		SkipCompare: true,
		Error:       nil,
	},
	{
		Name: "bad-unequal-rows",
		Config: Config{
			Format: "csv",
			Fields: []string{"x", "y"},
		},
		SeriesSpec: `
series_spec:
  series_directory: 'testdata/foam_series.bad_unequal_rows'
  series_file: 'data.csv'
  series_time_name: 'time'
`,
		Output: dataframe.DataFrame{},
		Error:  errors.New("arguments have different dimensions"),
	},
}

func TestReadSeries(t *testing.T) {
	for _, tt := range readSeriesTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			raw, err := io.ReadAll(strings.NewReader(tt.SeriesSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			out, err := ReadSeries(&tt.Config)

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else if !tt.SkipCompare {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

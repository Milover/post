package input

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// handleError is a helper that fails the test if the error is not nil.
func handleError(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
}

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
		Name:    "TODO:bad-decode",
		Input:   string(rune(-1)),
		Default: 'x',
		Output:  'x',
	},
}

func TestDecodeRuneOrDefault(t *testing.T) {
	for _, tt := range decodeRuneTests {
		t.Run(tt.Name, func(t *testing.T) {
			out := decodeRuneOrDefault(tt.Input, tt.Default)

			if !reflect.DeepEqual(out, tt.Output) {
				t.Fatalf("DecodeRuneOrDefault() output:\ngot  %q\nwant %q", out, tt.Output)
			}
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
	Crash      bool
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
		Crash: false,
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
		Crash: false,
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
		Crash: false,
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
		Crash: false,
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
		Crash: false,
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
		Crash: false,
	},
	{
		Name:       "TODO:bad-config",
		Config:     Config{},
		FormatSpec: "",
		Input:      "",
		Output:     dataframe.DataFrame{},
		Crash:      true,
	},
}

func TestFromCSV(t *testing.T) {
	for _, tt := range fromCSVTests {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.Crash {
				t.Skip("skipping because the test requires a custom logger")
			}
			tt.Config.Log = logrus.New()
			raw, err := io.ReadAll(strings.NewReader(tt.FormatSpec))
			handleError(err, t)
			handleError(yaml.Unmarshal(raw, &tt.Config), t)

			out := fromCSV(strings.NewReader(tt.Input), &tt.Config)

			err = out.Error()
			if err != nil {
				t.Fatalf("unexpected fromCSV() error: %v", err)
			}
			if !reflect.DeepEqual(*out, tt.Output) {
				t.Fatalf("fromCSV() output:\ngot  %v\nwant %v", *out, tt.Output)
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
	Crash  bool
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
		Crash: false,
	},
	{
		Name:   "TODO:bad-dat",
		Config: Config{},
		Input:  "",
		Output: dataframe.DataFrame{},
		Crash:  true,
	},
}

func TestFromDAT(t *testing.T) {
	for _, tt := range fromDATTests {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.Crash {
				t.Skip("skipping because the test requires a custom logger")
			}
			tt.Config.Log = logrus.New()
			out := fromDAT(strings.NewReader(tt.Input), &tt.Config)

			err := out.Error()
			if err != nil {
				t.Fatalf("unexpected fromDAT() error: %v", err)
			}
			if !reflect.DeepEqual(*out, tt.Output) {
				t.Fatalf("fromDAT() output:\ngot  %v\nwant %v", *out, tt.Output)
			}
		})
	}
}

// Test weather CreateDataFrame correctly applies config options, and
// reads and constructs the dataframe.DataFrame.
type createDataFrameTest struct {
	Name   string
	Config Config
	Input  string
	Output dataframe.DataFrame
	Crash  bool
}

var createDataFrameTests = []createDataFrameTest{
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
		Crash: false,
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
		Crash: false,
	},
	{
		Name: "TODO:bad-format",
		Config: Config{
			Format: "",
		},
		Input:  "",
		Output: dataframe.DataFrame{},
		Crash:  true,
	},
	{
		Name: "TODO:bad-dataframe",
		Config: Config{
			Format: "dat",
		},
		Input:  "",
		Output: dataframe.DataFrame{},
		Crash:  true,
	},
	{
		Name: "TODO:bad-fields",
		Config: Config{
			Format: "csv",
			Fields: []string{"a", "b", "c", "d", "e"},
		},
		Input:  "x,y\n0,1\n1,2",
		Output: dataframe.DataFrame{},
		Crash:  true,
	},
}

func TestCreateDataFrame(t *testing.T) {
	for _, tt := range createDataFrameTests {
		t.Run(tt.Name, func(t *testing.T) {
			if tt.Crash {
				t.Skip("skipping because the test requires a custom logger")
			}
			tt.Config.Log = logrus.New()
			out := CreateDataFrame(strings.NewReader(tt.Input), &tt.Config)

			err := out.Error()
			if err != nil {
				t.Fatalf("unexpected CreateDataFrame() error: %v", err)
			}
			if !reflect.DeepEqual(*out, tt.Output) {
				t.Fatalf("CreateDataFrame() output:\ngot  %v\nwant %v", *out, tt.Output)
			}
		})
	}
}

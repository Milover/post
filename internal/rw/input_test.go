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

// Test whether DecodeRuneOrDefault works correctly.
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
			out := DecodeRuneOrDefault(tt.Input, tt.Default)

			assert.Equal(tt.Output, out)
		})
	}
}

// Test whether ReadFromFn works correctly.
type readFromFnTest struct {
	Name   string
	Config string
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var readFromFnTests = []readFromFnTest{
	{
		Name: "good-dat",
		Config: `
type: dat
fields: [a, b, c, d, e]
`,
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
		Config: `
type: csv
`,
		Input: "x,y\n0,1\n1,2",
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "bad-type",
		Config: `
type: 'CRASH_ME_BBY!'
`,
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  ErrBadReaderOutOf,
	},
}

type readFakeCloser struct {
	r io.Reader
}

func (r *readFakeCloser) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *readFakeCloser) Close() error               { return nil }

func TestReadFromFn(t *testing.T) {
	for _, tt := range readFromFnTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config Config
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			fn := func(_ string) (io.ReadCloser, error) {
				return &readFakeCloser{r: strings.NewReader(tt.Input)}, nil
			}
			out, err := ReadFromFn(fn, &config)

			assert.ErrorIs(err, tt.Error)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}

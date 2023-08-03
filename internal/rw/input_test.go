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

// Test whether ReadOutOf works correctly.
type readOutOfTest struct {
	Name   string
	Config string
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var readOutOfTests = []readOutOfTest{
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
	{
		Name: "bad-dat",
		Config: `
type: dat
`,
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  errors.New("load records: empty DataFrame"),
	},
	{
		Name: "bad-csv",
		Config: `
type: csv
`,
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  errors.New("load records: empty DataFrame"),
	},
	{
		Name: "bad-fields",
		Config: `
type: csv
fields: [a, b, c, d, e]
`,
		Input:  "x,y\n0,1\n1,2",
		Output: dataframe.DataFrame{},
		Error:  errors.New("setting names: wrong dimensions"),
	},
}

func TestReadOutOf(t *testing.T) {
	for _, tt := range readOutOfTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config Config
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			out, err := ReadOutOf(strings.NewReader(tt.Input), &config)

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, *out)
			}
		})
	}
}
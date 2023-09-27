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

type regexpRenameTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var regexpRenameTests = []regexpRenameTest{
	{
		Name: "good-numbered-capture-group",
		Config: Config{
			Type: "regexp-rename",
		},
		TypeSpec: `
type_spec:
  src: '(.*)'
  repl: '${1}_0'
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x_0"),
			series.New([]int{0, 1}, series.Int, "y_0"),
		),
		Error: nil,
	},
	{
		Name: "good-named-capture-group",
		Config: Config{
			Type: "regexp-rename",
		},
		TypeSpec: `
type_spec:
  src: '(?P<name>.*)'
  repl: '${name}_0'
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x_0"),
			series.New([]int{0, 1}, series.Int, "y_0"),
		),
		Error: nil,
	},
}

// TextRegexpRenameProcessor tests whether regexp renaming of field names,
// as defined in the config, is applied correctly to a dataframe.DataFrame.
func TestRegexpRenameProcessor(t *testing.T) {
	for _, tt := range regexpRenameTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = regexpRenameProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

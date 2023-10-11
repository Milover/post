package process

import (
	"io"
	"strings"
	"testing"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type renameTest struct {
	Name     string
	Config   Config
	TypeSpec string
	Input    dataframe.DataFrame
	Output   dataframe.DataFrame
	Error    error
}

var renameTests = []renameTest{
	{
		Name: "good",
		Config: Config{
			Type: "rename",
		},
		TypeSpec: `
type_spec:
  fields:
    x: y
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "good-multiple",
		Config: Config{
			Type: "rename",
		},
		TypeSpec: `
type_spec:
  fields:
    x: y
    y: z
    z: x
`,
		Input: dataframe.New(
			series.New([]int{0, 0}, series.Int, "x"),
			series.New([]int{0, 1}, series.Int, "y"),
			series.New([]int{0, 2}, series.Int, "z"),
		),
		Output: dataframe.New(
			series.New([]int{0, 0}, series.Int, "y"),
			series.New([]int{0, 1}, series.Int, "z"),
			series.New([]int{0, 2}, series.Int, "x"),
		),
		Error: nil,
	},
	{
		Name: "good-some",
		Config: Config{
			Type: "rename",
		},
		TypeSpec: `
type_spec:
  fields:
    def_0: xyz
`,
		Input: dataframe.New(
			series.New([]int{0, 0}, series.Int, "abc_0"),
			series.New([]int{0, 1}, series.Int, "def_0"),
			series.New([]int{0, 2}, series.Int, "ghi_0"),
		),
		Output: dataframe.New(
			series.New([]int{0, 0}, series.Int, "abc_0"),
			series.New([]int{0, 1}, series.Int, "xyz"),
			series.New([]int{0, 2}, series.Int, "ghi_0"),
		),
		Error: nil,
	},
	{
		Name: "bad-field",
		Config: Config{
			Type: "rename",
		},
		TypeSpec: `
type_spec:
  fields:
    y: x
`,
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
		),
		Error: common.ErrBadField,
	},
}

// TestRenameProcessor tests whether column selection is applied correctly,
// as defined in the config, to a dataframe.DataFrame.
func TestRenameProcessor(t *testing.T) {
	for _, tt := range renameTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.TypeSpec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = renameProcessor(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

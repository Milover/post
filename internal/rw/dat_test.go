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

type datTest struct {
	Name   string
	Config string
	Input  string
	Output dataframe.DataFrame
	Error  error
}

var datReadTests = []datTest{
	{
		Name:   "good-default",
		Config: "",
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
		Config: "",
		Input:  "",
		Output: dataframe.DataFrame{},
		Error:  errors.New("load records: empty DataFrame"),
	},
	//	{ // TODO: not sure how to trigger this one
	//		Name: "bad-dat-read",
	//		Config: `
	//
	// file: test
	// `,
	//
	//		Input:  "",
	//		Output: dataframe.DataFrame{},
	//		Error:  nil,
	//	},
}

func TestDatReadOutOf(t *testing.T) {
	for _, tt := range datReadTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewDat(&config)
			assert.Nil(err, "unexpected NewDat() error")
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

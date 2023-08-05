package process

import (
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
)

type processorTest struct {
	Name   string
	Config Config
	Input  dataframe.DataFrame
	Output dataframe.DataFrame
	Error  error
}

var processorTests = []processorTest{
	{
		Name: "good-dummy",
		Config: Config{
			Type: "dummy",
		},
		Input: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Output: dataframe.New(
			series.New([]int{0, 1}, series.Int, "x"),
			series.New([]int{1, 2}, series.Int, "y"),
		),
		Error: nil,
	},
	{
		Name: "bad-unknown",
		Config: Config{
			Type: "unknown",
		},
		Input:  dataframe.DataFrame{},
		Output: dataframe.DataFrame{},
		Error:  ErrInvalidType,
	},
}

// TestProcess tests weather a single processor is applied correctly, as
// defined in the config, to a dataframe.DataFrame.
func TestProcess(t *testing.T) {
	for _, tt := range processorTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			err := process(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

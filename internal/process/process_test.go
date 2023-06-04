package process

import (
	"errors"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// handleError is a helper that fails the test if the error is not nil.
func handleError(err error, t *testing.T) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected test error: %v", err)
	}
}

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
			var hook *test.Hook
			tt.Config.Log, hook = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			err := process(&tt.Input, &tt.Config)

			assert.ErrorIs(err, tt.Error)
			assert.Equal(tt.Input, tt.Output)

			// check the log
			if err == nil || !errors.Is(err, ErrInvalidType) {
				assert.Equal(1, len(hook.Entries))
				assert.Equal(logrus.DebugLevel, hook.LastEntry().Level)
				assert.Equal("Applying processor", hook.LastEntry().Message)
				assert.Equal(tt.Config.Type, hook.LastEntry().Data["processor"])
			}
			hook.Reset()
			assert.Nil(hook.LastEntry())
		})
	}
}

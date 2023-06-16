package process

import (
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

type expressionTest struct {
	Name   string
	Config Config
	Spec   string
	Input  dataframe.DataFrame
	Output dataframe.DataFrame
	Error  error
}

var expressionTests = []expressionTest{
	{
		Name: "good-simple",
		Config: Config{
			Type: "filter",
		},
		Spec: `
type_spec:
  expression: 'x + 1.0'
  result: 'res'
`,
		Input: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
		),
		Output: dataframe.New(
			series.New([]float64{0, 1}, series.Float, "x"),
			series.New([]float64{1, 2}, series.Float, "res"),
		),
		Error: nil,
	},
}

// TestFilterProcessor tests weather filters are applied correctly, as
// defined in the config, to a dataframe.DataFrame.
func TestExpressionProcessor(t *testing.T) {
	for _, tt := range expressionTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			tt.Config.Log, _ = test.NewNullLogger()
			tt.Config.Log.SetLevel(logrus.DebugLevel)

			// read spec
			raw, err := io.ReadAll(strings.NewReader(tt.Spec))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &tt.Config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = expressionProcessor(&tt.Input, &tt.Config)

			assert.Equal(tt.Error, err)
			assert.Equal(tt.Output, tt.Input)
		})
	}
}

package cmd

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type runTest struct {
	Name   string
	Error  error
	Config string
	Input  string
}

var runTests = []runTest{
	{
		Name:  "basic-csv",
		Error: nil,
		Config: `
input:
  file: in.csv
  fields: [time, u_max, u_x, u_y, u_z]
  format: csv
  format_spec:
    has_header: true
    delimiter: "\t"
    comment: "#"
process:
  - type: dummy
  - type: filter
    type_spec:
      aggregation: and
      filters:
        - field: time
          op: '<='
          value: 4
        - field: u_max
          op: '<'
          value: 7
  - type: dummy
output:
  directory: ""
  table_file: "basic-csv"
  graphs:
    - name: "graph-csv0"
      axes:
        - x:
            min: 1.0
            max: 3.0
            label: "$x$-axis"
          y:
            min: 1.0
            max: 6.0
            label: "$y$-axis"
          tables:
            - x_field: time
              y_field: u_max
              legend_entry: '$u_\text{max}$'
            - x_field: time
              y_field: u_x
              legend_entry: "$u_x$"
`,
		Input: `
time	u_max	u_x	u_y	u_z
1	2.0	1.0	0.0	0.0
2	4.0	2.0	0.0	0.0
3	6.0	3.0	0.0	0.0
4	8.0	4.0	0.0	0.0
5	10.0	5.0	0.0	0.0
6	12.0	6.0	0.0	0.0
`,
	},
	{
		Name:  "basic-dat",
		Error: nil,
		Config: `
input:
  file: in.dat
  fields: [time, u_max, u_x, u_y, u_z]
  format: dat
output:
  directory: ""
  table_file: "basic-dat"
  graphs:
    - name: "graph-dat0"
      axes:
        - x:
            min: 1.0
            max: 3.0
            label: "$x$-axis"
          y:
            min: 1.0
            max: 6.0
            label: "$y$-axis"
          tables:
            - x_field: time
              y_field: u_max
              legend_entry: '$u_\text{max}$'
            - x_field: time
              y_field: u_x
              legend_entry: "$u_x$"
`,
		Input: `
# time	u_max	u
1	2.0	(1.0 0.0 0.0)
2	4.0	(2.0 0.0 0.0)
3	6.0	(3.0 0.0 0.0)
`,
	},
}

func TestRun(t *testing.T) {
	for _, tt := range runTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var config Config
			config.Log, _ = test.NewNullLogger()
			config.Log.SetLevel(logrus.DebugLevel)

			// read config
			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			// write config
			conf, err := os.Create(configFile)
			assert.Nil(err, "unexpected os.Create() error")
			_, err = conf.WriteString(tt.Config)
			assert.Nil(err, "unexpected os.File.Write() error")
			defer conf.Close()

			// write input
			in, err := os.Create(config.Input.File)
			assert.Nil(err, "unexpected os.Create() error")
			_, err = in.WriteString(tt.Input)
			assert.Nil(err, "unexpected os.File.Write() error")
			defer in.Close()

			t.Cleanup(func() {
				err = os.RemoveAll(conf.Name())
				assert.Nil(err, "unexpected os.RemoveAll() error")
				err = os.RemoveAll(in.Name())
				assert.Nil(err, "unexpected os.RemoveAll() error")
			})

			err = run(&cobra.Command{}, []string{})
			assert.Equal(tt.Error, err)
		})
	}
}

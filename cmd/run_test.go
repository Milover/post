//go:build integration

package cmd

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

// run command tests
type runTest struct {
	Name   string
	Error  error
	Config string
	Input  string
}

var runTests = []runTest{
	{
		Name:  "good",
		Error: nil,
		Config: `
- input:
    type: dat
    fields: [t, patch, min, max]
    type_spec:
      file: testdata/dat/data.dat
  process:
    - type: dummy
    - type: filter
      type_spec:
        filters:
          - field: patch
            op: '=='
            value: patch_1
    - type: average-cycle
      type_spec:
        n_cycles: 5
        time_field: t
    - type: expression
      type_spec:
        expression: 'min+8'
        result: min
  output:
    - type: csv
      type_spec:
        file: 'cycle/cycle.csv'
  graph:
    type: tex
    graphs:
      - name: 'graph-cycle.tex'
        directory: 'cycle'
        table_file: 'cycle/cycle.csv'
        axes:
          - x:
              min: 0.0
              max: 1.0
              label: "$t$-axis"
            y:
              min: 8.0
              max: 12.0
              label: "$y$-axis"
            tables:
              - x_field: t
                y_field: min
                legend_entry: '$y_\text{min}$'
              - x_field: t
                y_field: max
                legend_entry: '$y_\text{max}$'

- input:
    type: time-series
    type_spec:
      directory: 'testdata/csv_series'
      file: data.csv
      format_spec:
        type: csv
  process:
    - type: dummy
    - type: average-cycle
      type_spec:
        n_cycles: 4
        time_field: 'time'
    - type: expression
      type_spec:
        expression: '100*y*y'
        result: yy
    - type: expression
      type_spec:
        expression: 'y*10'
        result: y10
  output:
    - type: ram
      type_spec:
        name: 'cycle_series'

- template:
    params:
      t:
        - tag: 0_25
          val: 0.25
        - tag: 0_75
          val: 0.75
    src: |
      - input:
          type: ram
          format_spec:
            name: 'cycle_series'
        process:
          - type: filter
            type_spec:
              filters:
                - field: time
                  op: '=='
                  value: {{ .t.val }}
        output:
          - type: csv
            type_spec:
              file: &tab_file_{{ .t.tag }} 'cycle_series/cycle_series_{{ .t.tag }}.csv'
        graph:
          type: tex
          graphs:
            - name: 'graph-cycle-series-{{ .t.tag }}.tex'
              directory: 'cycle_series'
              table_file: *tab_file_{{ .t.tag }}
              axes:
                - x:
                    min: 0.0
                    max: 1.0
                    label: '$x$-axis'
                  y:
                    min: 0
                    max: 25
                    label: '$y$-axis'
                  tables:
                    - x_field: x
                      y_field: y10
                      legend_entry: '$\left.10y\right\vert_{t={{ .t.val }}}$'
                    - x_field: x
                      y_field: yy
                      legend_entry: '$\left.100y^2\right\vert_{t={{ .t.val }}}$'
`,
	},
}

func TestRun(t *testing.T) {
	for _, tt := range runTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			// validate config
			var configs []Config
			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			err = yaml.Unmarshal(raw, &configs)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			// write config file
			conf, err := os.Create("post.yaml")
			assert.Nil(err, "unexpected os.Create() error")
			_, err = conf.WriteString(tt.Config)
			assert.Nil(err, "unexpected os.File.Write() error")
			defer conf.Close()

			t.Cleanup(func() {
				err = os.RemoveAll(conf.Name())
				assert.Nil(err, "unexpected os.RemoveAll() error")
			})
			err = run(&cobra.Command{}, []string{"post.yaml"})
			assert.Equal(tt.Error, err)
		})
	}
}

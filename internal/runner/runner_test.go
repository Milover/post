package runner

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type runTest struct {
	Name   string
	Error  error
	Config string
	Input  string
	Output [][]string
}

var runTests = []runTest{
	{
		Name:  "basic-csv",
		Error: nil,
		Config: `
input:
  file: null
  fields: [time, u_max, u_x, u_y, u_z]
  format: csv
  format_spec:
    has_header: true
    delimiter: "\t"
    comment: "#"
output:
  directory: ""
  write_file: true
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
`,
		Output: nil,
	},
	{
		Name:  "basic-dat",
		Error: nil,
		Config: `
input:
  file: null
  fields: [time, u_max, u_x, u_y, u_z]
  format: dat
output:
  directory: ""
  write_file: true
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
		Output: nil,
	},
}

func handleCloseError(c io.Closer, t *testing.T) {
	if err := c.Close(); err != nil {
		t.Fatalf("unexpected Run() error: %v", err)
	}
}

func handleError(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("unexpected Run() error: %v", err)
	}
}

func TestRun(t *testing.T) {
	for _, tt := range runTests {
		t.Run(tt.Name, func(t *testing.T) {
			var out [][]string

			// build the config
			var config Config
			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			handleError(err, t)
			handleError(yaml.Unmarshal(raw, &config), t)

			// create and set the data file
			csvFile, err := os.Create(fmt.Sprint(tt.Name, ".csv"))
			handleError(err, t)
			defer handleCloseError(csvFile, t)
			config.Output.Writer = csvFile

			// create and set the graph files
			graphFiles := make([]*os.File, len(config.Output.Graphs))
			for gID := range config.Output.Graphs {
				g := &config.Output.Graphs[gID]
				graphFiles[gID], err = os.Create(fmt.Sprintf("%v.tex", g.Name))
				handleError(err, t)
				g.Writer = graphFiles[gID]

				for aID := range g.Axes {
					a := &g.Axes[aID]
					for tID := range a.Tables {
						a.Tables[tID].TableFile = csvFile.Name()
					}
				}
			}
			// do the test
			err = Run(strings.NewReader(tt.Input), &config)

			// cleanup and check the errors
			for _, gf := range graphFiles {
				handleCloseError(gf, t)
			}
			if err != nil {
				if err != tt.Error {
					t.Fatalf("Run() error mismatch:\ngot  %v (%#v)\nwant %v (%#v)", err, err, tt.Error, tt.Error)
				}
				if out != nil {
					t.Fatalf("Run() output:\ngot  %q\nwant nil", out)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected Run() error: %v", err)
				}
				if !reflect.DeepEqual(out, tt.Output) {
					t.Fatalf("Run() output:\ngot  %q\nwant %q", out, tt.Output)
				}
			}
		})
	}
}

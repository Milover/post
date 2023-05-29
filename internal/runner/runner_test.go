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
		Name:  "basic",
		Error: nil,
		Config: `
input:
  path: null
  fields: [time, u_max, u_x, u_y, u_z]
  format: dat
output:
  graphs:
    - name: "graph0"
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
              legend_entry: "$u_\text{max}$"
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

func handleClose(c io.Closer, t *testing.T) {
	if err := c.Close(); err != nil {
		t.Fatalf("unexpected Run() error: %v", err)
	}
}

func TestRun(t *testing.T) {
	for _, tt := range runTests {
		t.Run(tt.Name, func(t *testing.T) {

			var out [][]string

			var config Config
			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			if err != nil {
				t.Fatalf("unexpected Run() error: %v", err)
			}
			if err := yaml.Unmarshal(raw, &config); err != nil {
				t.Fatalf("unexpected Run() error: %v", err)
			}

			csvFile, err := os.Create("test.csv")
			config.Output.Writer = csvFile
			if err != nil {
				t.Fatalf("unexpected Run() error: %v", err)
			}

			graphFiles := make([]*os.File, len(config.Output.Graphs))
			for gID := range config.Output.Graphs {
				g := &config.Output.Graphs[gID]
				graphFiles[gID], err = os.Create(fmt.Sprintf("%v.tex", g.Name))
				if err != nil {
					t.Fatalf("unexpected Run() error: %v", err)
				}
				g.Writer = graphFiles[gID]

				for aID := range g.Axes {
					a := &g.Axes[aID]
					for tID := range a.Tables {
						a.Tables[tID].TableFile = csvFile.Name()
					}
				}
			}
			err = Run(strings.NewReader(tt.Input), &config)

			for _, gf := range graphFiles {
				handleClose(gf, t)
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

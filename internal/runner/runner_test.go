package runner

import (
	"io"
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
output: null
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

			err = Run(strings.NewReader(tt.Input), config)

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

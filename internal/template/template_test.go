package template

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

// Test finding template nodes
type findNodesTest struct {
	Name   string
	Input  string
	Output []string
	Error  error
}

var findNodesTests = []findNodesTest{
	{
		Name:  "good-simple",
		Error: nil,
		Input: `template:
  params:
    z: [a, b, c]
  src: 'c: 1'`,
		Output: []string{
			`template:
    params:
        z: [a, b, c]
    src: 'c: 1'
`,
		},
	},
	{
		Name:  "good-mixed-mapping",
		Error: nil,
		Input: `something_before: 12
template:
  params:
    z: [a, b, c]
  src: 'c: 1'
something_after: 12`,
		Output: []string{
			`something_before: 12
template:
    params:
        z: [a, b, c]
    src: 'c: 1'
something_after: 12
`,
		},
	},
	{
		Name:  "good-mixed-placement",
		Error: nil,
		Input: `- id: "one"
  something:
    something_before: 12
    template:
      params:
        z: [a, b, c]
      src: 'c: 1'
    something_after: 12
    template:
      params:
        r: [x, y]
      src: 'bla: bla'
- id: "two"
- template:
    params:
      x: [1, 2, 3]
      y: [1, 2, 3]
    src: 'a: 1'
- template:
    params:
      x: [0, 0, 0]
    src: 'b: 2'
- id: "three"`,
		Output: []string{
			`something_before: 12
template:
    params:
        z: [a, b, c]
    src: 'c: 1'
something_after: 12
template:
    params:
        r: [x, y]
    src: 'bla: bla'
`,
			`template:
    params:
        x: [1, 2, 3]
        y: [1, 2, 3]
    src: 'a: 1'
`,
			`template:
    params:
        x: [0, 0, 0]
    src: 'b: 2'
`,
		},
	},
}

func TestFindNodes(t *testing.T) {
	for _, tt := range findNodesTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var n yaml.Node
			err := yaml.Unmarshal([]byte(tt.Input), &n)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			found := FindNodes(&n)
			for i := range tt.Output {
				out, err := yaml.Marshal(found[i])
				assert.Nil(err, "unexpected yaml.Unmarshal() error")
				assert.Equal(tt.Output[i], string(out))
			}
		})
	}
}

// Test finding and converting nodes to templates
type convertNodeTest struct {
	Name   string
	Input  string
	Output []Template
	Error  error
}

var convertNodesTests = []convertNodeTest{
	{
		Name:  "good-simple",
		Error: nil,
		Input: `template:
  params:
    z: [a, b, c]
  src: 'c: 1'`,
		Output: []Template{
			{
				Params: map[string][]string{"z": {"a", "b", "c"}},
				Src:    `c: 1`,
			},
		},
	},
	{
		Name:  "good-mixed-mapping",
		Error: nil,
		Input: `something_before: 12
template:
  params:
    z: [a, b, c]
  src: 'c: 1'
something_after: 12`,
		Output: []Template{
			{
				Params: map[string][]string{"z": {"a", "b", "c"}},
				Src:    `c: 1`,
			},
		},
	},
	{
		Name:  "good-mixed-placement",
		Error: nil,
		Input: `- id: "one"
  something:
    something_before: 12
    template:
      params:
        z: [a, b, c]
      src: 'c: 1'
    something_after: 12
    template:
      params:
        r: [x, y]
      src: 'bla: bla'
- id: "two"
- template:
    params:
      x: [1, 2, 3]
      y: [1, 2, 3]
    src: 'a: 1'
- template:
    params:
      x: [0, 0, 0]
    src: 'b: 2'
- id: "three"`,
		Output: []Template{
			{
				Params: map[string][]string{"z": {"a", "b", "c"}},
				Src:    `c: 1`,
			},
			{
				Params: map[string][]string{"r": {"x", "y"}},
				Src:    `bla: bla`,
			},
			{
				Params: map[string][]string{
					"x": {"1", "2", "3"},
					"y": {"1", "2", "3"},
				},
				Src: `a: 1`,
			},
			{
				Params: map[string][]string{"x": {"0", "0", "0"}},
				Src:    `b: 2`,
			},
		},
	},
}

func TestNodeToTemplate(t *testing.T) {
	for _, tt := range convertNodesTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var n yaml.Node
			err := yaml.Unmarshal([]byte(tt.Input), &n)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			found := FindNodes(&n)
			var tmpl []Template
			for _, f := range found {
				temp, errf := NodeToTemplates(f)
				errors.Join(err, errf)
				tmpl = append(tmpl, temp...)
			}
			assert.Equal(tt.Output, tmpl)
			assert.ErrorIs(err, tt.Error)
		})
	}
}

// Test executing templates
type executeTemplateTest struct {
	Name   string
	Error  error
	Input  Template
	Output string
}

var executeTemplateTests = []executeTemplateTest{
	{
		Name:  "good",
		Error: nil,
		Input: Template{
			Params: map[string][]string{
				"x": {"0"},
			},
			Src: `
input: 
  type: ram
  type_spec:
    name: '{{.x}}'
`,
		},
		Output: `
input: 
  type: ram
  type_spec:
    name: '0'
`,
	},
	{
		Name:  "good-realistic",
		Error: nil,
		Input: Template{
			Params: map[string][]string{
				"name":  {"a"},
				"value": {"0", "1"},
			},
			Src: `
- input: 
    type: ram
    type_spec:
      name: '{{.name}}'
  process:
  - type: expression
    type_spec:
      expression: 'phase-{{.value}}'
      result: phase
  - *phase_filter
  - type: regexp-rename
    type_spec:
      src: '(.*)'
      result: '${1}_{{.value}}'
  output:
  - type: ram
    type_spec:
      name: 'rans_{{.name}}'
`,
		},
		Output: `
- input: 
    type: ram
    type_spec:
      name: 'a'
  process:
  - type: expression
    type_spec:
      expression: 'phase-0'
      result: phase
  - *phase_filter
  - type: regexp-rename
    type_spec:
      src: '(.*)'
      result: '${1}_0'
  output:
  - type: ram
    type_spec:
      name: 'rans_a'

- input: 
    type: ram
    type_spec:
      name: 'a'
  process:
  - type: expression
    type_spec:
      expression: 'phase-1'
      result: phase
  - *phase_filter
  - type: regexp-rename
    type_spec:
      src: '(.*)'
      result: '${1}_1'
  output:
  - type: ram
    type_spec:
      name: 'rans_a'
`,
	},
}

func TestExecuteTemplate(t *testing.T) {
	for _, tt := range executeTemplateTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			out, err := tt.Input.Execute()

			assert.Equal(tt.Output, string(out))
			assert.ErrorIs(err, tt.Error)
		})
	}
}

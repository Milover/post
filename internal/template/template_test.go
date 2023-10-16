package template

import (
	"bytes"
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
		Input: `- template:
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
		Name:  "good-mixed",
		Error: nil,
		Input: `- something_before: 12
- template:
    params:
      z: [a, b, c]
    src: 'c: 1'
- something_after: 12`,
		Output: []string{
			`template:
    params:
        z: [a, b, c]
    src: 'c: 1'
`,
		},
	},
	{
		Name:  "good-multiple",
		Error: nil,
		Input: `
- template:
    params:
      z: [a, b, c]
    src: 'c: 1'
- template:
    params:
      x: [0, 1]
    src: 'b: 1'`,
		Output: []string{
			`template:
    params:
        z: [a, b, c]
    src: 'c: 1'
`,
			`template:
    params:
        x: [0, 1]
    src: 'b: 1'
`,
		},
	},
	{
		Name:  "good-multiple-mixed",
		Error: nil,
		Input: `
- one: bla
- two:
    x: y
- template:
    params:
      z: [a, b, c]
    src: 'c: 1'
- three: blabla
- template:
    params:
      x: [0, 1]
    src: 'b: 1'
- four:
    z: u`,
		Output: []string{
			`template:
    params:
        z: [a, b, c]
    src: 'c: 1'
`,
			`template:
    params:
        x: [0, 1]
    src: 'b: 1'
`,
		},
	},
	{
		Name:   "bad-empty",
		Error:  ErrBadTemplateNode,
		Input:  `- template:`,
		Output: []string{},
	},
	{
		Name:   "bad-definition-single",
		Error:  ErrBadTemplateNode,
		Input:  `- template: some_value`,
		Output: []string{},
	},
	{
		Name:  "bad-definition-additional-beggining",
		Error: ErrBadTemplateNode,
		Input: `- some_value: bla
  template:
    params:
      z: [a, b, c]
    src: 'c: 1'`,
		Output: []string{},
	},
	{
		Name:  "bad-definition-additional-end",
		Error: ErrBadTemplateNode,
		Input: `- template:
    params:
      z: [a, b, c]
    src: 'c: 1'
  some_value: bla`,
		Output: []string{},
	},
	{
		Name:  "bad-definition-additional-mixed",
		Error: ErrBadTemplateNode,
		Input: `- some_value: bla
  template:
    params:
      z: [a, b, c]
    src: 'c: 1'
  some_other_value: bla`,
		Output: []string{},
	},
}

func TestFindNodes(t *testing.T) {
	for _, tt := range findNodesTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var n yaml.Node
			err := yaml.Unmarshal([]byte(tt.Input), &n)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			found, err := findNodes(&n)
			assert.ErrorIs(err, tt.Error)
			for i := range tt.Output {
				out, err := yaml.Marshal(found[i])
				assert.Nil(err, "unexpected yaml.Unmarshal() error")
				assert.Equal(tt.Output[i], string(out))
			}
		})
	}
}

// Test finding and converting nodes to templates
type fromNodeTest struct {
	Name   string
	Input  string
	Output []Template
	Error  error
}

var fromNodeTests = []fromNodeTest{
	{
		Name:  "good-simple",
		Error: nil,
		Input: `- template:
    params:
      z: [a, b, c]
    src: 'c: 1'`,
		Output: []Template{
			{
				Params: map[string][]any{"z": {"a", "b", "c"}},
				Src:    `c: 1`,
			},
		},
	},
	{
		Name:  "good-multiple-mixed",
		Error: nil,
		Input: `- something_before: 12
- template:
    params:
      z: [a, b, c]
    src: 'c: 1'
- something_between:
    something_else:
      rand: bla
- template:
    params:
      z: [a, b, c]
      x: [0, 1, 2]
    src: 'd: 2'
- something_after: 12`,
		Output: []Template{
			{
				Params: map[string][]any{"z": {"a", "b", "c"}},
				Src:    `c: 1`,
			},
			{
				Params: map[string][]any{"z": {"a", "b", "c"}, "x": {0, 1, 2}},
				Src:    `d: 2`,
			},
		},
	},
}

func TestFromNode(t *testing.T) {
	for _, tt := range fromNodeTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var n yaml.Node
			err := yaml.Unmarshal([]byte(tt.Input), &n)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			found, err := findNodes(&n)
			assert.Nil(err, "unexpected findNodes() error")

			var tmpls []Template
			for _, f := range found {
				tmpl, errf := fromNode(f)
				errors.Join(err, errf)
				tmpls = append(tmpls, tmpl)
			}
			assert.Equal(tt.Output, tmpls)
			assert.ErrorIs(err, tt.Error)
		})
	}
}

// Test executing templates
type executeTemplateTest struct {
	Name   string
	Error  error
	Input  string
	Output string
}

var executeTemplateTests = []executeTemplateTest{
	{
		Name:  "good",
		Error: nil,
		Input: `
- template:
    params:
      x: [0]
    src: |
      input: 
        type: ram
        type_spec:
          name: '{{.x}}'
`,
		Output: `input: 
  type: ram
  type_spec:
    name: '0'
`,
	},
	{
		Name:  "good-multiple",
		Error: nil,
		Input: `
- template:
    params:
      x: ['zero', 'one', 'two']
      y: [0, 1, 2, 3, 4]
    src: |
      x: {{ .x }}
      y: {{ .y }}
`,
		Output: `x: zero
y: 0
x: one
y: 0
x: two
y: 0
x: zero
y: 1
x: one
y: 1
x: two
y: 1
x: zero
y: 2
x: one
y: 2
x: two
y: 2
x: zero
y: 3
x: one
y: 3
x: two
y: 3
x: zero
y: 4
x: one
y: 4
x: two
y: 4
`,
	},
	{
		Name:  "good-mapping-params",
		Error: nil,
		Input: `
- template:
    params:
      x:
        - name: a
          val: 0
        - name: b
          val: 1
        - name: c
          val: 2
    src: |
      input:
        type: ram
        type_spec:
          name: {{.x.name}}
          val: {{.x.val}}
`,
		Output: `input:
  type: ram
  type_spec:
    name: a
    val: 0
input:
  type: ram
  type_spec:
    name: b
    val: 1
input:
  type: ram
  type_spec:
    name: c
    val: 2
`,
	},
	{
		Name:  "good-realistic",
		Error: nil,
		Input: `
- template:
    params:
      name: [a]
      value: ['0', '1']
    src: |
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
		Output: `- input: 
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

			var n yaml.Node
			err := yaml.Unmarshal([]byte(tt.Input), &n)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			found, err := findNodes(&n)
			assert.Nil(err, "unexpected findNodes() error")
			var b bytes.Buffer
			for _, f := range found {
				tmpl, err := fromNode(f)
				assert.Nil(err, "unexpected fromNode() error")
				out, errf := tmpl.Execute()
				errors.Join(err, errf)
				_, errb := b.Write(out)
				assert.Nil(errb, "unexpected fromNode() error")
			}
			assert.Equal(tt.Output, b.String())
			assert.ErrorIs(err, tt.Error)
		})
	}
}

// End-to-end test processing and updating YAML files containing templates.
type processTest struct {
	Name   string
	Input  string
	Output string
	Error  error
}

var processTests = []processTest{
	{
		Name:  "good-simple",
		Error: nil,
		Input: `
- template:
    params:
      v: [0]
    src: |
      - value_{{.v}}: {{.v}}
`,
		Output: `- value_0: 0
`,
	},
	{
		Name:  "good-mapping",
		Error: nil,
		Input: `
- template:
    params:
      x:
        - tag: 'a'
          val: 0
        - tag: 'b'
          val: 1
    src: |
      - name: {{.x.tag}}
        value: {{.x.val}}
`,
		Output: `- name: a
  value: 0
- name: b
  value: 1
`,
	},
	{
		Name:  "good-realistic",
		Error: nil,
		Input: `
- template:
    params:
      name: [a]
      value: ['0', '1']
    src: |
      - input:
          type: ram
          type_spec:
            name: '{{.name}}'
        process:
        - type: expression
          type_spec:
            expression: 'phase-{{.value}}'
            result: phase
        - type: regexp-rename
          type_spec:
            src: '(.*)'
            result: '${1}_{{.value}}'
        output:
        - type: ram
          type_spec:
            name: 'rans_{{.name}}'
`,
		Output: `- input:
    type: ram
    type_spec:
        name: 'a'
  process:
    - type: expression
      type_spec:
        expression: 'phase-0'
        result: phase
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

func TestProcess(t *testing.T) {
	for _, tt := range processTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var n yaml.Node
			err := yaml.Unmarshal([]byte(tt.Input), &n)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			err = Process(&n)
			assert.ErrorIs(err, tt.Error)

			b, err := yaml.Marshal(&n)
			assert.Nil(err, "unexpected yaml.Marshal() error")
			assert.Equal(tt.Output, string(b))
		})
	}
}

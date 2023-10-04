package template

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"text/template"

	"gopkg.in/yaml.v3"
)

var (
	ErrBadInputNode    = errors.New("template: bad input node")
	ErrBadTemplateNode = errors.New("template: bad template node")
)

type Template struct {
	Params map[string][]string `yaml:"params"`
	Src    string              `yaml:"src"`
}

// Execute executes the template using all combinations of parameter values
// as input and returns the generated bytes.
func (t Template) Execute() ([]byte, error) {
	tmpl, err := template.New("tmpl").Option("missingkey=error").Parse(t.Src)
	if err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}
	// execute for all combinations of parameters
	var b bytes.Buffer
	par := make(map[string]string)
	combs := 1
	for i := range t.Params {
		combs *= len(t.Params[i])
	}
	for i := 0; i < combs; i++ {
		tmp := i
		for key, vals := range t.Params {
			index := tmp % len(vals)
			tmp /= len(vals)
			par[key] = vals[index]
		}
		if err := tmpl.Execute(&b, par); err != nil {
			return nil, fmt.Errorf("template: %w", err)
		}
	}
	return b.Bytes(), nil
}

// GenerateNodes generates new nodes from a Template.
func (t Template) GenerateNodes() ([]*yaml.Node, error) {
	b, err := t.Execute()
	if err != nil {
		return nil, err
	}
	// node is a document node, with exactly one sequence
	// whose content we want
	var node yaml.Node
	if err := yaml.Unmarshal(b, &node); err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}
	return node.Content[0].Content, nil
}

// isValidTemplateNode checks if n is a valid template node.
func isValidTemplateNode(n *yaml.Node) bool { // yolo
	return n.Kind == yaml.MappingNode &&
		len(n.Content) == 2 &&
		n.Content[1].Kind == yaml.MappingNode
}

// isValidInputNode checks if n can be used as a valid input node to findNodes.
func isValidInputNode(n *yaml.Node) bool {
	return n.Kind == yaml.DocumentNode &&
		len(n.Content) == 1 &&
		n.Content[0].Kind == yaml.SequenceNode
}

// findNodes performs a per-content-node breadth-first search of n,
// looking for mappings with the 'template' tag and extracts the nodes
// containing the entire 'template' mapping.
//
// If n is not a document node containing exactly 1 sequence,
// an error is returned.
//
// A valid template node must:
//   - contain the 'template' tag
//   - be a mapping node
//   - have exactly 2 child nodes
//   - have a mapping as the second child node
//
// Only valid nodes are extracted. If a node contains the 'template' tag,
// and is invalid, an error is returned.
func findNodes(n *yaml.Node) ([]*yaml.Node, error) {
	nodes := make([]*yaml.Node, 0, 5) // guesstimate
	if !isValidInputNode(n) {
		return nodes, ErrBadInputNode
	}
	seq := n.Content[0]
	for i := range seq.Content {
		child := seq.Content[i]
		for _, c := range child.Content {
			if c.Value == "template" {
				if !isValidTemplateNode(child) {
					return nodes, ErrBadTemplateNode
				}
				nodes = append(nodes, child)
			}
		}
	}
	return slices.Clip(nodes), nil
}

// nodeToTemplate correctly unmarshalls n into a Template.
// If n is not a valid template node, an error is returned.
func nodeToTemplate(n *yaml.Node) (Template, error) {
	var t Template
	if !isValidTemplateNode(n) {
		return t, ErrBadTemplateNode
	}
	if err := n.Content[1].Decode(&t); err != nil {
		return t, fmt.Errorf("template: %w", err)
	}
	return t, nil
}

func Process(n *yaml.Node) error {
	found, err := findNodes(n)
	if err != nil {
		return err
	}
	seq := &(n.Content[0].Content) // ptr to elements of the !!seq node
	var t Template
	for _, f := range found {
		// create template
		t, err = nodeToTemplate(f)
		if err != nil {
			return err
		}
		// generate new nodes
		nodes, err := t.GenerateNodes()
		if err != nil {
			return err
		}
		// replace template node with generated nodes
		index := slices.Index(*seq, f)
		if index == -1 {
			return fmt.Errorf("template: could not find index of template node")
		}
		*seq = slices.Replace(*seq, index, index+1, nodes...)
	}
	return nil
}

package graph

import (
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Graphing is the graphing program name.
	GrapherType string `yaml:"type"`
	// Graphs is list of graph specifications.
	Graphs []yaml.Node `yaml:"graphs"`
}

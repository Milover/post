package graph

import (
	"gopkg.in/yaml.v3"
)

type Config struct {
	// GrapherType is the graphing program name.
	GrapherType string `yaml:"type"`
	// Graphs is list of graph specifications.
	Graphs []yaml.Node `yaml:"graphs"`
}

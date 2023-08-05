package graph

import (
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Graphing is the graphing program name.
	GrapherType string `yaml:"type"`
	// Graphs is list of graph YAML specifications.
	Graphs []yaml.Node `yaml:"graphs"`

	Factory GrapherFactory `yaml:"-"`
}

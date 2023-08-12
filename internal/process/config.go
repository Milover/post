package process

import (
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Type is the name of the processor.
	Type string `yaml:"type"`
	// TypeSpec contains the specification for the processor.
	TypeSpec yaml.Node `yaml:"type_spec"`
}

package input

import "gopkg.in/yaml.v3"

type Config struct {
	File       string    `yaml:"file,omitempty"`
	Fields     []string  `yaml:"fields,omitempty"`
	Format     string    `yaml:"format,omitempty"`
	FormatSpec yaml.Node `yaml:"format_spec,omitempty"`
}

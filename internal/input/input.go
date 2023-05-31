package input

import "gopkg.in/yaml.v3"

// Config is holds data needed for reading and creating a dataframe.DataFrame
// from formatted input.
type Config struct {
	File       string    `yaml:"file,omitempty"`
	Fields     []string  `yaml:"fields,omitempty"`
	Format     string    `yaml:"format,omitempty"`
	FormatSpec yaml.Node `yaml:"format_spec,omitempty"`
}

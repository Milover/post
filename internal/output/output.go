package output

import (
	"io"

	_ "gopkg.in/yaml.v3"
)

type Config struct {
	Directory string `yaml:"directory,omitempty"`
	WriteFile bool   `yaml:"write_file,omitempty"`
	// TODO: This should be a *yaml.Node because we might not be using TeX,
	// and even if we are, the input needs to be validated.
	Graphs []TeXGraph `yaml:"graphs,omitempty"`

	// FIXME: This is confusing, and probably shouldn't be here.
	Writer io.Writer `yaml:"-"`
}

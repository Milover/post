package output

import (
	"io"
)

type Config struct {
	Directory string `yaml:"directory"`
	WriteFile bool   `yaml:"write_file"`
	// TODO: This should be a *yaml.Node because we might not be using TeX,
	// and even if we are, the input needs to be validated.
	Graphs []TeXGraph `yaml:"graphs"`

	// FIXME: This is confusing, and probably shouldn't be here.
	Writer io.Writer `yaml:"-"`
}

package input

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// Config is holds data needed for reading and creating a dataframe.DataFrame
// from formatted input.
type Config struct {
	File       string    `yaml:"file"`
	Fields     []string  `yaml:"fields"`
	Format     string    `yaml:"format"`
	FormatSpec yaml.Node `yaml:"format_spec"`

	Log *logrus.Logger `yaml:"-"`
}

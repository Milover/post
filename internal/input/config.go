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
	SeriesSpec yaml.Node `yaml:"series_spec"`

	Log *logrus.Logger `yaml:"-"`
}

// IsSeries returns true if the Config is to be used with series input.
// It returns false otherwise.
func (c *Config) IsSeries() bool {
	return !c.SeriesSpec.IsZero()
}

package output

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Directory is an output directory for all data. If it is an empty string,
	// the current working directory is used. The path is created recursively
	// if it does not exist.
	Directory string `yaml:"directory"`
	// TableFile is the file in which the CSV-fromatted dataframe.DataFrame
	// will be written.
	TableFile string `yaml:"table_file"`
	// Graphing is the graphing program name.
	Grapher string `yaml:"grapher"`
	// Graphs is list of graph YAML specifications.
	Graphs []yaml.Node `yaml:"graphs"`

	Log *logrus.Logger `yaml:"-"`
}

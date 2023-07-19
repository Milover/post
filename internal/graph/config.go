package graph

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Graphing is the graphing program name.
	GrapherType string `yaml:"type"`
	// Graphs is list of graph YAML specifications.
	Graphs []yaml.Node `yaml:"graphs"`

	Factory GrapherFactory `yaml:"-"`
	Log     *logrus.Logger `yaml:"-"`
}

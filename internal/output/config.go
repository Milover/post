package output

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Type     string    `yaml:"type"`
	TypeSpec yaml.Node `yaml:"type_spec"`

	Log *logrus.Logger `yaml:"-"`
}

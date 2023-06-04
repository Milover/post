package process

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Type     string    `yaml:"type,omitempty"`
	TypeSpec yaml.Node `yaml:"type_spec,omitempty"`

	Log *logrus.Logger `yaml:"-"`
}

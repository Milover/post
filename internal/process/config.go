package process

import (
	"gopkg.in/yaml.v3"
)

type Config struct {
	Type     string    `yaml:"type"`
	TypeSpec yaml.Node `yaml:"type_spec"`
}

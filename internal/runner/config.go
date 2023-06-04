package runner

import (
	"github.com/Milover/foam-postprocess/internal/input"
	"github.com/Milover/foam-postprocess/internal/output"
	"github.com/Milover/foam-postprocess/internal/process"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Input   input.Config     `yaml:"input,omitempty"`
	Process []process.Config `yaml:"process,omitempty"`
	Output  output.Config    `yaml:"output,omitempty"`

	Log *logrus.Logger `yaml:"-"`
}

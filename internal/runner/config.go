package runner

import (
	"github.com/Milover/foam-postprocess/internal/input"
	"github.com/Milover/foam-postprocess/internal/output"
	"github.com/Milover/foam-postprocess/internal/process"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Input   input.Config     `yaml:"input"`
	Process []process.Config `yaml:"process"`
	Output  output.Config    `yaml:"output"`

	Log *logrus.Logger `yaml:"-"`
}

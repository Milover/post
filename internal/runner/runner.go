package runner

import (
	"errors"
	"fmt"
	"io"

	"github.com/Milover/foam-postprocess/internal/input"
	"github.com/Milover/foam-postprocess/internal/output"
	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Input  input.Config  `yaml:"input,omitempty"`
	Output output.Config `yaml:"output,omitempty"`

	Log *logrus.Logger `yaml:"-"`
}

// TODO: Should just take a raw config (io.Reader or file name) and do
// everything else.
func Run(in io.Reader, config *Config) error {
	df, err := input.CreateDataFrame(in, &config.Input)
	if err != nil {
		fmt.Errorf("error creating data frame: %w", err)
	}
	// TODO: Process data

	// FIXME: LaTeX has an upper size limit for CSV files that it can handle
	// so the output should be decimated down to this size if it's too large.
	err = df.WriteCSV(config.Output.Writer, dataframe.WriteHeader(true))
	if err != nil {
		return err
	}
	for i := range config.Output.Graphs {
		if e := output.WriteTeXGraph(&config.Output.Graphs[i]); e != nil {
			err = errors.Join(err, e)
			continue
		}
		// FIXME: This doesn't make sense if the graph isn't written to a file.
		// FIXME: Also this should probably be a separate step, since we may
		// only want to recompile without rewriting the graphs.
		if e := output.CompileTeXGraph(&config.Output.Graphs[i]); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

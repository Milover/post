package runner

import (
	"errors"
	"io"

	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Input  InputConfig  `yaml:"input,omitempty"`
	Output OutputConfig `yaml:"output,omitempty"`
}

type InputConfig struct {
	File       string    `yaml:"file,omitempty"`
	Fields     []string  `yaml:"fields,omitempty"`
	Format     string    `yaml:"format,omitempty"`
	FormatSpec yaml.Node `yaml:"format_spec,omitempty"`
}

type OutputConfig struct {
	Directory string `yaml:"directory,omitempty"`
	WriteFile bool   `yaml:"write_file,omitempty"`
	// TODO: This should be a *yaml.Node because we might not be using TeX,
	// and even if we are, the input needs to be validated.
	Graphs []TeXGraph `yaml:"graphs,omitempty"`

	// FIXME: This is confusing, and probably shouldn't be here.
	Writer io.Writer `yaml:"-"`
}

// TODO: Should just take a raw config (io.Reader or file name) and do
// everything else.
func Run(in io.Reader, config *Config) error {
	df := CreateDataFrame(in, &config.Input)
	// TODO: Process data

	// FIXME: LaTeX has an upper size limit for CSV files that it can handle
	// so the output should be decimated down to this size if it's too large.
	err := df.WriteCSV(config.Output.Writer, dataframe.WriteHeader(true))
	if err != nil {
		return err
	}
	for i := range config.Output.Graphs {
		if e := WriteTeXGraph(&config.Output.Graphs[i]); e != nil {
			err = errors.Join(err, e)
			continue
		}
		// FIXME: This doesn't make sense if the graph isn't written to a file.
		// FIXME: Also this should probably be a separate step, since we may
		// only want to recompile without rewriting the graphs.
		if e := CompileTeXGraph(&config.Output.Graphs[i]); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

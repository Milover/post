package runner

import (
	"errors"
	"fmt"
	"io"
	"os/exec"

	"github.com/go-gota/gota/dataframe"
	_ "gopkg.in/yaml.v3"
)

type Config struct {
	Input  InputConfig  `yaml:"input,omitempty"`
	Output OutputConfig `yaml:"output,omitempty"`
}

type InputConfig struct {
	Path   string   `yaml:"path,omitempty"`
	Fields []string `yaml:"fields,omitempty"`
	Format string   `yaml:"format,omitempty"`
}

type OutputConfig struct {
	Graphs []TeXGraph `yaml:"graphs,omitempty"`

	Writer io.Writer `yaml:"-"`
}

func Run(in io.Reader, config *Config) error {
	df := CreateDataFrame(in, &config.Input)
	// Process data

	// Output csv
	// FIXME: LaTeX has an upper size limit for CSV files that it can handle
	// so the output should be decimated down to this size if it's too large.
	err := df.WriteCSV(config.Output.Writer, dataframe.WriteHeader(true))
	if err != nil {
		return err
	}

	// Output LaTeX graphs
	for i := range config.Output.Graphs {
		g := &config.Output.Graphs[i]
		if e := CreateTeXGraph(g); e != nil {
			err = errors.Join(err, e)
			continue
		}
		// FIXME: This doesn't make sense if the graph isn't written to a file.
		// FIXME: Also this should probably be a separate step, since we may
		// want to only re-run the compilation without re-generating the graphs.
		cmd := exec.Command("pdflatex",
			"-halt-on-error",
			"-interaction=nonstopmode",
			fmt.Sprint(g.Name, ".tex"),
		)
		if e := cmd.Run(); e != nil {
			err = errors.Join(err, e)
		}
	}
	return err
}

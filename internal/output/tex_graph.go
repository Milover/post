package output

import (
	"embed"
	"io"
	"os/exec"
	"path/filepath"
	"text/template"
)

//go:embed tmpl
var templates embed.FS

type TeXGraph struct {
	Name      string    `yaml:"name"`
	Axes      []TexAxis `yaml:"axes"`
	TableFile string    `yaml:"table-file"`
}

type TexAxis struct {
	X      AxisLine   `yaml:"x"`
	Y      AxisLine   `yaml:"y"`
	Tables []TeXTable `yaml:"tables"`
}

type AxisLine struct {
	Min   float64 `yaml:"min"`
	Max   float64 `yaml:"max"`
	Label string  `yaml:"label"`
}

type TeXTable struct {
	XField      string `yaml:"x_field"`
	YField      string `yaml:"y_field"`
	LegendEntry string `yaml:"legend_entry"`
	TableFile   string `yaml:"table_file"`
}

func GenerateTeXGraph(file string) error {
	return exec.Command("pdflatex",
		"-halt-on-error",
		"-interaction=nonstopmode",
		"-output-directory="+filepath.Dir(file),
		file,
	).Run()
}

func WriteTeXGraph(w io.Writer, g *TeXGraph) error {
	g.propagateTableFile()
	return template.Must(template.
		New("master.tmpl").
		Delims("__{", "}__").
		ParseFS(templates, "tmpl/*.tmpl")).
		Execute(w, g)
}

// propagateTableFile is a function which propagates the graphs 'TableFile'
// to each TeXTable present in the graph.
// The TeXTable.TableFile is set only if it is undefined, i.e., if it was not
// specified in the run file.
func (g *TeXGraph) propagateTableFile() {
	for aID := range g.Axes {
		a := &g.Axes[aID]
		for tID := range a.Tables {
			if len(a.Tables[tID].TableFile) == 0 {
				a.Tables[tID].TableFile = g.TableFile
			}
		}
	}
}

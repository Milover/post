package output

import (
	"embed"
	"fmt"
	"io"
	"os/exec"
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
	TableFile   string `yaml:"-"`
}

func GenerateTeXGraph(file string) error {
	return exec.Command("pdflatex",
		"-halt-on-error",
		"-interaction=nonstopmode",
		fmt.Sprint(file),
	).Run()
}

// TODO: Should probably also take an io.Writer.
func WriteTeXGraph(w io.Writer, g *TeXGraph) error {
	g.propagateCSV()
	return template.Must(template.
		New("master.tmpl").
		Delims("__{", "}__").
		ParseFS(templates, "tmpl/*.tmpl")).
		Execute(w, g)
}

func (g *TeXGraph) propagateCSV() {
	for aID := range g.Axes {
		a := &g.Axes[aID]
		for tID := range a.Tables {
			a.Tables[tID].TableFile = g.TableFile
		}
	}
}

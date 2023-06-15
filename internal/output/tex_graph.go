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
	Name string    `yaml:"name"`
	Axes []TexAxis `yaml:"axes"`

	// TODO: This probably shouldn't be here.
	Writer io.Writer `yaml:"-"`
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

func CompileTeXGraph(g *TeXGraph) error {
	return exec.Command("pdflatex",
		"-halt-on-error",
		"-interaction=nonstopmode",
		fmt.Sprint(g.Name, ".tex"),
	).Run()
}

// TODO: Should probably also take an io.Writer.
func WriteTeXGraph(g *TeXGraph) error {
	return template.Must(template.
		New("master.tmpl").
		Delims("__{", "}__").
		ParseFS(templates, "tmpl/*.tmpl")).
		Execute(g.Writer, g)
}

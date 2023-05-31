package output

import (
	"embed"
	"fmt"
	"io"
	"os/exec"
	"text/template"

	_ "gopkg.in/yaml.v3"
)

//go:embed tmpl
var templates embed.FS

type TeXGraph struct {
	Name string    `yaml:"name,omitempty"`
	Axes []TexAxis `yaml:"axes,omitempty"`

	// TODO: This probably shouldn't be here.
	Writer io.Writer `yaml:"-"`
}

type TexAxis struct {
	X      AxisLine   `yaml:"x,omitempty"`
	Y      AxisLine   `yaml:"y,omitempty"`
	Tables []TeXTable `yaml:"tables,omitempty"`
}

type AxisLine struct {
	Min   float64 `yaml:"min,omitempty"`
	Max   float64 `yaml:"max,omitempty"`
	Label string  `yaml:"label,omitempty"`
}

type TeXTable struct {
	XField      string `yaml:"x_field,omitempty"`
	YField      string `yaml:"y_field,omitempty"`
	LegendEntry string `yaml:"legend_entry,omitempty"`
	TableFile   string `yaml:"-,omitempty"`
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

package runner

import (
	"embed"
	"errors"
	"fmt"
	"io"
	"text/template"

	_ "gopkg.in/yaml.v3"
)

//go:embed tmpl
var templates embed.FS

var (
	ErrCreateTeXGraph = errors.New("could not create TeX graph")
)

type TeXGraph struct {
	Name string    `yaml:"name,omitempty"`
	Axes []TexAxis `yaml:"axes,omitempty"`

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

func CreateTeXGraph(g *TeXGraph) error {
	tmpl := template.Must(template.
		New("master.tmpl").
		Delims("__{", "}__").
		ParseFS(templates, "tmpl/*.tmpl"),
	)
	if err := tmpl.Execute(g.Writer, g); err != nil {
		return fmt.Errorf("%v: %q: %v", ErrCreateTeXGraph, g.Name, err)
	}
	return nil
}

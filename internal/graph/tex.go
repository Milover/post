package graph

import (
	"embed"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

//go:embed tmpl
var dfltTeXTemplates embed.FS

var (
	ErrTeXGraphSpec      = errors.New("output: TeX: graph spec == nil")
	ErrTeXGraphName      = errors.New("output: TeX: graph name not specified")
	ErrTeXGraphTableFile = errors.New("output: TeX: graph table file not specified")
)

const (
	DfltTexTemplateDir  string = "tmpl/*.tmpl"
	DfltTexTemplateMain string = "master.tmpl"
)

var (
	DfltTeXTemplateDelims = []string{"__{", "}__"}
)

type TeXGrapher struct {
	Name string `yaml:"name"`
	// Directory is an output directory for all files. If it is an empty string,
	// the current working directory is used. The path is created recursively
	// if it does not exist.
	Directory      string    `yaml:"directory"`
	Axes           []TexAxis `yaml:"axes"`
	TableFile      string    `yaml:"table_file"`
	TemplateDir    string    `yaml:"template_file"`
	TemplateMain   string    `yaml:"template_main"`
	TemplateDelims []string  `yaml:"template_delims"`

	Templates fs.FS          `yaml:"-"`
	Spec      *yaml.Node     `yaml:"-"`
	Log       *logrus.Logger `yaml:"-"`
}

func newTeXGrapher(spec *yaml.Node, config *Config) (Grapher, error) {
	g := &TeXGrapher{
		TemplateDir:    DfltTexTemplateDir,
		TemplateMain:   DfltTexTemplateMain,
		TemplateDelims: DfltTeXTemplateDelims,
		Templates:      dfltTeXTemplates,
	}
	if err := spec.Decode(g); err != nil {
		return nil, err
	}
	if len(g.Name) == 0 {
		return nil, ErrTeXGraphName
	}
	if len(g.Directory) != 0 {
		if err := os.MkdirAll(filepath.Clean(g.Directory), 0755); err != nil {
			return nil, err
		}
	}
	g.Log = config.Log
	return g, nil
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

func (g *TeXGrapher) Write() error {
	path := filepath.Join(g.Directory, g.Name)
	g.Log.WithFields(logrus.Fields{
		"file": path,
	}).Trace("writing TeX graph file")
	w, err := os.Create(path)
	if err != nil {
		return err
	}
	defer w.Close()

	if err := g.propagateTableFile(); err != nil {
		return err
	}
	if g.TemplateDir != DfltTexTemplateDir {
		if _, err := os.Stat(g.TemplateDir); err != nil {
			return err
		}
		g.Templates = os.DirFS(g.TemplateDir)
	}
	return template.Must(template.
		New(g.TemplateMain).
		Delims(g.TemplateDelims[0], g.TemplateDelims[1]).
		ParseFS(g.Templates, g.TemplateDir)).
		Execute(w, g)
}

func (g *TeXGrapher) Generate() error {
	path := filepath.Join(g.Directory, g.Name)
	if _, err := os.Stat(path); err != nil {
		return err
	}
	g.Log.WithFields(logrus.Fields{
		"file": path,
	}).Trace("generating TeX graph")
	return exec.Command("pdflatex",
		"-halt-on-error",
		"-interaction=nonstopmode",
		"-output-directory="+g.Directory,
		path,
	).Run()
}

// propagateTableFile is a function which propagates the graphs 'TableFile'
// to each TeXTable present in the graph.
// The TeXTable.TableFile is set only if it is undefined, i.e., if it was not
// specified in the run file.
func (g *TeXGrapher) propagateTableFile() error {
	for aID := range g.Axes {
		a := &g.Axes[aID]
		for tID := range a.Tables {
			if len(a.Tables[tID].TableFile) == 0 {
				a.Tables[tID].TableFile = g.TableFile
			}
			if len(a.Tables[tID].TableFile) == 0 {
				return ErrTeXGraphTableFile
			}
		}
	}
	return nil
}

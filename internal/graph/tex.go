package graph

import (
	"embed"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

	"github.com/Milover/post/internal/common"
	"gopkg.in/yaml.v3"
)

//go:embed tmpl
var DfltTeXTemplates embed.FS

// Default TeXGraph parameter values.
const (
	DfltTexTemplateExt  string = ".tmpl"
	DfltTexTemplateDir  string = "tmpl"
	DfltTexTemplateMain string = "master.tmpl"
	DfltTexCommand      string = "pdflatex"
)

var (
	DfltTeXTemplateDelims = []string{"__{", "}__"}
)

type TeXGrapher struct {
	// Name is the graph handle.
	Name string `yaml:"name"`
	// Directory is an output directory for all files. If it is an empty string,
	// the current working directory is used. The path is created recursively
	// if it does not exist.
	Directory string    `yaml:"directory"`
	Axes      []TexAxis `yaml:"axes"`
	// TableFile is the path to the data file, usually a CSV file,
	// used for creating the graph.
	// It is propagated to all child TeXTables, if they do not
	// have a TableFile defined locally.
	TableFile string `yaml:"table_file"`
	// TemplateDir is the path to the directory which contains
	// the graph file templates.
	TemplateDir string `yaml:"template_directory"`
	// TemplateMain is the file name of the main (root) template.
	TemplateMain string `yaml:"template_main"`
	// TemplateDelims are the Go template control structure delimiters.
	TemplateDelims []string `yaml:"template_delims"`
	// TeXCommand is the name of the binary which is used to generate
	// the graph from graph files.
	TeXCommand string `yaml:"tex_command"`

	TemplatePattern string     `yaml:"-"`
	Templates       fs.FS      `yaml:"-"`
	Spec            *yaml.Node `yaml:"-"`
}

func newTeXGrapher(spec *yaml.Node, config *Config) (Grapher, error) {
	g := &TeXGrapher{
		TemplateDir:     DfltTexTemplateDir,
		TemplateMain:    DfltTexTemplateMain,
		TemplateDelims:  DfltTeXTemplateDelims,
		TemplatePattern: filepath.Join(DfltTexTemplateDir, "*"+DfltTexTemplateExt),
		Templates:       DfltTeXTemplates,
		TeXCommand:      DfltTexCommand,
	}
	if err := spec.Decode(g); err != nil {
		return nil, fmt.Errorf("tex: %w", err)
	}
	if g.Name == "" {
		return nil, fmt.Errorf("tex: %w: %q", common.ErrUnsetField, "name")
	}
	if g.Directory != "" {
		if err := os.MkdirAll(filepath.Clean(g.Directory), 0755); err != nil {
			return nil, fmt.Errorf("tex: %w", err)
		}
	}
	return g, nil
}

type TexAxis struct {
	X           AxisLine   `yaml:"x"`
	Y           AxisLine   `yaml:"y"`
	Tables      []TeXTable `yaml:"tables"`
	RawOptions  string     `yaml:"raw_options"`
	Width       string     `yaml:"width"`
	Height      string     `yaml:"height"`
	LegendStyle string     `yaml:"legend_style"`
}

type AxisLine struct {
	Min   float64 `yaml:"min"`
	Max   float64 `yaml:"max"`
	Label string  `yaml:"label"`
}

type TeXTable struct {
	XField      string `yaml:"x_field"`
	YField      string `yaml:"y_field"`
	ColSep      string `yaml:"col_sep"`
	LegendEntry string `yaml:"legend_entry"`
	TableFile   string `yaml:"table_file"`
}

// Write writes the graph files from templates.
func (g *TeXGrapher) Write() error {
	path := filepath.Join(g.Directory, g.Name)
	if common.Verbose {
		log.Printf("tex: writing graph file: %v", path)
	}
	w, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("tex: %w", err)
	}
	defer w.Close()

	if err := g.propagateTableFile(); err != nil {
		return fmt.Errorf("tex: %w", err)
	}
	if g.TemplateDir != DfltTexTemplateDir {
		if _, err := os.Stat(g.TemplateDir); err != nil {
			return fmt.Errorf("tex: %w", err)
		}
		g.Templates = os.DirFS(g.TemplateDir)
		g.TemplatePattern = "*" + DfltTexTemplateExt
	}
	err = template.Must(template.
		New(g.TemplateMain).
		Delims(g.TemplateDelims[0], g.TemplateDelims[1]).
		ParseFS(g.Templates, g.TemplatePattern)).
		Execute(w, g)
	if err != nil {
		return fmt.Errorf("tex: %w", err)
	}
	return nil
}

// Generate generates graphs from graph files.
func (g *TeXGrapher) Generate() error {
	path := filepath.Join(g.Directory, g.Name)
	if common.Verbose {
		log.Printf("tex: generating graph: %v", path)
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("tex: %w", err)
	}
	err := exec.Command(g.TeXCommand,
		"-halt-on-error",
		"-interaction=nonstopmode",
		"-output-directory="+g.Directory,
		path,
	).Run()
	if err != nil {
		return fmt.Errorf("tex: %w", err)
	}
	return nil
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
			if a.Tables[tID].TableFile == "" {
				return fmt.Errorf("%w: %q", common.ErrUnsetField, "table_file")
			}
		}
	}
	return nil
}

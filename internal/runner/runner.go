package runner

import (
	"embed"
	"fmt"
	"io"
	"log"
	"strings"
	"text/template"

	"github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

//go:embed tmpl
var templates embed.FS

const (
	CSV string = "csv"
	DAT string = "dat"
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

func CreateDataFrame(in io.Reader, config *InputConfig) (*dataframe.DataFrame, error) {
	var df dataframe.DataFrame
	switch strings.ToLower(config.Format) {
	case CSV:
		df = dataframe.ReadCSV(in, dataframe.HasHeader(true))
	case DAT:
		r := dat.NewReader(in)
		records, err := r.ReadAll()
		if err != nil {
			return &df, err
		}
		df = dataframe.LoadRecords(records, dataframe.HasHeader(false))
	}
	// WARNING: not sure what this actually catches?
	if df.Error() != nil {
		return &df, df.Error()
	}
	if len(config.Fields) > 0 {
		err := df.SetNames(config.Fields...)
		if err != nil {
			return &df, err
		}
	}
	return &df, nil
}

func Run(in io.Reader, config *Config) error {
	df, err := CreateDataFrame(in, &config.Input)
	if err != nil {
		return err
	}
	// Process data

	// Output csv
	err = df.WriteCSV(config.Output.Writer, dataframe.WriteHeader(true))
	if err != nil {
		return err
	}

	// Output LaTeX graphs
	tmpl := template.Must(template.New("master.tmpl").Delims("__{", "}__").ParseFS(templates, "tmpl/*.tmpl"))
	for i := range config.Output.Graphs {
		g := &config.Output.Graphs[i]
		if err := tmpl.Execute(g.Writer, g); err != nil {
			log.Fatalf("error: %v", err)
		}
	}

	// Debug
	confd, err := yaml.Marshal(config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	//fmt.Printf("--- config:\n%v\n", config)
	fmt.Printf("--- config:\n%v\n", string(confd))
	fmt.Println(df)

	return nil
}

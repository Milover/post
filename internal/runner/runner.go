package runner

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

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
}

func CreateDataFrame(in io.Reader, config InputConfig) (*dataframe.DataFrame, error) {
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

func Run(in io.Reader, config Config) error {
	df, err := CreateDataFrame(in, config.Input)
	if err != nil {
		return err
	}

	// Process data

	// Output LaTeX graphs

	// Debug
	confd, err := yaml.Marshal(&config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- config:\n%v\n", string(confd))
	fmt.Println(df)

	return nil
}

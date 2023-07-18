package output

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/go-gota/gota/dataframe"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	ErrOutputCsvFile = errors.New("output file not set")
)

type CSVOutputer struct {
	// Directory is an output directory for all data. If it is an empty string,
	// the current working directory is used. The path is created recursively
	// if it does not exist.
	Directory string `yaml:"directory"`
	// File is the file in which the CSV-fromatted dataframe.DataFrame
	// will be written.
	File string `yaml:"file"`

	Log *logrus.Logger `yaml:"-"`
}

func NewCSVOutputer(n *yaml.Node, config *Config) (Outputer, error) {
	o := CSVOutputer{}
	o.Log = config.Log
	if err := n.Decode(&o); err != nil {
		return nil, err
	}
	if len(o.File) == 0 {
		return nil, ErrOutputCsvFile
	}
	return &o, nil
}

// WriteCSV writes df to a CSV file, using options from the config.
// FIXME: LaTeX has an upper size limit for CSV files that it can handle
// so the output should be decimated down to this size if it's too large.
func (o *CSVOutputer) Output(df *dataframe.DataFrame) error {
	csv, err := OutDir(o.Directory)
	if err != nil {
		return err
	}
	csv = filepath.Join(csv, o.File)
	o.Log.WithFields(logrus.Fields{
		"file": csv,
	}).Debug("writing csv")
	w, err := os.Create(csv)
	if err != nil {
		return err
	}
	if err := df.WriteCSV(w, dataframe.WriteHeader(true)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

package rw

import (
	"io"
	"os"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"gopkg.in/yaml.v3"
)

const (
	CSVExt       string = ".csv"
	CSVDelimiter rune   = ','
	CSVComment   rune   = '#'
)

type csv struct {
	fileReader `yaml:",inline"`

	Header    bool   `yaml:"header"`
	Delimiter string `yaml:"delimiter"`
	Comment   string `yaml:"comment"`
}

func defaultCsv() *csv {
	return &csv{
		Header:    true,
		Delimiter: string(CSVDelimiter),
		Comment:   string(CSVComment),
	}
}

func NewCsv(n *yaml.Node) (*csv, error) {
	rw := defaultCsv()
	if err := n.Decode(rw); err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *csv) Read() (*dataframe.DataFrame, error) {
	f, err := rw.openFile()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return rw.ReadOutOf(f)
}

func (rw *csv) ReadOutOf(in io.Reader) (*dataframe.DataFrame, error) {
	df := dataframe.ReadCSV(
		in,
		dataframe.HasHeader(rw.Header),
		dataframe.WithDelimiter(DecodeRuneOrDefault(rw.Delimiter, CSVDelimiter)),
		dataframe.WithComments(DecodeRuneOrDefault(rw.Comment, CSVComment)),
		dataframe.DefaultType(series.Float),
	)
	if df.Error() != nil {
		return nil, df.Error()
	}
	return &df, nil
}

// WriteCSV writes df to a CSV file, using options from the config.
// FIXME: LaTeX has an upper size limit for CSV files that it can handle
// so the output should be decimated down to this size if it's too large.
func (rw *csv) Write(df *dataframe.DataFrame) error {
	if err := OutDir(rw.File); err != nil {
		return err
	}
	// LaTeX needs a 'proper' extension to determine the format
	path := rw.enforceExt(CSVExt)
	//	o.Log.WithFields(logrus.Fields{
	//		"file": csv,
	//	}).Debug("writing csv")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	// TODO: apply options from the spec
	if err := df.WriteCSV(f, dataframe.WriteHeader(rw.Header)); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

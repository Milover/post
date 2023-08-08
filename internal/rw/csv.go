package rw

import (
	"fmt"
	"io"
	"os"

	"github.com/Milover/post/internal/common"
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
	// File is the name of the file from which data is read or written to.
	File string `yaml:"file"`
	// EnforceExtension determines whether a file name extension will be
	// enforced on the output file name.
	EnforceExtension bool `yaml:"enforce_extension"`

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
	fn := func(name string) (io.ReadCloser, error) {
		return os.Open(name)
	}
	return rw.ReadFromFn(fn)
}

func (rw *csv) ReadFromFn(fn ReaderFunc) (*dataframe.DataFrame, error) {
	var rc io.ReadCloser
	var err error
	if rw.File == "" { // yolo
		rc, err = fn("")
	} else {
		rc, err = fn(rw.File)
	}
	if err != nil {
		return nil, fmt.Errorf("csv: %w", err)
	}
	defer rc.Close()
	return rw.read(rc)
}

func (rw *csv) read(in io.Reader) (*dataframe.DataFrame, error) {
	df := dataframe.ReadCSV(
		in,
		dataframe.HasHeader(rw.Header),
		dataframe.WithDelimiter(DecodeRuneOrDefault(rw.Delimiter, CSVDelimiter)),
		dataframe.WithComments(DecodeRuneOrDefault(rw.Comment, CSVComment)),
		dataframe.DefaultType(series.Float),
	)
	if df.Error() != nil {
		return nil, fmt.Errorf("csv: %w", df.Error())
	}
	return &df, nil
}

// WriteCSV writes df to a CSV file, using options from the config.
// FIXME: LaTeX has an upper size limit for CSV files that it can handle
// so the output should be decimated down to this size if it's too large.
func (rw *csv) Write(df *dataframe.DataFrame) error {
	if rw.File == "" {
		return fmt.Errorf("csv: %w: %v", common.ErrUnsetField, "file")
	}
	if err := OutDir(rw.File); err != nil {
		return err
	}
	// LaTeX needs a 'proper' extension to determine the format
	path := rw.File
	if rw.EnforceExtension {
		path = SetExt(path, CSVExt)
	}
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

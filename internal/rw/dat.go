package rw

import (
	"fmt"
	"io"
	"os"

	datenc "github.com/Milover/post/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"gopkg.in/yaml.v3"
)

const (
	// XXX: but can also have no extension?
	DATExt string = ".dat"
)

type dat struct {
	// File is the name of the file from which data is read or written to.
	File string `yaml:"file"`
}

func defaultDat() *dat {
	return &dat{}
}

func NewDat(n *yaml.Node) (*dat, error) {
	rw := defaultDat()
	if err := n.Decode(rw); err != nil {
		return nil, fmt.Errorf("dat: %w", err)
	}
	return rw, nil
}

func (rw *dat) Read() (*dataframe.DataFrame, error) {
	fn := func(name string) (io.ReadCloser, error) {
		return os.Open(name)
	}
	return rw.ReadFromFn(fn)
}

func (rw *dat) ReadFromFn(fn ReaderFunc) (*dataframe.DataFrame, error) {
	var rc io.ReadCloser
	var err error
	if rw.File == "" { // yolo
		rc, err = fn("")
	} else {
		rc, err = fn(rw.File)
	}
	if err != nil {
		return nil, fmt.Errorf("dat: %w", err)
	}
	defer rc.Close()
	return rw.read(rc)
}

func (rw *dat) read(in io.Reader) (*dataframe.DataFrame, error) {
	r := datenc.NewReader(in)
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("dat: %w", err)
	}
	df := dataframe.LoadRecords(
		records,
		dataframe.HasHeader(false),
		dataframe.DefaultType(series.Float),
	)
	if df.Error() != nil {
		return nil, fmt.Errorf("dat: %w", df.Error())
	}
	return &df, nil
}

package rw

import (
	"io"

	datenc "github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"gopkg.in/yaml.v3"
)

const (
	// XXX: but can also have no extension?
	DATExt string = ".dat"
)

type dat struct {
	fileReader `yaml:",inline"`
}

func defaultDat() *dat {
	return &dat{}
}

func NewDat(n *yaml.Node) (*dat, error) {
	rw := defaultDat()
	if err := n.Decode(rw); err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *dat) Read() (*dataframe.DataFrame, error) {
	f, err := rw.openFile()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return rw.ReadOutOf(f)
}

func (rw *dat) ReadOutOf(in io.Reader) (*dataframe.DataFrame, error) {
	r := datenc.NewReader(in)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	df := dataframe.LoadRecords(
		records,
		dataframe.HasHeader(false),
		dataframe.DefaultType(series.Float),
	)
	if df.Error() != nil {
		return nil, df.Error()
	}
	return &df, nil
}

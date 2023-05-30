package runner

import (
	"io"
	"log"
	"strings"

	"github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
)

const (
	CSV string = "csv"
	DAT string = "dat"
)

func fromCSV(in io.Reader) *dataframe.DataFrame {
	df := dataframe.ReadCSV(in, dataframe.HasHeader(true))
	return &df
}

func fromDAT(in io.Reader) *dataframe.DataFrame {
	r := dat.NewReader(in)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	df := dataframe.LoadRecords(records, dataframe.HasHeader(false))
	return &df
}

func CreateDataFrame(in io.Reader, config *InputConfig) *dataframe.DataFrame {
	var df *dataframe.DataFrame
	switch strings.ToLower(config.Format) {
	case CSV:
		df = fromCSV(in)
	case DAT:
		df = fromDAT(in)
	}
	// WARNING: not sure what this actually catches?
	if df.Error() != nil {
		log.Fatalf("error: %v", df.Error())
	}
	if len(config.Fields) > 0 {
		if err := df.SetNames(config.Fields...); err != nil {
			log.Fatalf("error: %v", err)
		}
	}
	return df
}

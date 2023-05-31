package input

import (
	"io"
	"log"
	"strings"
	"unicode/utf8"

	"github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

const (
	CSV string = "csv"
	DAT string = "dat"

	DfltCSVDelimiter rune = ','
	DfltCSVComment   rune = '#'
)

type csvSpec struct {
	HasHeader bool   `yaml:"has_header,omitempty"`
	Delimiter string `yaml:"delimiter,omitempty"`
	Comment   string `yaml:"comment,omitempty"`
}

func decodeRuneOrDefault(s string, dflt rune) rune {
	r, n := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && n == 0 {
		return dflt
	}
	if r == utf8.RuneError && n == 1 {
		log.Fatalf("could not decode rune from: %v", s)
	}
	return r
}

func fromCSV(in io.Reader, config *Config) *dataframe.DataFrame {
	var s csvSpec
	if err := config.FormatSpec.Decode(&s); err != nil {
		log.Fatalf("error: %v", err)
	}
	df := dataframe.ReadCSV(
		in,
		dataframe.HasHeader(s.HasHeader),
		dataframe.WithDelimiter(decodeRuneOrDefault(s.Delimiter, DfltCSVDelimiter)),
		dataframe.WithComments(decodeRuneOrDefault(s.Comment, DfltCSVComment)),
		dataframe.DefaultType(series.Float),
	)
	return &df
}

func fromDAT(in io.Reader) *dataframe.DataFrame {
	r := dat.NewReader(in)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	df := dataframe.LoadRecords(
		records,
		dataframe.HasHeader(false),
		dataframe.DefaultType(series.Float),
	)
	return &df
}

func CreateDataFrame(in io.Reader, config *Config) *dataframe.DataFrame {
	var df *dataframe.DataFrame
	switch strings.ToLower(config.Format) {
	case CSV:
		df = fromCSV(in, config)
	case DAT:
		df = fromDAT(in)
	}
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

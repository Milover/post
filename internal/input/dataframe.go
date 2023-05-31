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
	// Tags for supported input format types.
	CSV string = "csv"
	DAT string = "dat"

	DfltCSVDelimiter rune = ','
	DfltCSVComment   rune = '#'
)

// csvSpec contains data needed for parsing CSV formatted input.
type csvSpec struct {
	HasHeader bool   `yaml:"has_header,omitempty"`
	Delimiter string `yaml:"delimiter,omitempty"`
	Comment   string `yaml:"comment,omitempty"`
}

// newCsvSpec returns a csvSpec with 'sensible' default values.
func newCsvSpec() csvSpec {
	return csvSpec{
		HasHeader: true,
		Delimiter: string(DfltCSVDelimiter),
		Comment:   string(DfltCSVComment),
	}
}

// decodeRuneOrDefault tries to decode a rune from a string and returns the
// decoded rune on success, or the dflt if the string is empty.
// It fails if the encoding is invalid.
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

// fromCSV reads and returns a dataframe.DataFrame from CSV formatted input,
// applying options from the config.
func fromCSV(in io.Reader, config *Config) *dataframe.DataFrame {
	s := newCsvSpec()
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

// fromCSV reads and returns a dataframe.DataFrame from OpenFOAM DAT formatted
// input.
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

// CreateDataFrame reads and returns a dataframe.DataFrame from formatted input,
// applying options from the config.
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

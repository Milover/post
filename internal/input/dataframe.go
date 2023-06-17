package input

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
)

var (
	ErrInputFile     = errors.New("input: input file not specified")
	ErrInvalidFormat = fmt.Errorf(
		"bad input format type, available formats are: %q",
		maps.Keys(FormatTypes))
)

// FormatReader is a function which reads in a dataframe.DataFrame from
// formatted input, the details of which are described in the config.
type FormatReader func(io.Reader, *Config) (*dataframe.DataFrame, error)

// FormatTypes maps Format type tags to FormatReaders.
var FormatTypes = map[string]FormatReader{
	"csv": fromCSV,
	"dat": fromDAT,
}

// CSV reader defaults.
const (
	DfltCSVDelimiter rune = ','
	DfltCSVComment   rune = '#'
)

// csvSpec contains data needed for parsing CSV formatted input.
type csvSpec struct {
	HasHeader bool   `yaml:"has_header"`
	Delimiter string `yaml:"delimiter"`
	Comment   string `yaml:"comment"`
}

// defaultCsvSpec returns a csvSpec with 'sensible' default values.
func defaultCsvSpec() csvSpec {
	return csvSpec{
		HasHeader: true,
		Delimiter: string(DfltCSVDelimiter),
		Comment:   string(DfltCSVComment),
	}
}

// decodeRuneOrDefault tries to decode a rune from a string and returns the
// decoded rune on success or dflt if the decoding fails.
func decodeRuneOrDefault(s string, dflt rune) rune {
	r, _ := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return dflt
	}
	return r
}

// fromCSV reads and returns a dataframe.DataFrame from CSV formatted input,
// applying options from the config.
// If an error occurs, *dataframe.DataFrame will be nil.
func fromCSV(in io.Reader, config *Config) (*dataframe.DataFrame, error) {
	s := defaultCsvSpec()
	if err := config.FormatSpec.Decode(&s); err != nil {
		return nil, err
	}
	df := dataframe.ReadCSV(
		in,
		dataframe.HasHeader(s.HasHeader),
		dataframe.WithDelimiter(decodeRuneOrDefault(s.Delimiter, DfltCSVDelimiter)),
		dataframe.WithComments(decodeRuneOrDefault(s.Comment, DfltCSVComment)),
		dataframe.DefaultType(series.Float),
	)
	if df.Error() != nil {
		return nil, df.Error()
	}
	return &df, nil
}

// fromCSV reads and returns a dataframe.DataFrame from OpenFOAM DAT formatted
// input.
// If an error occurs, *dataframe.DataFrame will be nil.
func fromDAT(in io.Reader, config *Config) (*dataframe.DataFrame, error) {
	r := dat.NewReader(in)
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

// ReadDataFrame reads and returns a dataframe.DataFrame from formatted input,
// applying options from the config.
// If an error occurs, *dataframe.DataFrame will be nil.
func ReadDataFrame(in io.Reader, config *Config) (*dataframe.DataFrame, error) {
	formatter, found := FormatTypes[strings.ToLower(config.Format)]
	if !found {
		return nil, ErrInvalidFormat
	}
	config.Log.WithFields(logrus.Fields{
		"format": strings.ToLower(config.Format),
	}).Debug("reading input")
	df, err := formatter(in, config)
	if err != nil {
		return nil, err
	}
	if len(config.Fields) > 0 {
		if err = df.SetNames(config.Fields...); err != nil {
			return nil, err
		}
	}
	return df, nil
}

// CeateDataFrame creates a dataframe.DataFrame as specified in the config.
// If an error occurs, *dataframe.DataFrame will be nil.
func CreateDataFrame(config *Config) (*dataframe.DataFrame, error) {
	f, err := os.Open(config.File)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadDataFrame(f, config)
}

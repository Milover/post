package input

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Milover/foam-postprocess/internal/encoding/dat"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
)

var (
	ErrSeriesFile      = errors.New("input: series table file not specified")
	ErrSeriesDirectory = errors.New("input: series directory not specified")
	ErrInvalidFormat   = fmt.Errorf(
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

// DefaultCsvSpec returns a csvSpec with 'sensible' default values.
func DefaultCsvSpec() csvSpec {
	return csvSpec{
		HasHeader: true,
		Delimiter: string(DfltCSVDelimiter),
		Comment:   string(DfltCSVComment),
	}
}

// seriesSpec contains data needed for parsing an OpenFOAM table series,
// which is of the following format:
//
//	.
//	├── 0.0
//	│   ├── data_0.csv
//	│   ├── data_1.dat
//	│   └── ...
//	├── 0.1
//	│   ├── data_0.csv
//	│   ├── data_1.dat
//	│   └── ...
//	└── ...
//
// where each data_*.* file contains the data in some format at the moment in
// time specified by the directory name.
// Each series dataset is output into a different file, i.e., the data_0.csv
// files contain one dataset, data_1.dat another one, and so on.
type seriesSpec struct {
	// SeriesDirectory is the top-level directory which contains
	// all series files and directories.
	SeriesDirectory string `yaml:"series_directory"`
	// SeriesFile is the file name of a specfic data set within the series.
	SeriesFile string `yaml:"series_file"`
	// TimeName is the name of the time field.
	// If left empty it is set to 'time'.
	SeriesTimeName string `yaml:"series_time_name"`

	Log *logrus.Logger `yaml:"-"`
}

// DefaultCsvSeriesSpec returns a seriesSpec with 'sensible' default values.
func DefaultSeriesSpec() seriesSpec {
	return seriesSpec{
		SeriesTimeName: "time",
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
	s := DefaultCsvSpec()
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

// fromDAT reads and returns a dataframe.DataFrame from OpenFOAM DAT formatted
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
	config.Log.Debug("reading input")
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

// walkStruct is a helper struct for holding data related to walking a
// foam series directory structure.
type walkStruct struct {
	// Time is the time (directory) currently being processed.
	Time float64
	// Rows is a slice used for building the time field.
	Rows []float64
}

// ReadSeries reads and returns a dataframe.DataFrame from
// an OpenFOAM table series input.
// If an error occurs, *dataframe.DataFrame will be nil.
func ReadSeries(config *Config) (*dataframe.DataFrame, error) {
	s := DefaultSeriesSpec()
	if err := config.SeriesSpec.Decode(&s); err != nil {
		return nil, err
	}
	if len(s.SeriesDirectory) == 0 {
		return nil, ErrSeriesDirectory
	}
	if len(s.SeriesFile) == 0 {
		return nil, ErrSeriesFile
	}

	// walk and process the series directory
	var df *dataframe.DataFrame
	var ws walkStruct
	fsys := os.DirFS(s.SeriesDirectory)
	// FIXME: the dataframe.DataFrame operations are mysterious, so no idea
	// where allocations happen or how many there are --- should check this
	// at some point.
	walkFn := func(path string, d fs.DirEntry, err error) error {
		// stop walking on any error, since there shouldn't be any
		if err != nil {
			return err
		}
		// the directory name is the current time
		if d.IsDir() {
			if d.Name() == "." {
				return nil
			}
			ws.Time, err = strconv.ParseFloat(d.Name(), 64)
			return err
		}
		// only process the specified files
		if d.Name() != s.SeriesFile {
			return nil
		}

		config.Log.WithFields(logrus.Fields{
			"file":   path,
			"format": strings.ToLower(config.Format),
		}).Debug("reading dataframe")
		// try to create a dataframe from the file
		f, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		temp, err := ReadDataFrame(f, config)
		if err != nil {
			return err
		}

		// all files should have the same number of rows, so we allocate
		// only once, hence we can error if this is not the case
		if len(ws.Rows) == 0 {
			ws.Rows = make([]float64, temp.Nrow())
		}
		for i := range ws.Rows {
			ws.Rows[i] = ws.Time
		}
		*temp = dataframe.New(series.New(
			ws.Rows, series.Float, s.SeriesTimeName)).CBind(*temp)
		if temp.Error() != nil {
			return temp.Error()
		}
		// concatonate the new dataframe
		if df == nil {
			df = temp
			return nil
		}
		*df = df.RBind(*temp)
		return df.Error()
	}
	if err := fs.WalkDir(fsys, ".", walkFn); err != nil {
		return nil, err
	}

	if df != nil {
		*df = df.Arrange(dataframe.Sort(s.SeriesTimeName))
		if df.Error() != nil {
			return nil, df.Error()
		}
	}
	return df, nil
}

// CeateDataFrame creates a dataframe.DataFrame as specified in the config.
// If an error occurs, *dataframe.DataFrame will be nil.
func CreateDataFrame(config *Config) (*dataframe.DataFrame, error) {
	if config.IsSeries() {
		return ReadSeries(config)
	}
	config.Log.WithFields(logrus.Fields{
		"file":   config.File,
		"format": strings.ToLower(config.Format),
	}).Debug("creating dataframe")
	f, err := os.Open(config.File)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadDataFrame(f, config)
}

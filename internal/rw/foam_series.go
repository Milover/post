package rw

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"gopkg.in/yaml.v3"
)

// foamSeries contains data needed for parsing an OpenFOAM table series,
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
type foamSeries struct {
	// File is the file name of the CSV-formatted data files.
	File string `yaml:"file"`
	// Archive is the path to the archive file which is
	// the root of the archived filesystem.
	Archive string `yaml:"archive"`
	// Directory is the root directory of the foam-series.
	Directory string `yaml:"directory"`
	// TimeName is the name of the time field.
	// If left empty it is set to 'time'.
	TimeName string `yaml:"time_name"`
	// FormatSpec is the config for the series file type input,
	// e.g., if the series consists of CSV files, FormatSpec would define
	// a config for a CSV input type.
	FormatSpec Config `yaml:"format_spec"`
}

func defaultFoamSeries() *foamSeries {
	return &foamSeries{
		TimeName: "time",
	}
}

func NewFoamSeries(n *yaml.Node) (*foamSeries, error) {
	rw := defaultFoamSeries()
	if err := n.Decode(rw); err != nil {
		return nil, err
	}
	if len(rw.File) == 0 {
		return nil, fmt.Errorf("foam-series: %w: %v", common.ErrUnsetField, "file")
	}
	if len(rw.Directory) == 0 {
		return nil, fmt.Errorf("foam-series: %w: %v", common.ErrUnsetField, "directory")
	}
	return rw, nil
}

// walkStruct is a helper struct for holding data related to walking a
// foam series directory structure.
type walkStruct struct {
	// Time is the time (directory) currently being processed.
	Time float64
	// Rows is a slice used for building the time field.
	Rows []float64
}

func (rw *foamSeries) Read() (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame
	var ws walkStruct
	var fsys fs.FS
	if rw.Archive != "" {
		var err error
		fsys, err = NewArchiveFS(rw.Archive)
		if err != nil {
			return nil, err
		}
	} else {
		if _, err := os.Stat(rw.Directory); err != nil {
			return nil, err
		}
		fsys = os.DirFS(rw.Directory)
	}
	// FIXME: the dataframe.DataFrame operations are mysterious, so no idea
	// where allocations happen or how many there are --- should check this
	// at some point.
	// OPTIMIZE: we should skip directories (return fs.SkipDir) which are not
	// on the correct path to the foam-series root directory.
	targetPrefix := filepath.Clean(rw.Directory) + string(os.PathSeparator)
	walkFn := func(path string, d fs.DirEntry, err error) error {
		// stop walking on any error, since there shouldn't be any
		if err != nil {
			return err
		}
		// regular FSs need to skip the root directory ("."),
		// archiveFSs also need to skip until the foam-series root directory
		if path == "." {
			return nil
		} else if rw.Archive != "" && !strings.HasPrefix(path, targetPrefix) {
			return nil
		}
		// the directory name is the current time
		if d.IsDir() {
			ws.Time, err = strconv.ParseFloat(d.Name(), 64)
			return err
		}
		// only process the specified files
		if d.Name() != rw.File {
			return nil
		}
		f, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		temp, err := ReadOutOf(f, &rw.FormatSpec)
		if err != nil {
			return fmt.Errorf("%w: in file: %v", err, path)
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
			ws.Rows, series.Float, rw.TimeName)).CBind(*temp)
		if temp.Error() != nil {
			return fmt.Errorf("%w: in file: %v", temp.Error(), path)
		}
		// concatonate the new dataframe
		if df == nil {
			df = temp
			return nil
		}
		*df = df.RBind(*temp)
		if df.Error() != nil {
			return fmt.Errorf("%w: in file: %v", df.Error(), path)
		}
		return nil
	}
	if err := fs.WalkDir(fsys, ".", walkFn); err != nil {
		return nil, fmt.Errorf("foam-series: %w", err)
	}
	if df != nil {
		*df = df.Arrange(dataframe.Sort(rw.TimeName))
		if df.Error() != nil {
			return nil, fmt.Errorf("foam-series: %w", df.Error())
		}
	}
	return df, nil
}

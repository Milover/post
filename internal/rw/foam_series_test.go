package rw

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	stdcsv "encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/stretchr/testify/assert"
	"github.com/ulikunitz/xz"
	"gopkg.in/yaml.v3"
)

type foamSeriesTest struct {
	Name        string
	Config      string
	Output      dataframe.DataFrame
	SkipCompare bool
	Error       error
}

var goodSeries = dataframe.New(
	series.New([]float64{
		0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
		0.2, 0.2, 0.2, 0.2, 0.2, 0.2,
		0.3, 0.3, 0.3, 0.3, 0.3, 0.3}, series.Float, "time"),
	series.New([]int{
		0, 1, 2, 3, 4, 5,
		0, 1, 2, 3, 4, 5,
		0, 1, 2, 3, 4, 5}, series.Int, "x"),
	series.New([]int{
		0, 1, 2, 2, 1, 0,
		0, 1, 2, 2, 1, 0,
		0, 1, 2, 2, 1, 0}, series.Int, "y"),
)

var foamSeriesReadTests = []foamSeriesTest{
	{
		Name: "good-csv",
		Config: `
directory: 'testdata/foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "good-dat",
		Config: `
directory: 'testdata/foam_series.good'
file: data.dat
time_name: 'time'
format_spec:
  type: dat
`,
		Output: dataframe.New(
			series.New([]float64{
				0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
				0.2, 0.2, 0.2, 0.2, 0.2, 0.2,
				0.3, 0.3, 0.3, 0.3, 0.3, 0.3}, series.Float, "time"),
			series.Ints([]int{
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5,
				0, 1, 2, 3, 4, 5}),
			series.Ints([]int{
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0,
				0, 1, 2, 2, 1, 0}),
		),
		Error: nil,
	},
	{
		Name: "good-unsorted",
		Config: `
directory: 'testdata/foam_series.good_unsorted'
file: data.dat
time_name: 'time'
format_spec:
  type: dat
`,
		Output: dataframe.New(
			series.New([]float64{
				0, 0,
				1, 1,
				2, 2,
				3, 3,
				4, 4,
				5, 5,
				6, 6,
				7, 7,
				8, 8,
				9, 9,
				10, 10,
				11, 11,
				12, 12,
				13, 13,
				14, 14,
				15, 15,
				16, 16,
				17, 17,
				18, 18,
				19, 19,
				20, 20}, series.Float, "time"),
			series.Ints([]int{
				0, 0,
				0, 1,
				0, 2,
				0, 3,
				0, 4,
				0, 5,
				0, 6,
				0, 7,
				0, 8,
				0, 9,
				0, 10,
				0, 11,
				0, 12,
				0, 13,
				0, 14,
				0, 15,
				0, 16,
				0, 17,
				0, 18,
				0, 19,
				0, 20}),
		),
		Error: nil,
	},
	{
		Name: "good-empty",
		Config: `
directory: 'testdata/foam_series.good_empty'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
`,
		Output:      dataframe.DataFrame{},
		SkipCompare: true,
		Error:       nil,
	},
	{
		Name: "good-empty-times",
		Config: `
directory: 'testdata/foam_series.good_empty_times'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
`,
		Output:      dataframe.DataFrame{},
		SkipCompare: true,
		Error:       nil,
	},
	{
		Name: "good-csv-tar.xz",
		Config: `
archive: 'testdata/foam_series.good.tar.xz'
directory: 'foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "good-csv-zip",
		Config: `
archive: 'testdata/foam_series.good.zip'
directory: 'foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "good-csv-archive.tgz",
		Config: `
archive: 'testdata/archive.tgz'
directory: 'archive/foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "good-csv-archive.zip",
		Config: `
archive: 'testdata/archive.zip'
directory: 'archive/foam_series.good'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: goodSeries,
		Error:  nil,
	},
	{
		Name: "bad-unequal-rows",
		Config: `
directory: 'testdata/foam_series.bad_unequal_rows'
file: data.csv
time_name: 'time'
format_spec:
  type: csv
  type_spec:
    header: true
`,
		Output: dataframe.DataFrame{},
		Error:  errors.New("error"), // not matching explicitly, so doesn't matter
	},
}

func TestFoamSeriesRead(t *testing.T) {
	for _, tt := range foamSeriesReadTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			raw, err := io.ReadAll(strings.NewReader(tt.Config))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			rw, err := NewFoamSeries(&config)
			assert.Nil(err, "unexpected NewFoamSeries() error")
			out, err := rw.Read()

			if tt.Error != nil {
				assert.NotNil(err)
				assert.Nil(out)
			} else {
				assert.Nil(err)
				if !tt.SkipCompare {
					assert.Equal(tt.Output, *out)
				}
			}
		})
	}
}

// Benchmarks for reading a foam-series in various configurations.
// The benchmark reads floats from CSV file into a foam-series,
// since this is the most common use case.
type foamSeriesBench struct {
	Name      string
	Config    string
	FormatTyp int // foam-series input format type
	NFiles    int // the total number of files (time directories)
	FileSize  int // the approx. size in bytes of individual data files
}

const (
	// benchDir is the name of the root directory of the foam-series.
	benchDir string = "foam-series-read-bench"
	// benchCsv is the name of the CSV data file(s).
	benchCsv string = "data.csv"
	// benchConfig is the benchmark Config.
	// The 'directory' and 'archive' fields are set during the benchmark.
	benchConfig string = `
file: data.csv
format_spec:
  type: csv
`
	// Constants for the various foam-series input types.
	B_REG int = iota
	B_TAR
	B_TXZ
	B_ZIP
)

var benchTemplates = []foamSeriesBench{
	{
		Name:      "regular",
		Config:    benchConfig,
		FormatTyp: B_REG,
	},
	{
		Name:      "tar",
		Config:    benchConfig,
		FormatTyp: B_TAR,
	},
	{
		Name:      "tar.xz",
		Config:    benchConfig,
		FormatTyp: B_TXZ,
	},
	{
		Name:      "zip",
		Config:    benchConfig,
		FormatTyp: B_ZIP,
	},
}

// csvBytes writes about n bytes using CSV encoding.
func csvBytes(n int) ([]byte, error) {
	rng := rand.New(rand.NewSource(0))
	get := func() string {
		return strconv.FormatFloat(rng.Float64(), 'f', 8, 64)
	}
	size := n / 22 // approx. 22B per a row; this will floor, but that's fine

	records := make([][]string, 0, size+1)
	records = append(records, []string{"x", "y"}) // header
	row := make([]string, 2)
	for i := 0; i < size; i++ {
		row[0], row[1] = get(), get()
		records = append(records, row)
	}

	var b bytes.Buffer
	b.Grow(n)
	w := stdcsv.NewWriter(&b)
	err := w.WriteAll(records)
	return b.Bytes(), err
}

func BenchmarkFoamSeriesRead(b *testing.B) {
	// A map of the ammount of files and file sizes used during benchmarking.
	var benchNBySize map[int][]int
	if testing.Short() { // debugging
		benchNBySize = map[int][]int{
			10: {1024},
		}
	} else {
		benchNBySize = map[int][]int{
			10:   {1024, 10240, 102400},
			100:  {1024, 10240, 102400},
			1000: {1024, 10240, 102400},
		}
	}
	// create the benchmark test structures
	benchesCap := len(benchTemplates) * len(benchNBySize) * len(benchNBySize[10])
	benches := make([]foamSeriesBench, 0, benchesCap)
	for _, tmpl := range benchTemplates {
		for nfiles, fsizes := range benchNBySize {
			for _, s := range fsizes {
				b := tmpl
				b.Name += "-" + strconv.Itoa(nfiles) + "x" + strconv.Itoa(s) + "B"
				b.NFiles = nfiles
				b.FileSize = s

				benches = append(benches, b)
			}
		}
	}
	for _, bb := range benches {
		b.Run(bb.Name, func(b *testing.B) {
			assert := assert.New(b)

			// create the file list
			tempDir := b.TempDir() // purged by b.Cleanup()
			rootDir := benchDir
			files := make([]string, (2*bb.NFiles)+1)
			var dir, time, csvPath string
			// we leave index 0 empty because it's bb.FormatTyp specific,
			// and we add both the directory and the file in a single pass
			for i := 0; i < len(files)-1; i += 2 {
				if i%2 != 0 {
					continue
				}
				time = strconv.FormatFloat(float64(i/2+1), 'f', 1, 64)
				dir = filepath.Join(rootDir, time)
				csvPath = filepath.Join(dir, benchCsv)

				files[i+1] = dir + "/" // so we know it's a directory
				files[i+2] = csvPath
			}

			// write the files and adjust the config
			var configString string
			csvBody, err := csvBytes(bb.FileSize)
			assert.Nil(err, "unexpected csvBytes() error")
			switch bb.FormatTyp {
			case B_REG:
				root := filepath.Join(tempDir, rootDir)
				configString = fmt.Sprintf("directory: %v", root) + bb.Config

				files[0] = rootDir + "/"
				for _, file := range files {
					path := filepath.Join(tempDir, file)
					if strings.HasSuffix(file, "/") {
						err := os.Mkdir(path, 0700)
						assert.Nil(err, "unexpected os.Mkdir() error")
					} else {
						err := os.WriteFile(path, csvBody, 0600)
						assert.Nil(err, "unexpected os.WriteFile() error")
					}
				}
			case B_TAR, B_TXZ:
				var archive string
				if bb.FormatTyp == B_TXZ {
					archive = filepath.Join(tempDir, rootDir+".tar.xz")
					configString = fmt.Sprintf("directory: %v\narchive: %v",
						rootDir, archive) + bb.Config
				} else {
					archive = filepath.Join(tempDir, rootDir+".tar")
					configString = fmt.Sprintf("directory: %v\narchive: %v",
						rootDir, archive) + bb.Config
				}

				// create the writers
				f, err := os.Create(archive)
				assert.Nil(err, "unexpected os.Create() error")
				var xzw *xz.Writer
				var w *tar.Writer
				if bb.FormatTyp == B_TXZ {
					xzw, err = xz.NewWriter(f)
					assert.Nil(err, "unexpected xz.Create() error")
					w = tar.NewWriter(xzw)
				} else {
					w = tar.NewWriter(f)
				}

				// write the archive
				files[0] = benchDir + "/"
				var hdr tar.Header
				for _, file := range files {
					if strings.HasSuffix(file, "/") {
						hdr.Typeflag = tar.TypeDir
						hdr.Name = file
						hdr.Mode = 0700
					} else {
						hdr.Typeflag = tar.TypeReg
						hdr.Name = file
						hdr.Mode = 0600
						hdr.Size = int64(len(csvBody))
					}
					err := w.WriteHeader(&hdr)
					assert.Nil(err, "unexpected tar.Writer.WriteHeader() error")
					if !strings.HasSuffix(file, "/") {
						_, err = w.Write(csvBody)
						assert.Nil(err, "unexpected tar.Writer.Write() error")
					}
				}

				// close everything
				err = w.Close()
				assert.Nil(err, "unexpected tar.Writer.Close() error")
				if bb.FormatTyp == B_TXZ {
					err = xzw.Close()
					assert.Nil(err, "unexpected xz.Writer.Close() error")
				}
				err = f.Close()
				assert.Nil(err, "unexpected os.File.Close() error")
			case B_ZIP:
				archive := filepath.Join(tempDir, rootDir+".zip")
				configString = fmt.Sprintf("directory: %v\narchive: %v",
					rootDir, archive) + bb.Config

				// create the writers
				f, err := os.Create(archive)
				assert.Nil(err, "unexpected os.Create() error")
				w := zip.NewWriter(f)

				// write the archive
				files[0] = benchDir + "/"
				for _, file := range files {
					fw, err := w.Create(file)
					assert.Nil(err, "unexpected zip.Writer.Create() error")
					if strings.HasSuffix(file, "/") {
						_, err = fw.Write([]byte{})
						assert.Nil(err, "unexpected io.Writer.Write() error")
					} else {
						_, err = fw.Write(csvBody)
						assert.Nil(err, "unexpected io.Writer.Write() error")
					}
				}

				// close everything
				err = w.Close()
				assert.Nil(err, "unexpected zip.Writer.Close() error")
				err = f.Close()
				assert.Nil(err, "unexpected os.File.Close() error")
			default:
				panic("bad foam-series input type")
			}

			// create the config
			raw, err := io.ReadAll(strings.NewReader(configString))
			assert.Nil(err, "unexpected io.ReadAll() error")
			var config yaml.Node
			err = yaml.Unmarshal(raw, &config)
			assert.Nil(err, "unexpected yaml.Unmarshal() error")

			// create the reader
			rw, err := NewFoamSeries(&config)
			assert.Nil(err, "unexpected NewFoamSeries() error")

			// benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rw.Read(); err != nil {
					b.Fail()
				}
			}
		})
	}
}

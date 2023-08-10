package rw

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	ErrBadReader = fmt.Errorf(
		"bad input type, available types are: %q",
		common.MapKeys(Readers))
	ErrBadReaderOutOf = fmt.Errorf(
		"bad input type, available types are: %q",
		common.MapKeys(ReadersFromFn))
)

type Reader interface {
	Read() (*dataframe.DataFrame, error)
}

type ReaderFunc func(string) (io.ReadCloser, error)
type ReaderFromFn interface {
	ReadFromFn(ReaderFunc) (*dataframe.DataFrame, error)
}

type ReaderFactory func(*yaml.Node) (Reader, error)
type ReaderOutOfFactory func(*yaml.Node) (ReaderFromFn, error)

var Readers = map[string]ReaderFactory{
	"csv":         func(n *yaml.Node) (Reader, error) { return NewCsv(n) },
	"dat":         func(n *yaml.Node) (Reader, error) { return NewDat(n) },
	"time-series": func(n *yaml.Node) (Reader, error) { return NewTimeSeries(n) },
	"ram":         func(n *yaml.Node) (Reader, error) { return NewRam(n) },
	"multiple":    func(n *yaml.Node) (Reader, error) { return NewMultiple(n) },
	"archive":     func(n *yaml.Node) (Reader, error) { return NewArchive(n) },
}
var ReadersFromFn = map[string]ReaderOutOfFactory{
	"csv":         func(n *yaml.Node) (ReaderFromFn, error) { return NewCsv(n) },
	"dat":         func(n *yaml.Node) (ReaderFromFn, error) { return NewDat(n) },
	"time-series": func(n *yaml.Node) (ReaderFromFn, error) { return NewTimeSeries(n) },
}

// DecodeRuneOrDefault tries to decode a rune from a string and returns the
// decoded rune on success or dflt if the decoding fails.
func DecodeRuneOrDefault(s string, dflt rune) rune {
	r, _ := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return dflt
	}
	return r
}

func Read(config *Config) (*dataframe.DataFrame, error) {
	factory, found := Readers[strings.ToLower(config.Type)]
	if !found {
		return nil, fmt.Errorf("%w, got: %q", ErrBadReader, config.Type)
	}
	r, err := factory(&config.TypeSpec)
	if err != nil {
		return nil, err
	}
	df, err := r.Read()
	if err != nil {
		return nil, err
	}
	return SetNames(df, config.Fields)
}

func ReadFromFn(fn ReaderFunc, config *Config) (*dataframe.DataFrame, error) {
	factory, found := ReadersFromFn[strings.ToLower(config.Type)]
	if !found {
		return nil, fmt.Errorf("%w, got: %q", ErrBadReaderOutOf, config.Type)
	}
	r, err := factory(&config.TypeSpec)
	if err != nil {
		return nil, err
	}
	df, err := r.ReadFromFn(fn)
	if err != nil {
		return nil, err
	}
	return SetNames(df, config.Fields)
}

func SetNames(df *dataframe.DataFrame, names []string) (*dataframe.DataFrame, error) {
	if len(names) > 0 {
		if err := df.SetNames(names...); err != nil {
			return nil, err
		}
	}
	return df, nil
}

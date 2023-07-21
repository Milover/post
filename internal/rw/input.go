package rw

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/go-gota/gota/dataframe"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

var (
	ErrBadReader = fmt.Errorf(
		"bad input type, available types are: %q",
		maps.Keys(Readers))
	ErrBadReaderOutOf = fmt.Errorf(
		"bad input type, available types are: %q",
		maps.Keys(ReadersOutOf))
)

type ReaderFactory func(*yaml.Node) (Reader, error)
type ReaderOutOfFactory func(*yaml.Node) (ReaderOutOf, error)

var Readers = map[string]ReaderFactory{
	"csv":         func(n *yaml.Node) (Reader, error) { return NewCsv(n) },
	"dat":         func(n *yaml.Node) (Reader, error) { return NewDat(n) },
	"foam-series": func(n *yaml.Node) (Reader, error) { return NewFoamSeries(n) },
	"ram":         func(n *yaml.Node) (Reader, error) { return NewRam(n) },
}
var ReadersOutOf = map[string]ReaderOutOfFactory{
	"csv": func(n *yaml.Node) (ReaderOutOf, error) { return NewCsv(n) },
	"dat": func(n *yaml.Node) (ReaderOutOf, error) { return NewDat(n) },
}

type Reader interface {
	Read() (*dataframe.DataFrame, error)
}

type ReaderOutOf interface {
	ReadOutOf(io.Reader) (*dataframe.DataFrame, error)
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
		return nil, ErrBadReader
	}
	r, err := factory(&config.TypeSpec)
	if err != nil {
		return nil, err
	}
	df, err := r.Read()
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

func ReadOutOf(in io.Reader, config *Config) (*dataframe.DataFrame, error) {
	factory, found := ReadersOutOf[strings.ToLower(config.Type)]
	if !found {
		return nil, ErrBadReaderOutOf
	}
	r, err := factory(&config.TypeSpec)
	if err != nil {
		return nil, err
	}
	df, err := r.ReadOutOf(in)
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

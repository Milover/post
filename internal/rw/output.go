package rw

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	ErrBadOutputType = fmt.Errorf(
		"bad output type, available types are: %q",
		common.MapKeys(Writers))
)

type WriterFactory func(*yaml.Node) (Writer, error)

var Writers = map[string]WriterFactory{
	"csv": func(n *yaml.Node) (Writer, error) { return NewCsv(n) },
	"ram": func(n *yaml.Node) (Writer, error) { return NewRam(n) },
}

type Writer interface {
	Write(*dataframe.DataFrame) error
}

// Write executes all Writers as defined in the config
// An error is returned if any of the Writers return an error.
func Write(df *dataframe.DataFrame, configs []Config) error {
	var err error
	for i := range configs {
		err = errors.Join(err, write(df, &configs[i]))
	}
	return err
}

// write is a helper function which executes a single Writer.
func write(df *dataframe.DataFrame, config *Config) error {
	factory, found := Writers[strings.ToLower(config.Type)]
	if !found {
		return ErrBadOutputType
	}
	w, err := factory(&config.TypeSpec)
	if err != nil {
		return err
	}
	return w.Write(df)
}

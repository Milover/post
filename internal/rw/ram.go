package rw

import (
	"errors"

	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	ErrNameUnset = errors.New("output name not set")
	ErrNoExist   = errors.New("no data under given name")
)

var (
	// RAM is the global in-memory store for dataframe.DataFrames.
	// It is a singleton, hence, any change persists throughout the program
	// run time.
	RAM *ram
)

type storage map[string]*dataframe.DataFrame

type ram struct {
	// Name is the key under which the *dataframe.DataFrame will be stored.
	Name string `yaml:"name"`

	s storage
}

func defaultRam() *ram {
	return &ram{
		s: make(storage, 10),
	}
}

// NewRam initializes RAM, if it has not been initialized,
// and marshals the run time config into it.
func NewRam(n *yaml.Node) (*ram, error) {
	if RAM == nil {
		RAM = defaultRam()
	}
	if err := n.Decode(RAM); err != nil {
		return nil, err
	}
	if len(RAM.Name) == 0 {
		return nil, ErrNameUnset
	}
	return RAM, nil
}

// Write writes df to w, under the key w.Name (read from the run time config).
func (rw *ram) Write(df *dataframe.DataFrame) error {
	rw.s[rw.Name] = df
	return nil
}

// Read returns a copy of a dataframe.DataFrame, stored under the key rw.Name
// (read from the run time config), from rw.
func (rw *ram) Read() (*dataframe.DataFrame, error) {
	v, ok := rw.s[rw.Name]
	if !ok {
		return nil, ErrNoExist
	}
	temp := v.Copy()
	return &temp, nil
}

func (rw *ram) Clear() {
	rw = defaultRam()
}

package rw

import (
	"fmt"
	"log"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	// RAM is the global in-memory store for dataframe.DataFrames.
	// It is (intended to be used as) a singleton, hence, any change
	// persists throughout the program run time.
	// WARNING: technically not a singleton because it's not in a separate
	// package, so anyone in 'rw' can instantiate a raw ram.
	RAM *ram
)

type ram struct {
	// Name is the key under which a *dataframe.DataFrame will be stored.
	Name string `yaml:"name"`
	// ClearAfterRead toggles whether RAM is cleared after reading.
	ClearAfterRead bool `yaml:"clear_after_read"`

	s map[string]*dataframe.DataFrame
}

func defaultRam() *ram {
	return &ram{
		s: make(map[string]*dataframe.DataFrame, 10),
	}
}

// NewRam initializes RAM, if it has not been initialized,
// and marshals the run time config into it.
func NewRam(n *yaml.Node) (*ram, error) {
	if RAM == nil {
		if common.Verbose {
			log.Printf("ram: initializing")
		}
		RAM = defaultRam()
	}
	if err := n.Decode(RAM); err != nil {
		return nil, fmt.Errorf("ram: %w", err)
	}
	if RAM.Name == "" {
		return nil, fmt.Errorf("ram: %w: %v", common.ErrUnsetField, "name")
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
		return nil, fmt.Errorf("ram: no data under %q, available names are: %q",
			rw.Name, common.MapKeys(rw.s))
	}
	temp := v.Copy()
	if temp.Error() != nil {
		return nil, fmt.Errorf("ram: %w", temp.Error())
	}
	if rw.ClearAfterRead {
		if common.Verbose {
			log.Printf("archive: clearing: %q", common.MapKeys(rw.s))
		}
		rw.Clear()
	}
	return &temp, nil
}

func (rw *ram) Clear() {
	*rw = *defaultRam()
}

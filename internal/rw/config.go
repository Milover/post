package rw

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Type     string    `yaml:"type"`
	Fields   []string  `yaml:"fields"`
	TypeSpec yaml.Node `yaml:"type_spec"`
}

// SetExt sets the file name extension of path to ext and
// returns the new path.
func SetExt(path, ext string) string {
	e := filepath.Ext(path)
	if e == ext {
		return path
	}
	if len(e) == 0 {
		return path + ext
	}
	return strings.TrimSuffix(path, e) + ext
}

// OutDir is a function which takes a file path and, if necessary, recursively
// creates all directories necessary for the path to be valid.
func OutDir(path string) error {
	if len(path) == 0 {
		return nil
	}
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return err
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}

package rw

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	ErrFileUnset = errors.New("file not set")
)

type Config struct {
	Type     string    `yaml:"type"`
	Fields   []string  `yaml:"fields"`
	TypeSpec yaml.Node `yaml:"type_spec"`
}

type fileReader struct {
	// File is the file in which the CSV-fromatted dataframe.DataFrame
	// will be written.
	File string `yaml:"file"`
	// EnforceExtension determines whether a file name extension will be
	// enforced on the output file name.
	EnforceExtension bool `yaml:"enforce_extension"`
}

// isValidFileReader checks whether the fileReader has all necessary fields set
// and is valid.
func (fr fileReader) isValidFileReader() error {
	if len(fr.File) == 0 {
		return ErrFileUnset
	}
	return nil
}

func (fr fileReader) enforceExt(ext string) string {
	if fr.EnforceExtension {
		return SetExt(fr.File, ext)
	}
	return fr.File
}

func (fr *fileReader) openFile() (*os.File, error) {
	if err := fr.isValidFileReader(); err != nil {
		return nil, err
	}
	return os.Open(fr.File)
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

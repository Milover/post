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
	ErrUnset          = errors.New("field unset")
	ErrBadFileHandler = errors.New("bad file handler definition")
	ErrBadFSHandler   = errors.New("bad filesystem handler definition")
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

type FileHandler struct {
	// Archive is the file path to the archive file.
	Archive string `yaml:"archive"`
	// File is the file in which the CSV-fromatted dataframe.DataFrame
	// will be written.
	File string `yaml:"file"`
	// EnforceExtension determines whether a file name extension will be
	// enforced on the output file name.
	// FIXME: rename the struct or move this somewhere else, why would a file
	// reader care about output file names.
	EnforceExtension bool `yaml:"enforce_extension"`
}

func (f FileHandler) EnforceExt(ext string) string {
	if f.EnforceExtension {
		return SetExt(f.File, ext)
	}
	return f.File
}

func (f FileHandler) Open() (fs.File, error) {
	isArch := len(f.Archive) != 0
	isFile := len(f.File) != 0
	if isArch && isFile {
		ar := archiveReader(f.Archive)
		return ar.Open(f.File)
	} else if isFile {
		return os.Open(f.File)
	}
	return nil, ErrBadFileHandler
}

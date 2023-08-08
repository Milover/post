package rw

import (
	"fmt"
	"io"
	"io/fs"
	"log"

	"github.com/Milover/post/internal/common"
	"github.com/go-gota/gota/dataframe"
	"gopkg.in/yaml.v3"
)

var (
	// Archive is the global in-memory store for archives.
	// It is a singleton, hence, any change persists throughout the program
	// run time.
	Archive *archive
)

type archive struct {
	// File is the file name of the archive from which the input will be
	// read. The archive will be read into memory if it is not yet available.
	File string `yaml:"file"`
	// FormatSpec is the config for the input,
	// e.g., if a CSV file is to be read from the archive, FromatSpec would
	// define a config for a CSV input type.
	FormatSpec Config `yaml:"format_spec"`

	s map[string]fs.FS
}

func defaultArchive() *archive {
	return &archive{
		s: make(map[string]fs.FS),
	}
}

// NewArchive initializes Archive, if it has not been initialized,
// and marshals the run time config into it.
func NewArchive(n *yaml.Node) (*archive, error) {
	if Archive == nil {
		if common.Verbose {
			log.Printf("archive: initializing")
		}
		Archive = defaultArchive()
	}
	if err := n.Decode(Archive); err != nil {
		return nil, err
	}
	if Archive.File == "" {
		return nil, fmt.Errorf("archive: %w: %v", common.ErrUnsetField, "file")
	}
	return Archive, nil
}

// Read reads a dataframe.DataFrame from the archive, using the reader
// specified by the 'format_spec'.
func (a *archive) Read() (*dataframe.DataFrame, error) {
	fsys, ok := a.s[a.File]
	if !ok {
		if common.Verbose {
			log.Printf("archive: loading: %v", a.File)
		}
		var err error
		fsys, err = NewArchiveFS(a.File)
		if err != nil {
			return nil, fmt.Errorf("archive: %w", err)
		}
		a.s[a.File] = fsys
	}
	fn := func(name string) (io.ReadCloser, error) {
		return fsys.Open(name)
	}
	return ReadFromFn(fn, &a.FormatSpec)
}

func (a *archive) Clear() {
	a = defaultArchive()
}
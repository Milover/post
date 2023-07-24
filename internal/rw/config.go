package rw

import (
	"archive/tar"
	"archive/zip"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ulikunitz/xz"
	"gopkg.in/yaml.v3"
)

var (
	ErrUnset            = errors.New("field unset")
	ErrBadFormat        = errors.New("bad file format")
	ErrBadFileReader    = errors.New("bad file reader configuration")
	ErrBadArchiveReader = errors.New("bad archive reader configuration")
)

type Config struct {
	Type     string    `yaml:"type"`
	Fields   []string  `yaml:"fields"`
	TypeSpec yaml.Node `yaml:"type_spec"`
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

// because!
type multiCloser struct {
	cs []io.Closer
}

func (mc multiCloser) Close() error {
	var err error
	for i := range mc.cs {
		err = errors.Join(mc.cs[i].Close())
	}
	return err
}

func (mc *multiCloser) Add(c io.Closer) {
	mc.cs = append(mc.cs, c)
}

// fileHandle is a struct satisfying the requirements of fs.File.
// It simplifies file handling, when a different io.Reader is used for reading
// the file contents, but the native file io.Closer is required.
type fileHandle struct {
	s func() (fs.FileInfo, error)
	r io.Reader
	c io.Closer
}

func (h *fileHandle) Stat() (fs.FileInfo, error) {
	return h.s()
}

func (h *fileHandle) Read(p []byte) (int, error) {
	return h.r.Read(p)
}

func (h *fileHandle) Close() error {
	return h.c.Close()
}

// fileReader implements fs.ReadDirFS.
type fileReader struct {
	// File is the file in which the CSV-fromatted dataframe.DataFrame
	// will be written.
	File string `yaml:"file"`
	// EnforceExtension determines whether a file name extension will be
	// enforced on the output file name.
	// FIXME: rename the struct or move this somewhere else, why would a file
	// reader care about output file names.
	EnforceExtension bool `yaml:"enforce_extension"`
}

func (fr fileReader) enforceExt(ext string) string {
	if fr.EnforceExtension {
		return SetExt(fr.File, ext)
	}
	return fr.File
}

// isValid checks whether fr is valid.
func (fr fileReader) isValid() error {
	if len(fr.File) == 0 {
		return fmt.Errorf("%w: %w: %v", ErrBadFileReader, ErrUnset, "file")
	}
	return nil
}

// Open opens a file for reading.
// The caller is responsible for making sure that the fr is valid.
func (fr fileReader) Open(path string) (fs.File, error) {
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &fileHandle{
		s: func() (fs.FileInfo, error) { return f.Stat() },
		r: f,
		c: f,
	}, nil
}

func (fr fileReader) openFile() (fs.File, error) {
	if err := fr.isValid(); err != nil {
		return nil, err
	}
	return fr.Open(fr.File)
}

func (fr fileReader) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

// archiveReader implements fs.ReadDirFS.
type archiveReader struct {
	fileReader `yaml:",inline"`

	// Archive is the file path to the archive file.
	Archive string `yaml:"archive"`
}

// isValid checks whether ar is valid.
func (ar archiveReader) isValid() error {
	if len(ar.Archive) == 0 {
		return fmt.Errorf("%w: %w: %v", ErrBadArchiveReader, ErrUnset, "archive")
	}
	return nil
}

func (ar archiveReader) openFile() (fs.File, error) {
	if err := errors.Join(ar.isValid(), ar.fileReader.isValid()); err != nil {
		return ar.fileReader.openFile()
	}
	return ar.Open(ar.File)
}

// Open opens a file from an archive for reading.
// The caller is responsible for making sure that the ar is valid.
func (ar archiveReader) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}
	ext := filepath.Ext(ar.Archive)
	// tar archives can have two extensions
	if e := filepath.Ext(strings.TrimSuffix(ar.Archive, ext)); e == ".tar" {
		ext = e + ext
	}
	switch ext {
	case ".tar", ".tar.xz", ".txz", ".tar.gz", ".tgz",
		".tar.bz2", ".tb2", ".tbz", ".tbz2", ".tz2":
		f, err := os.Open(ar.Archive)
		if err != nil {
			return nil, err
		}
		var reader io.Reader = f
		switch ext {
		case ".tar.xz", ".txz":
			reader, err = xz.NewReader(reader)
		case ".tar.gz", ".tgz":
			reader, err = gzip.NewReader(reader)
		case ".tar.bz2", ".tb2", ".tbz", ".tbz2", ".tz2":
			reader = bzip2.NewReader(reader)
		}
		if err != nil {
			return nil, err
		}
		// open a specific file in the archive
		tr := tar.NewReader(reader)
		var hdr *tar.Header
		for {
			hdr, err = tr.Next()
			if err != nil {
				return nil, &fs.PathError{
					Op:   "open",
					Path: filepath.Join(ar.Archive, name),
					Err:  err,
				}
			}
			if filepath.Clean(hdr.Name) == name {
				break
			}
		}
		return &fileHandle{
			s: func() (fs.FileInfo, error) { return hdr.FileInfo(), nil },
			r: tr,
			c: f,
		}, nil
	case ".zip":
		r, err := zip.OpenReader(ar.Archive)
		if err != nil {
			return nil, err
		}
		var mc multiCloser
		mc.Add(r)
		for _, f := range r.File {
			path := filepath.Clean(f.Name)
			if path == name {
				f, err := r.Open(name)
				if err != nil {
					return nil, err
				}
				mc.Add(f)
				return &fileHandle{
					s: func() (fs.FileInfo, error) { return f.Stat() },
					r: f,
					c: mc,
				}, nil
			}
		}
		return nil, &fs.PathError{
			Op:   "open",
			Path: filepath.Join(ar.Archive),
			Err:  fs.ErrNotExist,
		}
	}
	return nil, &fs.PathError{
		Op:   "stat",
		Path: filepath.Join(ar.Archive),
		Err:  fmt.Errorf("%w: %v", ErrBadFormat, ext),
	}
}

// FIXME: this is dumb
func (ar archiveReader) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}
	entries := make([]fs.DirEntry, 0, 10) // guesstimate
	ext := filepath.Ext(ar.Archive)
	// tar archives can have two extensions
	if e := filepath.Ext(strings.TrimSuffix(ar.Archive, ext)); e == ".tar" {
		ext = e + ext
	}
	switch ext {
	case ".tar", ".tar.xz", ".txz", ".tar.gz", ".tgz",
		".tar.bz2", ".tb2", ".tbz", ".tbz2", ".tz2":
		f, err := os.Open(ar.Archive)
		if err != nil {
			return entries, err
		}
		var reader io.Reader = f
		switch ext {
		case ".tar.xz", ".txz":
			reader, err = xz.NewReader(reader)
		case ".tar.gz", ".tgz":
			reader, err = gzip.NewReader(reader)
		case ".tar.bz2", ".tb2", ".tbz", ".tbz2", ".tz2":
			reader = bzip2.NewReader(reader)
		}
		if err != nil {
			return entries, err
		}
		// open a specific file in the archive
		tr := tar.NewReader(reader)
		var hdr *tar.Header
		for {
			hdr, err = tr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return entries, &fs.PathError{
					Op:   "readdir",
					Path: filepath.Join(ar.Archive, hdr.Name),
					Err:  err,
				}
			}
			path := filepath.Clean(hdr.Name)
			if filepath.Dir(path) == name &&
				len(filepath.Base(path)) != 0 &&
				filepath.Dir(path) != filepath.Base(path) {
				entries = append(entries, fs.FileInfoToDirEntry(hdr.FileInfo()))
			}
		}
		return entries, nil
	case ".zip":
		r, err := zip.OpenReader(ar.Archive)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		for _, f := range r.File {
			path := filepath.Clean(f.Name)
			if filepath.Dir(path) == name &&
				len(filepath.Base(path)) != 0 &&
				filepath.Dir(path) != filepath.Base(path) {
				entries = append(entries, fs.FileInfoToDirEntry(f.FileInfo()))
			}
		}
		return entries, nil
	}
	return entries, &fs.PathError{
		Op:   "stat",
		Path: filepath.Join(ar.Archive),
		Err:  fmt.Errorf("%w: %v", ErrBadFormat, ext),
	}
}

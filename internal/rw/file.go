package rw

import (
	"archive/tar"
	"archive/zip"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ulikunitz/xz"
)

var (
	ErrBadFormat = errors.New("bad file format")
)

// because!
type multiCloser struct {
	cs []io.Closer
}

func (mc multiCloser) Close() error {
	for i := range mc.cs {
		if err := mc.cs[i].Close(); err != nil {
			return err
		}
	}
	return nil
}
func (mc *multiCloser) Add(c io.Closer) { mc.cs = append(mc.cs, c) }

// fileHandle is a struct satisfying the requirements of fs.File.
// It simplifies file handling, when a different io.Reader is used for reading
// the file contents, but the native file io.Closer is required.
type fileHandle struct {
	s func() (fs.FileInfo, error)
	r io.Reader
	c io.Closer
}

func (h *fileHandle) Stat() (fs.FileInfo, error) { return h.s() }
func (h *fileHandle) Read(p []byte) (int, error) { return h.r.Read(p) }
func (h *fileHandle) Close() error               { return h.c.Close() }

type ArchiveFormat int

const (
	A_UNKNOWN ArchiveFormat = iota
	A_TAR
	A_TXZ
	A_TGZ
	A_TBZ
	A_ZIP
)

func MatchFormat(name string) ArchiveFormat {
	ext := filepath.Ext(name)
	// tar archives can have two extensions
	if e := filepath.Ext(strings.TrimSuffix(name, ext)); e == ".tar" {
		ext = e + ext
	}
	switch ext {
	case ".tar":
		return A_TAR
	case ".tar.xz", ".txz":
		return A_TXZ
	case ".tar.gz", ".tgz":
		return A_TGZ
	case ".tar.bz2", ".tb2", ".tbz", ".tbz2", ".tz2":
		return A_TBZ
	case ".zip":
		return A_ZIP
	}
	return A_UNKNOWN
}

type archiveReader string

// Open opens a file from an archive for reading.
func (ar archiveReader) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	af := MatchFormat(string(ar))
	switch af {
	case A_TAR, A_TXZ, A_TGZ, A_TBZ:
		f, err := os.Open(string(ar))
		if err != nil {
			return nil, err
		}
		var reader io.Reader = f
		switch af {
		case A_TXZ:
			reader, err = xz.NewReader(reader)
		case A_TGZ:
			reader, err = gzip.NewReader(reader)
		case A_TBZ:
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
				return nil, &fs.PathError{Op: "open", Path: filepath.Join(string(ar), name), Err: err}
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
	case A_ZIP:
		r, err := zip.OpenReader(string(ar))
		if err != nil {
			return nil, err
		}
		var mc multiCloser
		mc.Add(r)
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
	return nil, &fs.PathError{Op: "stat", Path: string(ar), Err: ErrBadFormat}
}

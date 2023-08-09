package rw

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"cmp"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ulikunitz/xz"
)

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

type fileList []*fileEntry

// sort recursively sorts the fileList.
func (l fileList) sort() {
	slices.SortFunc(l, func(a, b *fileEntry) int {
		return cmp.Compare(a.Info.Name(), b.Info.Name())
	})
	for _, e := range l {
		e.Files.sort()
	}
}

type fileEntry struct {
	Info  fs.FileInfo
	Body  []byte
	Files fileList

	r *bytes.Reader
}

func (f *fileEntry) Stat() (fs.FileInfo, error) { return f.Info, nil }
func (f *fileEntry) Close() error               { return nil }
func (f *fileEntry) Read(p []byte) (int, error) {
	if f.r == nil {
		f.r = bytes.NewReader(f.Body)
	}
	return f.r.Read(p)
}
func (f *fileEntry) ResetReader() { f.r.Reset(f.Body) }

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (f fileInfo) Name() string       { return f.name }
func (f fileInfo) Size() int64        { return f.size }
func (f fileInfo) Mode() fs.FileMode  { return f.mode }
func (f fileInfo) ModTime() time.Time { return f.modTime }
func (f fileInfo) IsDir() bool        { return f.isDir }
func (f fileInfo) Sys() any           { return nil }

func NewArchiveFS(name string) (fs.FS, error) {
	var fe fileEntry
	// FIXME: this is actually disgusting
	info, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	fe.Info = fileInfo{
		name:    info.Name(),
		size:    info.Size(),
		mode:    fs.ModeDir,
		modTime: info.ModTime(),
		isDir:   true,
	}

	format := MatchFormat(name)
	switch format {
	case A_TAR, A_TXZ, A_TGZ, A_TBZ:
		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		var reader io.Reader = f
		switch format {
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
		// get the next file entry in the archive and it's path
		getNext := func(tr *tar.Reader) (*fileEntry, string, error) {
			hdr, err := tr.Next()
			if err != nil {
				return nil, "", err
			}
			body, err := io.ReadAll(tr)
			if err != nil {
				return nil, filepath.Clean(hdr.Name), err
			}
			return &fileEntry{Info: hdr.FileInfo(), Body: body},
				filepath.Clean(hdr.Name), nil
		}
		// walk the archive and build the filesystem
		tr := tar.NewReader(reader)
		currentDir := &fe.Files
		currentDirPath := "."
		for {
			entry, path, err := getNext(tr)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			// search for a new parent only when necessary
			dir := filepath.Dir(path)
			if dir != currentDirPath && dir == "." {
				currentDirPath = dir
				currentDir = &fe.Files
			} else if dir != currentDirPath {
				currentDirPath = dir
				parent, err := fe.Find(dir)
				if err != nil {
					return nil, err
				}
				currentDir = &parent.Files
			}
			*currentDir = append(*currentDir, entry)
		}
	case A_ZIP:
		z, err := zip.OpenReader(name)
		if err != nil {
			return nil, err
		}
		defer z.Close()
		// walk the archive and build the filesystem
		currentDir := &fe.Files
		currentDirPath := "."
		for _, file := range z.File {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			body, err := io.ReadAll(f)
			if err != nil {
				return nil, err
			}
			dir := filepath.Dir(filepath.Clean(file.Name))
			if dir != currentDirPath && dir == "." {
				currentDirPath = dir
				currentDir = &fe.Files
			} else if dir != currentDirPath {
				currentDirPath = dir
				parent, err := fe.Find(dir)
				if err != nil {
					return nil, err
				}
				currentDir = &parent.Files
			}
			*currentDir = append(*currentDir,
				&fileEntry{Info: file.FileInfo(), Body: body})
			if err := f.Close(); err != nil {
				return nil, err
			}
		}
	default:
		return nil, &fs.PathError{Op: "stat", Path: name, Err: os.ErrInvalid}
	}
	fe.Files.sort()
	return fe, nil
}

// Find an entry within an ArchiveFS from a path.
func (fe fileEntry) Find(path string) (*fileEntry, error) {
	if path == "." { // FIXME: only directories should return themselves?
		return &fe, nil
	}
	components := make([]string, 0, 10) // guesstimate to reduce allocations
	p := path
	for {
		dir, file := filepath.Split(p)
		components = append(components, file)
		p = filepath.Clean(dir)
		if dir == "" {
			break
		}
	}
	// reverse component order
	for i, j := 0, len(components)-1; i < j; i, j = i+1, j-1 {
		components[i], components[j] = components[j], components[i]
	}
	search := func(filename string, files []*fileEntry) (*fileEntry, error) {
		for _, f := range files {
			if f.Info.Name() == filename {
				return f, nil
			}
		}
		return nil, fs.ErrNotExist
	}
	var err error
	var s *fileEntry
	sFiles := fe.Files
	sPath := ""
	for _, cmp := range components {
		sPath = filepath.Join(sPath, cmp)
		s, err = search(cmp, sFiles)
		if err != nil {
			return nil, &fs.PathError{Op: "stat", Path: sPath, Err: err}
		}
		sFiles = s.Files
	}
	return s, nil
}

func (fe fileEntry) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	return fe.Find(name)
}

func (fe fileEntry) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	dir, err := fe.Find(name)
	if err != nil {
		return nil, err
	}
	entries := make([]fs.DirEntry, len(dir.Files))
	for i, file := range dir.Files {
		entries[i] = fs.FileInfoToDirEntry(file.Info)
	}
	return entries, nil
}

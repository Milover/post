package dat

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

var errInvalidDelim = errors.New("dat: invalid comment delimiter")

func validDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

// A Reader reads records from an OpenFOAM DAT file.
//
// As returned by NewReader, a Reader expects input conforming to the usual,
// albeit variable, OpenFOAM DAT file format: fields delimited by whitespace,
// with or without leading whitespace, which is always ignored, and
// with lines beggining with '#' denoting comments.
// All parentheses are automatically removed while reading, hence tensors
// (of any order) will yield their component values as individual fields.
//
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
//
// The Reader converts all \r\n sequences in its input to plain \n,
// including in multiline field values, so that the returned data does
// not depend on which line-ending convention an input file uses.
type Reader struct {
	// Comment, if not 0, is the comment character.
	// It is set to pound ('#') by NewReader.
	// Lines beginning with the Comment character without preceding
	// whitespace are ignored.
	// Comment must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	Comment rune

	r *bufio.Reader

	// rawBuffer is a line buffer only used by the readLine method.
	rawBuffer []byte
}

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comment: '#',
		r:       bufio.NewReader(r),
	}
}

// Read reads one record (a slice of fields) from r.
// Read always returns either a non-nil record or a non-nil error, but not both.
// If there is no data left to be read, Read returns nil, io.EOF.
func (r *Reader) Read() ([]string, error) {
	return r.readRecord()
}

// ReadAll reads all the remaining records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF.
// Because ReadAll is defined to read until EOF, it does not treat end of file
// as an error to be reported.
func (r *Reader) ReadAll() (records [][]string, err error) {
	for {
		record, err := r.readRecord()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

// readLine reads the next line (with the trailing endline).
// If EOF is hit without a trailing endline, it will be omitted.
// If some bytes were read, then the error is never io.EOF.
// The result is only valid until the next call to readLine.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		r.rawBuffer = append(r.rawBuffer[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = r.r.ReadSlice('\n')
			r.rawBuffer = append(r.rawBuffer, line...)
		}
		line = r.rawBuffer
	}
	readSize := len(line)
	if readSize > 0 && err == io.EOF {
		err = nil
		// For backwards compatibility, drop trailing \r before EOF.
		if line[readSize-1] == '\r' {
			line = line[:readSize-1]
		}
	}
	// Normalize \r\n to \n on all input lines.
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		line[n-2] = '\n'
		line = line[:n-1]
	}
	return line, err
}

// lengthNL reports the number of bytes for the trailing \n.
func lengthNL(b []byte) int {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return 1
	}
	return 0
}

// nextRune returns the next rune in b or utf8.RuneError.
func nextRune(b []byte) rune {
	r, _ := utf8.DecodeRune(b)
	return r
}

func (r *Reader) readRecord() ([]string, error) {
	if r.Comment != 0 && !validDelim(r.Comment) {
		return nil, errInvalidDelim
	}

	// Read line (automatically skipping past empty lines and any comments).
	var line []byte
	var errRead error
	for errRead == nil {
		line, errRead = r.readLine()
		if r.Comment != 0 && nextRune(line) == r.Comment {
			line = nil
			continue // Skip comment lines
		}
		if errRead == nil && len(line) == lengthNL(line) {
			line = nil
			continue // Skip empty lines
		}
		break
	}
	if errRead == io.EOF {
		return nil, errRead
	}

	// Remove parenthesis
	s := bytes.Map(
		func(r rune) rune {
			if r == '(' || r == ')' {
				return -1
			}
			return r
		},
		line,
	)

	return strings.Fields(string(s)), nil
}

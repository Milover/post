package common

import "errors"

// Common errors used throughout the code.
var (
	ErrUnsetField    = errors.New("field unset")
	ErrBadField      = errors.New("field does not exist")
	ErrBadFieldType  = errors.New("bad field type")
	ErrBadFieldValue = errors.New("bad field value")
)

const (
	// Verbose controls log output.
	Verbose bool = false
)

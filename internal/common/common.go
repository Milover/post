package common

import "errors"

// Common errors used throughout the code.
var (
	ErrBadCast       = errors.New("cannot cast to type")
	ErrUnsetField    = errors.New("field unset")
	ErrBadField      = errors.New("field does not exist")
	ErrBadFieldType  = errors.New("bad field type")
	ErrBadFieldValue = errors.New("bad field value")
)

var (
	// Verbose controls log output.
	Verbose bool
)

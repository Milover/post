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

// MapKeys returns the keys of the map m.
// The keys will be in an indeterminate order.
func MapKeys[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}

// MapValues returns the values of the map m.
// The values will be in an indeterminate order.
func MapValues[M ~map[K]V, K comparable, V any](m M) []V {
	r := make([]V, 0, len(m))
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

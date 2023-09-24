package process

import (
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/Milover/post/internal/common"
	"github.com/Milover/post/internal/numeric"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var (
	ErrAssertEqualNFields = errors.New("assert-equal: need at least two fields")
	ErrAssertEqualFail    = errors.New("assert-equal: failed")
)

// assertEqualSpec contains data needed for defining an assert-equal Processor.
type assertEqualSpec struct {
	// Fields is a list of field names for which equality is asserted.
	Fields []string `yaml:"fields"`
	// Precision is the precision up to which element values are compared,
	// machine precision by default.
	Precision float64 `yaml:"precision"`
}

// DefaultAssertEqualSpec returns a assertEqualSpec with 'sensible' default
// values.
func DefaultAssertEqualSpec() assertEqualSpec {
	return assertEqualSpec{
		Precision: numeric.Eps,
	}
}

// assertEqualProcessor asserts that all 'fields' are equal elementwise,
// up to 'precision'.
// If the assertion is true then no error is returned, otherwise
// a non-nil error is returned. df remains unchainged in either case.
func assertEqualProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultAssertEqualSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("assert-equal: %w", err)
	}
	if len(spec.Fields) < 2 {
		return fmt.Errorf("%w: %q", ErrAssertEqualNFields, spec.Fields)
	}
	if spec.Precision < 0 {
		return fmt.Errorf("assert-equal: %w: %q: %v",
			common.ErrBadFieldValue, "precision", spec.Precision)
	}
	names := df.Names()
	for _, field := range spec.Fields {
		if !slices.Contains(names, field) {
			return fmt.Errorf("assert-equal: %w: %q", common.ErrBadField, field)
		}
	}

	a := df.Col(spec.Fields[0])
	for _, field := range spec.Fields {
		b := df.Col(field)

		var equal bool
		switch typ := a.Type(); typ {
		case series.Float:
			if common.Verbose {
				log.Printf("assert-equal: comparing as %q to within %v",
					typ, spec.Precision)
			}
			x := a.Float()
			y := b.Float()
			equal = slices.EqualFunc(x, y, func(x, y float64) bool {
				return numeric.EqualEps(x, y, spec.Precision)
			})
		case series.String:
			if common.Verbose {
				log.Printf("assert-equal: comparing as %q", typ)
			}
			x := a.Records()
			y := b.Records()
			equal = slices.Equal(x, y)
		case series.Int:
			if common.Verbose {
				log.Printf("assert-equal: comparing as %q", typ)
			}
			x, erra := a.Int()
			y, errb := b.Int()
			if err := errors.Join(erra, errb); err != nil {
				return fmt.Errorf("assert-equal: %w: %w", common.ErrBadCast, err)
			}
			equal = slices.Equal(x, y)
		case series.Bool:
			if common.Verbose {
				log.Printf("assert-equal: comparing as %q", typ)
			}
			x, erra := a.Bool()
			y, errb := b.Bool()
			if err := errors.Join(erra, errb); err != nil {
				return fmt.Errorf("assert-equal: %w: %w", common.ErrBadCast, err)
			}
			equal = slices.Equal(x, y)
		default:
			return fmt.Errorf("assert-equal: %w: %v", common.ErrBadFieldType, typ)
		}
		if !equal {
			return fmt.Errorf("%w: %q != %q", ErrAssertEqualFail, a.Name, b.Name)
		}
	}
	return nil
}

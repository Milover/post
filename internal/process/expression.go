// TODO: the gval.Language stuff should go into a separate module.
package process

import (
	"errors"
	"fmt"
	"log"
	"math"

	"github.com/Milover/post/internal/common"
	"github.com/PaesslerAG/gval"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var (
	ErrExpressionFieldSize = errors.New("expression: operand size mismatch")
)

type opFunc func(_, _ interface{}) (interface{}, error)

// A buffer for the result of an expression.
var buffer []float64

// resizeBuffer sets the length of a buffer to the length of a, allocating new
// memory if necessary.
func resizeBuffer(a []float64, buff *[]float64) {
	if len(a) > len(*buff) {
		*buff = make([]float64, len(a))
	} else {
		*buff = (*buff)[:len(a)]
	}
}

// makeVectorOp creates a new binary operator working on vectors (slices) and
// scalars, based on the binary operator op.
func makeVectorOp(op func(float64, float64) float64) opFunc {
	r := buffer
	return func(a, b interface{}) (interface{}, error) {
		if as, ok := a.([]float64); ok {
			resizeBuffer(as, &r)
		}
		if bs, ok := b.([]float64); ok {
			resizeBuffer(bs, &r)
		}

		switch a.(type) {
		case []float64:
			switch b.(type) {
			case []float64:
				x := a.([]float64)    // XXX
				y := b.([]float64)    // XXX
				if len(x) != len(y) { // unreachable, should fail sooner
					return nil, ErrExpressionFieldSize
				}
				for i := range x {
					r[i] = op(x[i], y[i])
				}
				return r, nil
			case float64:
				x := a.([]float64)
				y := b.(float64)
				for i := range x {
					r[i] = op(x[i], y)
				}
				return r, nil
			default:
				return nil, fmt.Errorf("expression: %w", common.ErrBadField)
			}
		case float64:
			switch b.(type) {
			case []float64:
				x := a.(float64)
				y := b.([]float64)
				for i := range y {
					r[i] = op(y[i], x)
				}
				return r, nil
			default:
				return nil, fmt.Errorf("expression: %w", common.ErrBadField)
			}
		default:
			return nil, fmt.Errorf("expression: %w", common.ErrBadField)
		}
	}
}

// Math operators for makeVectorOp().
func add(a, b float64) float64 {
	return a + b
}
func sub(a, b float64) float64 {
	return a - b
}
func mul(a, b float64) float64 {
	return a * b
}
func div(a, b float64) float64 {
	return a / b
}
func pow(a, b float64) float64 {
	return math.Pow(a, b)
}

var sliceArithmetic = gval.NewLanguage(
	gval.InfixOperator("+", makeVectorOp(add)),
	gval.InfixOperator("-", makeVectorOp(sub)),
	gval.InfixOperator("*", makeVectorOp(mul)),
	gval.InfixOperator("/", makeVectorOp(div)),
	gval.InfixOperator("**", makeVectorOp(pow)),
)

func SliceArithmetic() gval.Language {
	return sliceArithmetic
}

// expressionSetSpec contains data needed for defining a expression-set Processor.
type expressionSpec struct {
	// Expression is an arithmetic expression string.
	Expression string `yaml:"expression"`
	// Result is the field name of the expression result.
	Result string `yaml:"result"`
}

// DefaultExpressionSetSpec returns a expressionSetSpec
// with 'sensible' default values.
func DefaultExpressionSpec() expressionSpec {
	return expressionSpec{}
}

// expressionProcessor evaluates an arithmetic expression and appends the
// resulting field to df.
// The expression operands can be scalar values or fields present in df,
// which are referenced by their names.
// NOTE: at least one of the operands must be a field present in df.
//
// Each operation involving a field is applied elementwise. The following
// arithmetic operations are supported: '+', '-', '*', '/', '**'.
func expressionProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := DefaultExpressionSpec()
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return fmt.Errorf("expression: %w", err)
	}
	if spec.Expression == "" {
		return fmt.Errorf("expression: %w: %q", common.ErrUnsetField, "expression")
	}
	if spec.Result == "" {
		return fmt.Errorf("expression: %w: %q", common.ErrUnsetField, "result")
	}
	// map field names to columns
	names := df.Names()
	env := make(map[string]interface{}, len(names))
	for n := range names {
		env[names[n]] = df.Col(names[n]).Float()
		if df.Error() != nil {
			return fmt.Errorf("expression: %w", df.Error())
		}
	}
	// remap operations to work on slices/series.Series
	lang := gval.NewLanguage(
		gval.Arithmetic(),
		SliceArithmetic(),
	)
	if common.Verbose {
		log.Printf("expression: evaluating: %q", spec.Expression)
	}
	r, err := lang.Evaluate(spec.Expression, env)
	if err != nil {
		return fmt.Errorf("expression: %w", err)
	}
	*df = df.Mutate(series.New(r, series.Float, spec.Result))
	if df.Error() != nil {
		return fmt.Errorf("expression: %w", df.Error())
	}
	return nil
}

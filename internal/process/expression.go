package process

import (
	"errors"
	"math"

	"github.com/PaesslerAG/gval"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sirupsen/logrus"
)

var (
	ErrExpression           = errors.New("expression: bad expression")
	ErrExpressionType       = errors.New("expression: bad type in expression")
	ErrExpressionVectorSize = errors.New("expression: vector size mismatch")
	ErrResult               = errors.New("expression: result field name empty")
)

type opFunc func(_, _ interface{}) (interface{}, error)

var errFunc = func(err error) opFunc {
	return func(_, _ interface{}) (interface{}, error) {
		return nil, err
	}
}

func makeVectorOp(op func(float64, float64) float64) opFunc {
	return func(a, b interface{}) (interface{}, error) {
		switch a.(type) {
		case []float64:
			switch b.(type) {
			case []float64:
				x := a.([]float64) // XXX
				y := b.([]float64) // XXX
				if len(x) != len(y) {
					return nil, ErrExpressionVectorSize
				}
				for i := range x {
					x[i] = op(x[i], y[i])
				}
				return x, nil
			case float64:
				x := a.([]float64)
				y := b.(float64)
				for i := range x {
					x[i] = op(x[i], y)
				}
				return x, nil
			default:
				return nil, ErrExpressionType
			}
		case float64:
			switch b.(type) {
			case []float64:
				x := a.(float64)
				y := b.([]float64)
				for i := range y {
					y[i] = op(y[i], x)
				}
				return y, nil
			default:
				return nil, ErrExpressionType
			}
		default:
			return nil, ErrExpressionType
		}
	}
}
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

var seriesArithmetic = gval.NewLanguage(
	gval.InfixOperator("+", makeVectorOp(add)),
	gval.InfixOperator("-", makeVectorOp(sub)),
	gval.InfixOperator("*", makeVectorOp(mul)),
	gval.InfixOperator("/", makeVectorOp(div)),
	gval.InfixOperator("**", makeVectorOp(pow)),
)

func SeriesArithmetic() gval.Language {
	return seriesArithmetic
}

// expressionSetSpec contains data needed for defining a expression-set Processor.
type expressionSpec struct {
	Expression string `yaml:"expression"`
	Result     string `yaml:"result"`

	Log *logrus.Logger `yaml:"-"`
}

// defaultExpressionSetSpec returns a expressionSetSpec with 'sensible' default values.
func defaultExpressionSpec() expressionSpec {
	return expressionSpec{}
}

func expressionProcessor(df *dataframe.DataFrame, config *Config) error {
	spec := defaultExpressionSpec()
	spec.Log = config.Log
	if err := config.TypeSpec.Decode(&spec); err != nil {
		return err
	}
	if len(spec.Expression) == 0 {
		return nil
	}
	if len(spec.Result) == 0 {
		return ErrResult
	}
	// map field names to columns
	names := df.Names()
	env := make(map[string]interface{}, len(names))
	for n := range names {
		env[names[n]] = df.Col(names[n]).Float()
		if df.Error() != nil {
			panic("todo: implement non float fields")
		}
	}
	// remap operations to work on slices/series.Series
	lang := gval.NewLanguage(
		gval.Arithmetic(),
		SeriesArithmetic(),
	)

	spec.Log.WithFields(logrus.Fields{
		"expression": spec.Expression,
		"result":     spec.Result}).
		Debug("applying expression")
	r, err := lang.Evaluate(spec.Expression, env)
	if err != nil {
		return err
	}
	rs := series.New(r, series.Float, spec.Result)
	*df = df.Mutate(rs)
	return errors.Join(df.Error())
}

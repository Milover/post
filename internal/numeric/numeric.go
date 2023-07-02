package numeric

import "math"

var (
	// Eps is the machine precision.
	Eps float64 = math.Nextafter(1.0, 2.0) - 1.0
)

// EqualEpsUlp checks the equality of two floating point values, up to
// ulp units in the last place, using eps as the precision.
func EqualEpsUlp(x, y, eps float64, ulp int) bool {
	// the machine epsilon has to be scaled to the magnitude of the values used
	// and multiplied by the desired precision in ULPs (units in the last place)
	return math.Abs(x-y) <= eps*math.Abs(x+y)*float64(ulp) ||
		// unless the result is subnormal
		math.Abs(x-y) < math.SmallestNonzeroFloat64
}

// EqualEps checks the equality of two floating point values, up to 1 unit in
// the last place, using eps as the precision.
func EqualEps(x, y, eps float64) bool {
	return EqualEpsUlp(x, y, eps, 1)
}

// Equal checks the equality of two floating point values, up to 1 unit in
// the last place, using machine precision.
func Equal(x, y float64) bool {
	return EqualEpsUlp(x, y, Eps, 1)
}

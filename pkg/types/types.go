// Package types defines the portable scalar value model shared by both the
// row-by-row and the vectorized execution paths. All NULL propagation,
// arithmetic and comparison semantics live here so both paths behave
// identically.
package types

import (
	"errors"
	"fmt"
	"math"
)

// T enumerates the supported scalar types.
type T uint8

const (
	Int64 T = iota
	Float64
	String
	Bool
)

func (t T) String() string {
	switch t {
	case Int64:
		return "int64"
	case Float64:
		return "float64"
	case String:
		return "string"
	case Bool:
		return "bool"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// ParseType parses a type name as it appears in schema definitions / CSV
// headers.
func ParseType(s string) (T, error) {
	switch s {
	case "int64", "int", "bigint":
		return Int64, nil
	case "float64", "float", "double":
		return Float64, nil
	case "string", "text", "varchar":
		return String, nil
	case "bool", "boolean":
		return Bool, nil
	default:
		return 0, fmt.Errorf("%w: unknown type %q", ErrTypeMismatch, s)
	}
}

// Distinguishable failure causes. Callers should use errors.Is to tell them
// apart.
var (
	// ErrTypeMismatch is returned when an operation receives a value of an
	// incompatible type (e.g. adding a string to an int).
	ErrTypeMismatch = errors.New("type mismatch")
	// ErrOverflow is returned when integer arithmetic overflows int64.
	ErrOverflow = errors.New("numeric overflow")
	// ErrDivByZero is returned on division by zero.
	ErrDivByZero = errors.New("division by zero")
)

// Datum is a nullable scalar value. The meaningful field depends on T.
type Datum struct {
	T    T
	Null bool
	I    int64
	F    float64
	S    string
	B    bool
}

func NewInt64(v int64) Datum   { return Datum{T: Int64, I: v} }
func NewFloat64(v float64) Datum { return Datum{T: Float64, F: v} }
func NewString(v string) Datum { return Datum{T: String, S: v} }
func NewBool(v bool) Datum     { return Datum{T: Bool, B: v} }
func NewNull(t T) Datum        { return Datum{T: t, Null: true} }

// Truthy returns the boolean truth value used by filter predicates. SQL
// three-valued logic: NULL and FALSE are both treated as "row does not pass".
func (d Datum) Truthy() bool {
	if d.Null || d.T != Bool {
		return false
	}
	return d.B
}

func (d Datum) String() string {
	if d.Null {
		return "NULL"
	}
	switch d.T {
	case Int64:
		return fmt.Sprintf("%d", d.I)
	case Float64:
		return fmt.Sprintf("%g", d.F)
	case String:
		return d.S
	case Bool:
		return fmt.Sprintf("%t", d.B)
	default:
		return "???"
	}
}

// Compare compares two values for ordering. NULLs sort before non-NULLs and
// NULL equals NULL (this is the grouping/sort convention; predicate evaluation
// uses three-valued logic separately). A type mismatch returns
// ErrTypeMismatch.
func Compare(a, b Datum) (int, error) {
	if a.T != b.T {
		return 0, fmt.Errorf("%w: compare %s with %s", ErrTypeMismatch, a.T, b.T)
	}
	if a.Null || b.Null {
		switch {
		case a.Null && b.Null:
			return 0, nil
		case a.Null:
			return -1, nil
		default:
			return 1, nil
		}
	}
	switch a.T {
	case Int64:
		switch {
		case a.I < b.I:
			return -1, nil
		case a.I > b.I:
			return 1, nil
		default:
			return 0, nil
		}
	case Float64:
		switch {
		case a.F < b.F:
			return -1, nil
		case a.F > b.F:
			return 1, nil
		default:
			return 0, nil
		}
	case String:
		switch {
		case a.S < b.S:
			return -1, nil
		case a.S > b.S:
			return 1, nil
		default:
			return 0, nil
		}
	case Bool:
		ai, bi := 0, 0
		if a.B {
			ai = 1
		}
		if b.B {
			bi = 1
		}
		switch {
		case ai < bi:
			return -1, nil
		case ai > bi:
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("%w: unsupported compare type %s", ErrTypeMismatch, a.T)
	}
}

// Equal reports whether two values are equal for grouping. NULL equals NULL so
// that NULL values form one group; NULL never equals a non-NULL.
func Equal(a, b Datum) (bool, error) {
	if a.T != b.T {
		return false, fmt.Errorf("%w: equal %s with %s", ErrTypeMismatch, a.T, b.T)
	}
	if a.Null || b.Null {
		return a.Null && b.Null, nil
	}
	switch a.T {
	case Int64:
		return a.I == b.I, nil
	case Float64:
		return a.F == b.F, nil
	case String:
		return a.S == b.S, nil
	case Bool:
		return a.B == b.B, nil
	default:
		return false, fmt.Errorf("%w: unsupported equal type %s", ErrTypeMismatch, a.T)
	}
}

// Add returns a+b. NULL propagates. Integer overflow returns ErrOverflow.
func Add(a, b Datum) (Datum, error) {
	if a.Null || b.Null {
		return NewNull(a.T), nil
	}
	if a.T != b.T {
		return Datum{}, fmt.Errorf("%w: add %s with %s", ErrTypeMismatch, a.T, b.T)
	}
	switch a.T {
	case Int64:
		if (b.I > 0 && a.I > math.MaxInt64-b.I) || (b.I < 0 && a.I < math.MinInt64-b.I) {
			return Datum{}, fmt.Errorf("%w: %d + %d", ErrOverflow, a.I, b.I)
		}
		return NewInt64(a.I + b.I), nil
	case Float64:
		return NewFloat64(a.F + b.F), nil
	case String:
		return NewString(a.S + b.S), nil
	default:
		return Datum{}, fmt.Errorf("%w: add unsupported for %s", ErrTypeMismatch, a.T)
	}
}

// Sub returns a-b. NULL propagates. Integer overflow returns ErrOverflow.
func Sub(a, b Datum) (Datum, error) {
	if a.Null || b.Null {
		return NewNull(a.T), nil
	}
	if a.T != b.T {
		return Datum{}, fmt.Errorf("%w: sub %s with %s", ErrTypeMismatch, a.T, b.T)
	}
	switch a.T {
	case Int64:
		if (b.I > 0 && a.I < math.MinInt64+b.I) || (b.I < 0 && a.I > math.MaxInt64+b.I) {
			return Datum{}, fmt.Errorf("%w: %d - %d", ErrOverflow, a.I, b.I)
		}
		return NewInt64(a.I - b.I), nil
	case Float64:
		return NewFloat64(a.F - b.F), nil
	default:
		return Datum{}, fmt.Errorf("%w: sub unsupported for %s", ErrTypeMismatch, a.T)
	}
}

// Mul returns a*b. NULL propagates. Integer overflow returns ErrOverflow.
func Mul(a, b Datum) (Datum, error) {
	if a.Null || b.Null {
		return NewNull(a.T), nil
	}
	if a.T != b.T {
		return Datum{}, fmt.Errorf("%w: mul %s with %s", ErrTypeMismatch, a.T, b.T)
	}
	switch a.T {
	case Int64:
		// The only product that overflows int64 but survives the division
		// check below is MinInt64 * -1 (wraps back to MinInt64).
		if (a.I == math.MinInt64 && b.I == -1) || (a.I == -1 && b.I == math.MinInt64) {
			return Datum{}, fmt.Errorf("%w: %d * %d", ErrOverflow, a.I, b.I)
		}
		res := a.I * b.I
		if b.I != 0 && res/b.I != a.I {
			return Datum{}, fmt.Errorf("%w: %d * %d", ErrOverflow, a.I, b.I)
		}
		return NewInt64(res), nil
	case Float64:
		return NewFloat64(a.F * b.F), nil
	default:
		return Datum{}, fmt.Errorf("%w: mul unsupported for %s", ErrTypeMismatch, a.T)
	}
}

// Div returns a/b. NULL propagates. Division by zero returns ErrDivByZero;
// integer overflow (MinInt64 / -1) returns ErrOverflow.
func Div(a, b Datum) (Datum, error) {
	if a.Null || b.Null {
		return NewNull(a.T), nil
	}
	if a.T != b.T {
		return Datum{}, fmt.Errorf("%w: div %s with %s", ErrTypeMismatch, a.T, b.T)
	}
	switch a.T {
	case Int64:
		if b.I == 0 {
			return Datum{}, fmt.Errorf("%w: %d / %d", ErrDivByZero, a.I, b.I)
		}
		if a.I == math.MinInt64 && b.I == -1 {
			return Datum{}, fmt.Errorf("%w: %d / %d", ErrOverflow, a.I, b.I)
		}
		return NewInt64(a.I / b.I), nil
	case Float64:
		if b.F == 0 {
			return Datum{}, fmt.Errorf("%w: %g / %g", ErrDivByZero, a.F, b.F)
		}
		return NewFloat64(a.F / b.F), nil
	default:
		return Datum{}, fmt.Errorf("%w: div unsupported for %s", ErrTypeMismatch, a.T)
	}
}

// Neg returns -a. NULL propagates.
func Neg(a Datum) (Datum, error) {
	if a.Null {
		return NewNull(a.T), nil
	}
	switch a.T {
	case Int64:
		if a.I == math.MinInt64 {
			return Datum{}, fmt.Errorf("%w: neg %d", ErrOverflow, a.I)
		}
		return NewInt64(-a.I), nil
	case Float64:
		return NewFloat64(-a.F), nil
	default:
		return Datum{}, fmt.Errorf("%w: neg unsupported for %s", ErrTypeMismatch, a.T)
	}
}

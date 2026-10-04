package executor

import (
	"errors"
	"math"
	"math/bits"
)

// Type enumerates the supported column/value types.
type Type int

const (
	TypeInt64 Type = iota
	TypeFloat64
	TypeString
	TypeBool
)

func (t Type) String() string {
	switch t {
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	}
	return "unknown"
}

// Distinguishable failure reasons.
var (
	ErrTypeMismatch = errors.New("executor: type mismatch")
	ErrOverflow     = errors.New("executor: numeric overflow")
	ErrDivByZero    = errors.New("executor: division by zero")
	ErrSchema       = errors.New("executor: column index out of range")
	ErrMemoryLimit  = errors.New("executor: memory limit exceeded")
	ErrCanceled     = errors.New("executor: execution canceled")
	ErrTimeout      = errors.New("executor: execution deadline exceeded")
)

// Value is a single typed datum. Null=true means SQL NULL; other fields are
// then ignored.
type Value struct {
	Typ  Type
	Null bool
	I64  int64
	F64  float64
	Str  string
	B    bool
}

func NullOf(t Type) Value      { return Value{Typ: t, Null: true} }
func IntVal(v int64) Value     { return Value{Typ: TypeInt64, I64: v} }
func FloatVal(v float64) Value { return Value{Typ: TypeFloat64, F64: v} }
func StringVal(s string) Value { return Value{Typ: TypeString, Str: s} }
func BoolVal(b bool) Value     { return Value{Typ: TypeBool, B: b} }

func isNumeric(t Type) bool { return t == TypeInt64 || t == TypeFloat64 }

// asFloat converts a numeric value to float64.
func (v Value) asFloat() float64 {
	if v.Typ == TypeInt64 {
		return float64(v.I64)
	}
	return v.F64
}

// Compare returns -1, 0 or 1. NULL sorts before any non-NULL; two NULLs are
// equal. Numeric types are mutually comparable by promotion; any other type
// pairing yields ErrTypeMismatch.
func Compare(a, b Value) (int, error) {
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
	if isNumeric(a.Typ) && isNumeric(b.Typ) {
		if a.Typ == TypeInt64 && b.Typ == TypeInt64 {
			switch {
			case a.I64 < b.I64:
				return -1, nil
			case a.I64 > b.I64:
				return 1, nil
			}
			return 0, nil
		}
		fa, fb := a.asFloat(), b.asFloat()
		switch {
		case fa < fb:
			return -1, nil
		case fa > fb:
			return 1, nil
		}
		return 0, nil
	}
	if a.Typ != b.Typ {
		return 0, ErrTypeMismatch
	}
	switch a.Typ {
	case TypeString:
		switch {
		case a.Str < b.Str:
			return -1, nil
		case a.Str > b.Str:
			return 1, nil
		}
		return 0, nil
	case TypeBool:
		switch {
		case a.B == b.B:
			return 0, nil
		case !a.B:
			return -1, nil
		}
		return 1, nil
	}
	return 0, ErrTypeMismatch
}

// numericArith dispatches arithmetic with NULL propagation, int64 overflow
// checking and float promotion.
func numericArith(a, b Value, intOp func(x, y int64) (int64, error), floatOp func(x, y float64) (float64, error)) (Value, error) {
	if !isNumeric(a.Typ) || !isNumeric(b.Typ) {
		return Value{}, ErrTypeMismatch
	}
	if a.Null || b.Null {
		if a.Typ == TypeFloat64 || b.Typ == TypeFloat64 {
			return NullOf(TypeFloat64), nil
		}
		return NullOf(TypeInt64), nil
	}
	if a.Typ == TypeInt64 && b.Typ == TypeInt64 {
		r, err := intOp(a.I64, b.I64)
		if err != nil {
			return Value{}, err
		}
		return IntVal(r), nil
	}
	r, err := floatOp(a.asFloat(), b.asFloat())
	if err != nil {
		return Value{}, err
	}
	return FloatVal(r), nil
}

// Add: NULL propagates; int+int checked for overflow; float promotes.
func Add(a, b Value) (Value, error) {
	return numericArith(a, b,
		func(x, y int64) (int64, error) {
			r := x + y
			if (r^x)&(r^y) < 0 {
				return 0, ErrOverflow
			}
			return r, nil
		},
		func(x, y float64) (float64, error) { return x + y, nil })
}

// Sub: NULL propagates; int-int checked for overflow; float promotes.
func Sub(a, b Value) (Value, error) {
	return numericArith(a, b,
		func(x, y int64) (int64, error) {
			r := x - y
			if (x^y) < 0 && (r^x) < 0 {
				return 0, ErrOverflow
			}
			return r, nil
		},
		func(x, y float64) (float64, error) { return x - y, nil })
}

// Mul: NULL propagates; int*int checked for overflow; float promotes.
func Mul(a, b Value) (Value, error) {
	return numericArith(a, b,
		func(x, y int64) (int64, error) {
			hi, lo := bits.Mul64(uint64(x), uint64(y))
			// Adjust the unsigned high word to the signed 128-bit product:
			// hi_signed = hi - (x<0 ? y : 0) - (y<0 ? x : 0).
			if x < 0 {
				hi -= uint64(y)
			}
			if y < 0 {
				hi -= uint64(x)
			}
			r := int64(lo)
			// no overflow iff hi is the sign extension of r
			if int64(hi) != r>>63 {
				return 0, ErrOverflow
			}
			return r, nil
		},
		func(x, y float64) (float64, error) { return x * y, nil })
}

// Div: NULL propagates; int/int truncates toward zero; division by zero
// yields ErrDivByZero on both int and float paths.
func Div(a, b Value) (Value, error) {
	return numericArith(a, b,
		func(x, y int64) (int64, error) {
			if y == 0 {
				return 0, ErrDivByZero
			}
			if x == math.MinInt64 && y == -1 {
				return 0, ErrOverflow
			}
			return x / y, nil
		},
		func(x, y float64) (float64, error) {
			if y == 0 {
				return 0, ErrDivByZero
			}
			return x / y, nil
		})
}

// Neg negates a numeric value; NULL propagates; MinInt64 negation overflows.
func Neg(a Value) (Value, error) {
	if !isNumeric(a.Typ) {
		return Value{}, ErrTypeMismatch
	}
	if a.Null {
		return NullOf(a.Typ), nil
	}
	if a.Typ == TypeInt64 {
		if a.I64 == math.MinInt64 {
			return Value{}, ErrOverflow
		}
		return IntVal(-a.I64), nil
	}
	return FloatVal(-a.F64), nil
}

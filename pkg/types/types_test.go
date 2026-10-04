package types

import (
	"errors"
	"math"
	"testing"
)

func TestCompare(t *testing.T) {
	cases := []struct {
		a, b Datum
		want int
	}{
		{NewInt64(1), NewInt64(2), -1},
		{NewInt64(2), NewInt64(1), 1},
		{NewInt64(1), NewInt64(1), 0},
		{NewFloat64(1.5), NewFloat64(2.5), -1},
		{NewString("a"), NewString("b"), -1},
		{NewBool(false), NewBool(true), -1},
		// NULLs sort before non-NULLs; NULL equals NULL.
		{NewNull(Int64), NewNull(Int64), 0},
		{NewNull(Int64), NewInt64(1), -1},
		{NewInt64(1), NewNull(Int64), 1},
	}
	for _, c := range cases {
		got, err := Compare(c.a, c.b)
		if err != nil {
			t.Fatalf("Compare(%s,%s): %v", c.a, c.b, err)
		}
		if got != c.want {
			t.Errorf("Compare(%s,%s) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestCompareTypeMismatch(t *testing.T) {
	_, err := Compare(NewInt64(1), NewFloat64(1))
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestEqualNullGrouping(t *testing.T) {
	// NULL equals NULL for grouping purposes.
	eq, err := Equal(NewNull(String), NewNull(String))
	if err != nil || !eq {
		t.Errorf("NULL = NULL should be true for grouping, got %v, %v", eq, err)
	}
	eq, err = Equal(NewNull(String), NewString("a"))
	if err != nil || eq {
		t.Errorf("NULL = 'a' should be false, got %v, %v", eq, err)
	}
}

func TestArithmeticNullPropagation(t *testing.T) {
	// NULL + 1 = NULL
	d, err := Add(NewNull(Int64), NewInt64(1))
	if err != nil || !d.Null {
		t.Errorf("NULL + 1 should be NULL, got %v, %v", d, err)
	}
	// 1 + NULL = NULL
	d, err = Add(NewInt64(1), NewNull(Int64))
	if err != nil || !d.Null {
		t.Errorf("1 + NULL should be NULL, got %v, %v", d, err)
	}
	// NULL * NULL = NULL
	d, err = Mul(NewNull(Int64), NewNull(Int64))
	if err != nil || !d.Null {
		t.Errorf("NULL * NULL should be NULL, got %v, %v", d, err)
	}
}

func TestArithmeticOverflow(t *testing.T) {
	_, err := Add(NewInt64(math.MaxInt64), NewInt64(1))
	if !errors.Is(err, ErrOverflow) {
		t.Errorf("MaxInt64 + 1 should overflow, got %v", err)
	}
	_, err = Sub(NewInt64(math.MinInt64), NewInt64(1))
	if !errors.Is(err, ErrOverflow) {
		t.Errorf("MinInt64 - 1 should overflow, got %v", err)
	}
	_, err = Mul(NewInt64(math.MaxInt64), NewInt64(2))
	if !errors.Is(err, ErrOverflow) {
		t.Errorf("MaxInt64 * 2 should overflow, got %v", err)
	}
	_, err = Div(NewInt64(math.MinInt64), NewInt64(-1))
	if !errors.Is(err, ErrOverflow) {
		t.Errorf("MinInt64 / -1 should overflow, got %v", err)
	}
}

func TestDivByZero(t *testing.T) {
	_, err := Div(NewInt64(1), NewInt64(0))
	if !errors.Is(err, ErrDivByZero) {
		t.Errorf("1/0 should be div by zero, got %v", err)
	}
	_, err = Div(NewFloat64(1.0), NewFloat64(0.0))
	if !errors.Is(err, ErrDivByZero) {
		t.Errorf("1.0/0.0 should be div by zero, got %v", err)
	}
}

func TestArithmeticTypeMismatch(t *testing.T) {
	_, err := Add(NewInt64(1), NewFloat64(1.0))
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("int64 + float64 should be type mismatch, got %v", err)
	}
}

func TestTruthy(t *testing.T) {
	if NewBool(true).Truthy() != true {
		t.Error("true should be truthy")
	}
	if NewBool(false).Truthy() != false {
		t.Error("false should not be truthy")
	}
	if NewNull(Bool).Truthy() != false {
		t.Error("NULL should not be truthy")
	}
	if NewInt64(1).Truthy() != false {
		t.Error("int64 should not be truthy")
	}
}

func TestParseType(t *testing.T) {
	cases := map[string]T{
		"int64": Int64, "int": Int64, "bigint": Int64,
		"float64": Float64, "float": Float64, "double": Float64,
		"string": String, "text": String, "varchar": String,
		"bool": Bool, "boolean": Bool,
	}
	for s, want := range cases {
		got, err := ParseType(s)
		if err != nil || got != want {
			t.Errorf("ParseType(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	_, err := ParseType("unknown")
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("ParseType(unknown) should be type mismatch, got %v", err)
	}
}

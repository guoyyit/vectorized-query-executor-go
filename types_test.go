package executor

import (
	"errors"
	"math"
	"testing"
)

func TestMulOverflowCheck(t *testing.T) {
	ok := [][2]int64{
		{3, 5}, {-3, 5}, {3, -5}, {-3, -5}, {0, math.MaxInt64},
		{math.MinInt64, 0}, {1, math.MinInt64}, {-1, math.MaxInt64},
		{3037000499, 3037000499}, {-3037000499, 3037000499},
	}
	for _, c := range ok {
		if _, err := Mul(IntVal(c[0]), IntVal(c[1])); err != nil {
			t.Fatalf("Mul(%d,%d) unexpected error: %v", c[0], c[1], err)
		}
	}
	bad := [][2]int64{
		{math.MaxInt64, 2}, {math.MinInt64, -1}, {math.MinInt64, 2},
		{3037000500, 3037000500}, {-3037000500, 3037000500},
		{math.MaxInt64, math.MaxInt64}, {math.MinInt64, math.MinInt64},
	}
	for _, c := range bad {
		if _, err := Mul(IntVal(c[0]), IntVal(c[1])); !errors.Is(err, ErrOverflow) {
			t.Fatalf("Mul(%d,%d) expected overflow, got %v", c[0], c[1], err)
		}
	}
}

func TestAddSubOverflowCheck(t *testing.T) {
	if _, err := Add(IntVal(math.MaxInt64), IntVal(1)); !errors.Is(err, ErrOverflow) {
		t.Fatal("add overflow not detected")
	}
	if _, err := Add(IntVal(math.MinInt64), IntVal(-1)); !errors.Is(err, ErrOverflow) {
		t.Fatal("add negative overflow not detected")
	}
	if _, err := Sub(IntVal(math.MinInt64), IntVal(1)); !errors.Is(err, ErrOverflow) {
		t.Fatal("sub overflow not detected")
	}
	if _, err := Sub(IntVal(math.MaxInt64), IntVal(-1)); !errors.Is(err, ErrOverflow) {
		t.Fatal("sub overflow not detected")
	}
	if v, err := Add(IntVal(math.MaxInt64), IntVal(-1)); err != nil || v.I64 != math.MaxInt64-1 {
		t.Fatal("valid add failed")
	}
	if v, err := Sub(IntVal(math.MinInt64), IntVal(-1)); err != nil || v.I64 != math.MinInt64+1 {
		t.Fatal("valid sub failed")
	}
}

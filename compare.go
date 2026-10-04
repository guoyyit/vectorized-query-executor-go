package executor

import (
	"context"
	"errors"
	"fmt"
)

// CrossCheck executes the plan on both the row-at-a-time and the vectorized
// path and verifies the results match row by row. On success it returns the
// (identical) result. If the paths disagree it returns a descriptive error.
// If both paths fail with the same sentinel error the execution is
// considered consistent and that error is returned.
func CrossCheck(ctx context.Context, t *Table, p *Plan, o Options) ([]Row, error) {
	rowRes, rowErr := ExecuteRow(ctx, t, p, o)
	vecRes, vecErr := ExecuteVec(ctx, t, p, o)
	if rowErr != nil || vecErr != nil {
		if rowErr != nil && vecErr != nil && sameFailure(rowErr, vecErr) {
			return nil, rowErr
		}
		return nil, fmt.Errorf("executor: path divergence: row path err=%v, vectorized path err=%v", rowErr, vecErr)
	}
	if !EqualRows(rowRes, vecRes) {
		return nil, fmt.Errorf("executor: path divergence: %w", DiffRows(rowRes, vecRes))
	}
	return rowRes, nil
}

// sameFailure reports whether two errors carry the same sentinel reason.
func sameFailure(a, b error) bool {
	for _, s := range []error{ErrTypeMismatch, ErrOverflow, ErrDivByZero, ErrSchema, ErrMemoryLimit, ErrCanceled, ErrTimeout} {
		if errors.Is(a, s) || errors.Is(b, s) {
			return errors.Is(a, s) && errors.Is(b, s)
		}
	}
	return a.Error() == b.Error()
}

// EqualRows reports whether a and b contain identical rows in order.
// Two NULLs are equal regardless of their declared type.
func EqualRows(a, b []Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalRow(a[i], b[i]) {
			return false
		}
	}
	return true
}

func equalRow(a, b Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !EqualValue(a[i], b[i]) {
			return false
		}
	}
	return true
}

// EqualValue reports value equality consistent with Compare for non-NULLs.
func EqualValue(a, b Value) bool {
	if a.Null || b.Null {
		return a.Null && b.Null
	}
	if a.Typ != b.Typ {
		return false
	}
	switch a.Typ {
	case TypeInt64:
		return a.I64 == b.I64
	case TypeFloat64:
		return a.F64 == b.F64
	case TypeString:
		return a.Str == b.Str
	case TypeBool:
		return a.B == b.B
	}
	return false
}

// DiffRows describes the first difference between two row sets.
func DiffRows(a, b []Row) error {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if !equalRow(a[i], b[i]) {
			return fmt.Errorf("row %d differs: %v vs %v", i, a[i], b[i])
		}
	}
	if len(a) != len(b) {
		return fmt.Errorf("row count differs: %d vs %d", len(a), len(b))
	}
	return nil
}

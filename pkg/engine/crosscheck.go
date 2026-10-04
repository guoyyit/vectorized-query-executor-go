package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

// floatTolerance is the maximum relative error allowed when comparing float64
// results across the two paths. Float addition is not associative: the
// row-by-row path accumulates in scan order while the vectorized path merges
// per-partition partial sums, so float sums may differ by one ULP. Integer
// results are compared exactly.
const floatTolerance = 1e-9

// CheckResult describes the outcome of a cross-path validation.
type CheckResult struct {
	Match            bool
	SchemaMatch      bool
	RowRows          int
	VecRows          int
	FirstMismatchRow int
	Detail           string
}

// CrossCheck runs the same plan through both the row-by-row and the vectorized
// paths and compares the results row by row. It is a regression guard: the two
// paths must agree on schema, cardinality and every datum. Integer values are
// compared exactly; float64 values are compared within a relative tolerance
// (see floatTolerance) because concurrent partial-sum merging may shift the
// last ULP.
func CrossCheck(ctx context.Context, cfg Config, plan *Plan) (*CheckResult, error) {
	rr, err := ExecuteRow(ctx, cfg, plan)
	if err != nil {
		return nil, fmt.Errorf("row path: %w", err)
	}
	vr, err := ExecuteVector(ctx, cfg, plan)
	if err != nil {
		return nil, fmt.Errorf("vector path: %w", err)
	}

	res := &CheckResult{
		RowRows:          len(rr.Rows),
		VecRows:          len(vr.Rows),
		FirstMismatchRow: -1,
	}

	if len(rr.Schema) != len(vr.Schema) {
		res.Detail = fmt.Sprintf("schema width mismatch: row path %d columns, vector path %d columns", len(rr.Schema), len(vr.Schema))
		return res, nil
	}
	res.SchemaMatch = true
	for i := range rr.Schema {
		if rr.Schema[i] != vr.Schema[i] {
			res.SchemaMatch = false
			res.Detail = fmt.Sprintf("column %d type mismatch: row path %s, vector path %s", i, rr.Schema[i], vr.Schema[i])
			return res, nil
		}
	}

	if len(rr.Rows) != len(vr.Rows) {
		res.Detail = fmt.Sprintf("row count mismatch: row path %d, vector path %d", len(rr.Rows), len(vr.Rows))
		return res, nil
	}

	for i := 0; i < len(rr.Rows); i++ {
		r, v := rr.Rows[i], vr.Rows[i]
		if len(r.Vals) != len(v.Vals) {
			res.FirstMismatchRow = i
			res.Detail = fmt.Sprintf("row %d width mismatch: %d vs %d", i, len(r.Vals), len(v.Vals))
			return res, nil
		}
		for j := 0; j < len(r.Vals); j++ {
			if !datumsEqual(r.Vals[j], v.Vals[j]) {
				res.FirstMismatchRow = i
				res.Detail = fmt.Sprintf("row %d column %d: row path %s, vector path %s", i, j, r.Vals[j], v.Vals[j])
				return res, nil
			}
		}
	}

	res.Match = true
	return res, nil
}

// datumsEqual compares two datums. Integers, strings, bools and NULLs are
// compared exactly; float64 values are compared within floatTolerance.
func datumsEqual(a, b types.Datum) bool {
	if a.T != b.T {
		return false
	}
	if a.Null || b.Null {
		return a.Null && b.Null
	}
	switch a.T {
	case types.Float64:
		if a.F == b.F {
			return true
		}
		// Relative tolerance for ULP-level rounding differences.
		diff := math.Abs(a.F - b.F)
		scale := math.Max(math.Abs(a.F), math.Abs(b.F))
		if scale == 0 {
			return diff == 0
		}
		return diff/scale < floatTolerance
	default:
		eq, err := types.Equal(a, b)
		if err != nil {
			return false
		}
		return eq
	}
}

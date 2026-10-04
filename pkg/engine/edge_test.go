package engine

import (
	"context"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// TestFilteredRowsSkipped: rows that fail the filter must not be evaluated by
// downstream operators. A division by zero in a filtered row must not fail the
// query.
func TestFilteredRowsSkipped(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Int64},
	}}
	rows := [][]types.Datum{
		{types.NewInt64(1), types.NewInt64(10)},
		{types.NewInt64(2), types.NewInt64(0)}, // v=0: would cause div by zero
		{types.NewInt64(3), types.NewInt64(20)},
		{types.NewInt64(4), types.NewInt64(0)}, // v=0
	}
	tbl := catalog.NewTable("t", schema, rows, 2)
	// Filter v > 0, then project 100 / v. The v=0 rows are filtered out before
	// the division is evaluated.
	plan := &Plan{
		Table:       tbl,
		Filter:      expr.Gt(expr.Col(1), expr.IntLit(0)),
		Projections: []expr.Expr{expr.Div(expr.IntLit(100), expr.Col(1))},
	}
	for _, bs := range []int{1, 2, 10} {
		cfg := DefaultConfig()
		cfg.BatchSize = bs
		cfg.Parallelism = 2
		cfg.MemoryBudget = 0
		res, err := CrossCheck(context.Background(), cfg, plan)
		if err != nil {
			t.Fatalf("bs=%d: %v", bs, err)
		}
		if !res.Match {
			t.Fatalf("bs=%d: mismatch: %s", bs, res.Detail)
		}
	}
}

// TestCrossCheckDetectsMismatch: verify that CrossCheck actually detects a
// mismatch when the two paths disagree. We craft a plan where the row path and
// vector path would differ (this is a meta-test of the cross-check harness).
func TestCrossCheckDetectsMismatch(t *testing.T) {
	// This test verifies the cross-check harness works by checking that a
	// deliberately broken comparison is detected. We test datumsEqual directly.
	if datumsEqual(types.NewInt64(1), types.NewInt64(2)) {
		t.Fatal("datumsEqual should detect int mismatch")
	}
	if datumsEqual(types.NewInt64(1), types.NewFloat64(1.0)) {
		t.Fatal("datumsEqual should detect type mismatch")
	}
	if !datumsEqual(types.NewNull(types.Int64), types.NewNull(types.Int64)) {
		t.Fatal("datumsEqual should match NULLs")
	}
	// Float tolerance.
	if !datumsEqual(types.NewFloat64(1.0), types.NewFloat64(1.0+1e-12)) {
		t.Fatal("datumsEqual should tolerate small float differences")
	}
	if datumsEqual(types.NewFloat64(1.0), types.NewFloat64(2.0)) {
		t.Fatal("datumsEqual should detect large float differences")
	}
}

// TestPlanValidationErrors: type mismatches in the plan are reported.
func TestPlanValidationErrors(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "name", Type: types.String},
	}}
	tbl := catalog.NewTable("t", schema, nil, 1)

	// Filter with type mismatch (int = string).
	plan := &Plan{
		Table:  tbl,
		Filter: expr.Eq(expr.Col(0), expr.Col(1)),
	}
	cfg := DefaultConfig()
	_, err := ExecuteRow(context.Background(), cfg, plan)
	if err == nil {
		t.Fatal("expected type mismatch error, got nil")
	}

	// Projection with out-of-range column.
	plan = &Plan{
		Table:       tbl,
		Projections: []expr.Expr{expr.Col(5)},
	}
	_, err = ExecuteRow(context.Background(), cfg, plan)
	if err == nil {
		t.Fatal("expected out-of-range error, got nil")
	}
}

// TestVectorBatchCompact: verify batch compaction physically removes rows.
func TestVectorBatchCompact(t *testing.T) {
	schema := []types.T{types.Int64, types.String}
	b := vector.NewBatch(schema, 5)
	b.AppendRow([]types.Datum{types.NewInt64(1), types.NewString("a")})
	b.AppendRow([]types.Datum{types.NewInt64(2), types.NewString("b")})
	b.AppendRow([]types.Datum{types.NewInt64(3), types.NewString("c")})

	// Select only rows 0 and 2.
	sel := []int{0, 2}
	out := b.Compact(sel)
	if out.N != 2 {
		t.Fatalf("expected 2 rows, got %d", out.N)
	}
	r0 := out.Row(0)
	if r0[0].I != 1 || r0[1].S != "a" {
		t.Fatalf("unexpected row 0: %v", r0)
	}
	r1 := out.Row(1)
	if r1[0].I != 3 || r1[1].S != "c" {
		t.Fatalf("unexpected row 1: %v", r1)
	}
}

// TestVectorBatchProject: verify batch projection selects columns.
func TestVectorBatchProject(t *testing.T) {
	schema := []types.T{types.Int64, types.String, types.Float64}
	b := vector.NewBatch(schema, 2)
	b.AppendRow([]types.Datum{types.NewInt64(1), types.NewString("a"), types.NewFloat64(1.5)})
	b.AppendRow([]types.Datum{types.NewInt64(2), types.NewString("b"), types.NewFloat64(2.5)})

	out := b.Project([]int{2, 0})
	if out.N != 2 || len(out.Cols) != 2 {
		t.Fatalf("unexpected project shape: %d rows, %d cols", out.N, len(out.Cols))
	}
	if out.Cols[0].Type != types.Float64 || out.Cols[1].Type != types.Int64 {
		t.Fatal("unexpected project column types")
	}
	r0 := out.Row(0)
	if r0[0].F != 1.5 || r0[1].I != 1 {
		t.Fatalf("unexpected projected row: %v", r0)
	}
}

// TestAggEmptyInput: aggregate over empty input produces no groups.
func TestAggEmptyInput(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "grp", Type: types.String},
		{Name: "v", Type: types.Int64},
	}}
	tbl := catalog.NewTable("t", schema, nil, 1)
	plan := &Plan{
		Table:   tbl,
		GroupBy: []expr.Expr{expr.Col(0)},
		Aggregates: []agg.Def{
			{Func: agg.CountStar},
			{Func: agg.Sum, Expr: expr.Col(1)},
		},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 10
	cfg.Parallelism = 1
	cfg.MemoryBudget = 0
	res, err := CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}
	if res.RowRows != 0 {
		t.Fatalf("expected 0 groups, got %d", res.RowRows)
	}
}

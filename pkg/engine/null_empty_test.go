package engine

import (
	"context"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

// nullTable builds a table with NULLs in every column type.
func nullTable() *catalog.Table {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "name", Type: types.String},
		{Name: "score", Type: types.Float64},
		{Name: "active", Type: types.Bool},
	}}
	rows := [][]types.Datum{
		{types.NewInt64(1), types.NewString("a"), types.NewFloat64(1.5), types.NewBool(true)},
		{types.NewNull(types.Int64), types.NewString("b"), types.NewFloat64(2.5), types.NewBool(false)},
		{types.NewInt64(3), types.NewNull(types.String), types.NewFloat64(3.5), types.NewBool(true)},
		{types.NewInt64(4), types.NewString("d"), types.NewNull(types.Float64), types.NewBool(false)},
		{types.NewInt64(5), types.NewString("e"), types.NewFloat64(5.5), types.NewNull(types.Bool)},
		{types.NewNull(types.Int64), types.NewNull(types.String), types.NewNull(types.Float64), types.NewNull(types.Bool)},
	}
	return catalog.NewTable("t", schema, rows, 2)
}

// TestNullComparison: NULL in comparison yields NULL, which filters out.
func TestNullComparison(t *testing.T) {
	tbl := nullTable()
	plan := &Plan{
		Table:  tbl,
		Filter: expr.Gt(expr.Col(2), expr.FloatLit(2.0)),
		Projections: []expr.Expr{
			expr.Col(0), expr.Col(1), expr.Col(2),
		},
	}
	for _, bs := range []int{1, 2, 3, 10} {
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

// TestNullArithmetic: NULL propagates through arithmetic.
func TestNullArithmetic(t *testing.T) {
	tbl := nullTable()
	plan := &Plan{
		Table: tbl,
		Projections: []expr.Expr{
			expr.Col(0),
			expr.Add(expr.Col(2), expr.FloatLit(10.0)),
			expr.Mul(expr.Col(2), expr.FloatLit(2.0)),
		},
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

// TestNullAggregate: NULLs are ignored by aggregates; NULL group forms its own
// group.
func TestNullAggregate(t *testing.T) {
	tbl := nullTable()
	plan := &Plan{
		Table:   tbl,
		GroupBy: []expr.Expr{expr.Col(1)},
		Aggregates: []agg.Def{
			{Func: agg.CountStar},
			{Func: agg.Count, Expr: expr.Col(2)},
			{Func: agg.Sum, Expr: expr.Col(2)},
			{Func: agg.Avg, Expr: expr.Col(2)},
			{Func: agg.Min, Expr: expr.Col(2)},
			{Func: agg.Max, Expr: expr.Col(2)},
		},
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

// TestEmptyBatch: batch size larger than the table produces empty final
// batches; empty table and filter-matches-nothing are also exercised.
func TestEmptyBatch(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Int64},
	}}
	rows := [][]types.Datum{
		{types.NewInt64(1), types.NewInt64(10)},
		{types.NewInt64(2), types.NewInt64(20)},
		{types.NewInt64(3), types.NewInt64(30)},
	}
	tbl := catalog.NewTable("t", schema, rows, 1)

	plan := &Plan{
		Table:  tbl,
		Filter: expr.Gt(expr.Col(1), expr.IntLit(0)),
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 1000
	cfg.Parallelism = 1
	cfg.MemoryBudget = 0
	res, err := CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}

	// Filter matches nothing: empty result.
	plan = &Plan{
		Table:  tbl,
		Filter: expr.Gt(expr.Col(1), expr.IntLit(1000)),
	}
	res, err = CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}
	if res.RowRows != 0 || res.VecRows != 0 {
		t.Fatalf("expected empty result, got row=%d vec=%d", res.RowRows, res.VecRows)
	}

	// Empty table.
	emptyTbl := catalog.NewTable("empty", schema, nil, 1)
	plan = &Plan{
		Table:  emptyTbl,
		Filter: expr.Gt(expr.Col(1), expr.IntLit(0)),
	}
	res, err = CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}
}

// TestEmptyBatchContinues: when a whole batch is filtered out, the pipeline
// must continue (not stall). Verified by running sort/agg after a filter that
// produces only empty batches.
func TestEmptyBatchContinues(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Int64},
	}}
	rows := make([][]types.Datum, 100)
	for i := range rows {
		rows[i] = []types.Datum{types.NewInt64(int64(i)), types.NewInt64(int64(i % 5))}
	}
	tbl := catalog.NewTable("t", schema, rows, 4)

	// Sort after filter that matches nothing: must produce empty output and
	// not hang.
	plan := &Plan{
		Table:   tbl,
		Filter:  expr.Gt(expr.Col(1), expr.IntLit(5)),
		OrderBy: []vexec.SortKey{{Expr: expr.Col(0)}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 7
	cfg.Parallelism = 2
	cfg.MemoryBudget = 0
	res, err := CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}

	// Agg after filter that matches nothing: must produce empty output.
	plan = &Plan{
		Table:   tbl,
		Filter:  expr.Gt(expr.Col(1), expr.IntLit(5)),
		GroupBy: []expr.Expr{expr.Col(1)},
		Aggregates: []agg.Def{
			{Func: agg.CountStar},
		},
	}
	res, err = CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}
}

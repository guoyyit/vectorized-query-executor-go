package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

// TestSortSpill: with a very small memory budget, the sort must spill to disk
// and still produce correct results matching the row-by-row path.
func TestSortSpill(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Int64},
	}}
	tbl := catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows: 2000, NumPartitions: 4, Seed: 321, NullFrac: 0.05, IntRange: 10000,
	})
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1), Desc: false}},
	}
	for _, budget := range []int64{1, 4096, 64 << 20} {
		cfg := DefaultConfig()
		cfg.BatchSize = 100
		cfg.Parallelism = 2
		cfg.MemoryBudget = budget
		res, err := CrossCheck(context.Background(), cfg, plan)
		if err != nil {
			t.Fatalf("budget=%d: %v", budget, err)
		}
		if !res.Match {
			t.Fatalf("budget=%d: mismatch: %s", budget, res.Detail)
		}
	}
}

// TestSortSpillDesc: descending sort with spill.
func TestSortSpillDesc(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Int64},
	}}
	tbl := catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows: 1500, NumPartitions: 3, Seed: 654, NullFrac: 0.05, IntRange: 10000,
	})
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1), Desc: true}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 50
	cfg.Parallelism = 2
	cfg.MemoryBudget = 2048
	res, err := CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}
}

// TestAggQuotaExceeded: with a tiny memory budget, the aggregate must fail
// with a distinguishable error (not silently drop data).
func TestAggQuotaExceeded(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "grp", Type: types.String},
		{Name: "v", Type: types.Int64},
	}}
	// Many distinct groups to exceed the tiny budget.
	rows := make([][]types.Datum, 500)
	for i := range rows {
		rows[i] = []types.Datum{
			types.NewString(string(rune('a' + i%26))),
			types.NewInt64(int64(i)),
		}
	}
	tbl := catalog.NewTable("t", schema, rows, 2)
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
	cfg.Parallelism = 2
	cfg.MemoryBudget = 1 // 1 byte: every group reservation fails
	_, err := ExecuteVector(context.Background(), cfg, plan)
	if err == nil {
		t.Fatal("expected memory quota error, got nil")
	}
	if !errors.Is(err, vexec.ErrMemoryQuotaExceeded) {
		t.Fatalf("expected ErrMemoryQuotaExceeded, got %v", err)
	}
}

// TestAggQuotaOk: with a sufficient budget, the aggregate succeeds.
func TestAggQuotaOk(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "grp", Type: types.String},
		{Name: "v", Type: types.Int64},
	}}
	rows := make([][]types.Datum, 500)
	for i := range rows {
		rows[i] = []types.Datum{
			types.NewString(string(rune('a' + i%26))),
			types.NewInt64(int64(i)),
		}
	}
	tbl := catalog.NewTable("t", schema, rows, 2)
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
	cfg.Parallelism = 2
	cfg.MemoryBudget = 64 << 20
	res, err := CrossCheck(context.Background(), cfg, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Match {
		t.Fatalf("mismatch: %s", res.Detail)
	}
}

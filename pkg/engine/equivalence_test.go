package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

func eqTable() *catalog.Table {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "name", Type: types.String},
		{Name: "score", Type: types.Float64},
		{Name: "active", Type: types.Bool},
	}}
	return catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows:       1500,
		NumPartitions: 4,
		Seed:          123,
		NullFrac:      0.1,
		StringCard:    15,
	})
}

// allRows flattens a table's partitions into a single slice.
func allRows(tbl *catalog.Table) [][]types.Datum {
	var rows [][]types.Datum
	for _, p := range tbl.Parts {
		rows = append(rows, p...)
	}
	return rows
}

// TestEquivalenceSort: filter + project + sort across all batch sizes,
// partition counts and parallelism degrees.
func TestEquivalenceSort(t *testing.T) {
	tbl := eqTable()
	base := &Plan{
		Filter: expr.Gt(expr.Col(2), expr.FloatLit(300)),
		Projections: []expr.Expr{
			expr.Col(0), expr.Col(1), expr.Col(2),
		},
		OrderBy: []vexec.SortKey{
			{Expr: expr.Col(2), Desc: true},
			{Expr: expr.Col(0), Desc: false},
		},
	}
	batchSizes := []int{1, 100, 1024}
	partCounts := []int{1, 4, 8}
	parallelisms := []int{1, 4}
	for _, bs := range batchSizes {
		for _, parts := range partCounts {
			for _, par := range parallelisms {
				t.Run(fmt.Sprintf("bs=%d/parts=%d/par=%d", bs, parts, par), func(t *testing.T) {
					t2 := catalog.NewTable("t", tbl.Schema, allRows(tbl), parts)
					plan := *base
					plan.Table = t2
					cfg := DefaultConfig()
					cfg.BatchSize = bs
					cfg.Parallelism = par
					cfg.MemoryBudget = 0
					res, err := CrossCheck(context.Background(), cfg, &plan)
					if err != nil {
						t.Fatalf("CrossCheck: %v", err)
					}
					if !res.Match {
						t.Fatalf("mismatch: %s", res.Detail)
					}
				})
			}
		}
	}
}

// TestEquivalenceAgg: filter + aggregate across all batch sizes, partition
// counts and parallelism degrees.
func TestEquivalenceAgg(t *testing.T) {
	tbl := eqTable()
	base := &Plan{
		Filter: expr.Gt(expr.Col(2), expr.FloatLit(100)),
		GroupBy: []expr.Expr{expr.Col(1)},
		Aggregates: []agg.Def{
			{Func: agg.CountStar},
			{Func: agg.Sum, Expr: expr.Col(2)},
			{Func: agg.Avg, Expr: expr.Col(2)},
			{Func: agg.Min, Expr: expr.Col(2)},
			{Func: agg.Max, Expr: expr.Col(2)},
			{Func: agg.Count, Expr: expr.Col(0)},
		},
	}
	batchSizes := []int{1, 100, 1024}
	partCounts := []int{1, 4, 8}
	parallelisms := []int{1, 4}
	for _, bs := range batchSizes {
		for _, parts := range partCounts {
			for _, par := range parallelisms {
				t.Run(fmt.Sprintf("bs=%d/parts=%d/par=%d", bs, parts, par), func(t *testing.T) {
					t2 := catalog.NewTable("t", tbl.Schema, allRows(tbl), parts)
					plan := *base
					plan.Table = t2
					cfg := DefaultConfig()
					cfg.BatchSize = bs
					cfg.Parallelism = par
					cfg.MemoryBudget = 0
					res, err := CrossCheck(context.Background(), cfg, &plan)
					if err != nil {
						t.Fatalf("CrossCheck: %v", err)
					}
					if !res.Match {
						t.Fatalf("mismatch: %s", res.Detail)
					}
				})
			}
		}
	}
}

// TestEquivalencePlain: filter + project (no sort/agg) across configurations.
func TestEquivalencePlain(t *testing.T) {
	tbl := eqTable()
	base := &Plan{
		Filter: expr.And(
			expr.Gt(expr.Col(2), expr.FloatLit(200)),
			expr.Eq(expr.Col(3), expr.BoolLit(true)),
		),
		Projections: []expr.Expr{
			expr.Col(0), expr.Col(1), expr.Col(2),
		},
	}
	for _, bs := range []int{1, 50, 1024} {
		for _, parts := range []int{1, 4, 8} {
			for _, par := range []int{1, 4} {
				t.Run(fmt.Sprintf("bs=%d/parts=%d/par=%d", bs, parts, par), func(t *testing.T) {
					t2 := catalog.NewTable("t", tbl.Schema, allRows(tbl), parts)
					plan := *base
					plan.Table = t2
					cfg := DefaultConfig()
					cfg.BatchSize = bs
					cfg.Parallelism = par
					cfg.MemoryBudget = 0
					res, err := CrossCheck(context.Background(), cfg, &plan)
					if err != nil {
						t.Fatalf("CrossCheck: %v", err)
					}
					if !res.Match {
						t.Fatalf("mismatch: %s", res.Detail)
					}
				})
			}
		}
	}
}

// TestEquivalenceAggInt: integer aggregate (exact, no float tolerance).
func TestEquivalenceAggInt(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "grp", Type: types.String},
		{Name: "val", Type: types.Int64},
	}}
	tbl := catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows: 1500, NumPartitions: 4, Seed: 777, NullFrac: 0.1, StringCard: 10, IntRange: 1000,
	})
	base := &Plan{
		GroupBy: []expr.Expr{expr.Col(1)},
		Aggregates: []agg.Def{
			{Func: agg.CountStar},
			{Func: agg.Sum, Expr: expr.Col(2)},
			{Func: agg.Min, Expr: expr.Col(2)},
			{Func: agg.Max, Expr: expr.Col(2)},
			{Func: agg.Count, Expr: expr.Col(2)},
		},
	}
	for _, bs := range []int{1, 100, 1024} {
		for _, parts := range []int{1, 4, 8} {
			t.Run(fmt.Sprintf("bs=%d/parts=%d", bs, parts), func(t *testing.T) {
				t2 := catalog.NewTable("t", tbl.Schema, allRows(tbl), parts)
				plan := *base
				plan.Table = t2
				cfg := DefaultConfig()
				cfg.BatchSize = bs
				cfg.Parallelism = 4
				cfg.MemoryBudget = 0
				res, err := CrossCheck(context.Background(), cfg, &plan)
				if err != nil {
					t.Fatalf("CrossCheck: %v", err)
				}
				if !res.Match {
					t.Fatalf("mismatch: %s", res.Detail)
				}
			})
		}
	}
}

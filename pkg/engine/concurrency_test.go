package engine

import (
	"time"
	"context"
	"sync"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

// TestConcurrentCrossCheck runs many cross-checks concurrently to verify there
// are no data races in the vectorized path. Run with -race.
func TestConcurrentCrossCheck(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "grp", Type: types.String},
		{Name: "val", Type: types.Int64},
	}}
	tbl := catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows: 2000, NumPartitions: 8, Seed: 555, NullFrac: 0.1, StringCard: 10, IntRange: 1000,
	})

	plans := []*Plan{
		{
			Table:   tbl,
			Filter:  expr.Gt(expr.Col(2), expr.IntLit(100)),
			OrderBy: []vexec.SortKey{{Expr: expr.Col(2)}},
		},
		{
			Table:   tbl,
			GroupBy: []expr.Expr{expr.Col(1)},
			Aggregates: []agg.Def{
				{Func: agg.CountStar},
				{Func: agg.Sum, Expr: expr.Col(2)},
				{Func: agg.Min, Expr: expr.Col(2)},
				{Func: agg.Max, Expr: expr.Col(2)},
			},
		},
		{
			Table:  tbl,
			Filter: expr.Eq(expr.Col(1), expr.StrLit("k5")),
			Projections: []expr.Expr{
				expr.Col(0), expr.Col(1), expr.Col(2),
			},
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			plan := plans[i%len(plans)]
			cfg := DefaultConfig()
			cfg.BatchSize = 50
			cfg.Parallelism = 4
			cfg.MemoryBudget = 1 << 20
			res, err := CrossCheck(context.Background(), cfg, plan)
			if err != nil {
				t.Errorf("iter %d: %v", i, err)
				return
			}
			if !res.Match {
				t.Errorf("iter %d: mismatch: %s", i, res.Detail)
			}
		}(i)
	}
	wg.Wait()
}

// TestConcurrentCancel runs many queries and cancels them concurrently to
// verify cancellation is race-free. Run with -race.
func TestConcurrentCancel(t *testing.T) {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "val", Type: types.Int64},
	}}
	tbl := catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows: 100000, NumPartitions: 8, Seed: 444, IntRange: 10000,
	})
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1)}},
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg := DefaultConfig()
			cfg.BatchSize = 64
			cfg.Parallelism = 4
			cfg.MemoryBudget = 4096
			handle := NewHandle(cfg)
			defer handle.Cancel()

			done := make(chan error, 1)
			go func() {
				_, err := ExecuteVector(handle.Ctx(), cfg, plan)
				done <- err
			}()

			// Cancel after a short, variable delay.
			delay := time.Duration(i+1) * time.Millisecond
			time.Sleep(delay)
			handle.Cancel()

			select {
			case err := <-done:
				if err == nil {
					t.Errorf("iter %d: expected cancellation error, got nil", i)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("iter %d: query did not stop", i)
			}
		}(i)
	}
	wg.Wait()
}

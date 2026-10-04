package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

// largeTable builds a table big enough to take a while to process.
func largeTable() *catalog.Table {
	schema := catalog.Schema{Cols: []catalog.Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Int64},
	}}
	return catalog.GenerateTable("t", schema, catalog.GeneratorConfig{
		NumRows: 200000, NumPartitions: 8, Seed: 999, NullFrac: 0.05, IntRange: 100000,
	})
}

// TestCancelSort: cancel a sort mid-execution; must return an error and leave
// no partial result.
func TestCancelSort(t *testing.T) {
	tbl := largeTable()
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1)}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 64
	cfg.Parallelism = 4
	cfg.MemoryBudget = 4096 // small budget to force spilling

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ExecuteVector(ctx, cfg, plan)
		done <- err
	}()

	// Let it run briefly, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query did not stop after cancellation")
	}
}

// TestCancelAgg: cancel an aggregate mid-execution.
func TestCancelAgg(t *testing.T) {
	tbl := largeTable()
	plan := &Plan{
		Table:   tbl,
		GroupBy: []expr.Expr{expr.Col(0)},
		Aggregates: []agg.Def{
			{Func: agg.CountStar},
			{Func: agg.Sum, Expr: expr.Col(1)},
		},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 64
	cfg.Parallelism = 4
	cfg.MemoryBudget = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ExecuteVector(ctx, cfg, plan)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query did not stop after cancellation")
	}
}

// TestCancelIdempotent: calling cancel multiple times is safe.
func TestCancelIdempotent(t *testing.T) {
	tbl := largeTable()
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1)}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 64
	cfg.Parallelism = 4
	cfg.MemoryBudget = 4096

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ExecuteVector(ctx, cfg, plan)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	// Cancel many times, concurrently.
	for i := 0; i < 10; i++ {
		go cancel()
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query did not stop after cancellation")
	}
}

// TestTimeout: a timeout cancels the query.
func TestTimeout(t *testing.T) {
	tbl := largeTable()
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1)}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 64
	cfg.Parallelism = 4
	cfg.MemoryBudget = 4096
	cfg.Timeout = 50 * time.Millisecond

	handle := NewHandle(cfg)
	defer handle.Cancel()

	done := make(chan error, 1)
	go func() {
		_, err := ExecuteVector(handle.Ctx(), cfg, plan)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error, got nil")
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query did not stop after timeout")
	}
}

// TestCancelCleansSpillFiles: after cancellation, no spill files remain in the
// spill directory.
func TestCancelCleansSpillFiles(t *testing.T) {
	dir := t.TempDir()
	tbl := largeTable()
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1)}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 64
	cfg.Parallelism = 4
	cfg.MemoryBudget = 4096
	cfg.SpillDir = dir

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ExecuteVector(ctx, cfg, plan)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	// Check no spill files remain.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".spill" {
			t.Fatalf("spill file not cleaned up: %s", e.Name())
		}
	}
}

// TestCancelRowPath: the row-by-row path also respects cancellation.
func TestCancelRowPath(t *testing.T) {
	tbl := largeTable()
	plan := &Plan{
		Table:   tbl,
		OrderBy: []vexec.SortKey{{Expr: expr.Col(1)}},
	}
	cfg := DefaultConfig()
	cfg.BatchSize = 64
	cfg.Parallelism = 4

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ExecuteRow(ctx, cfg, plan)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query did not stop after cancellation")
	}
}

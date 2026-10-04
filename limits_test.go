package executor

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func spillFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "vqsort-run-*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestSortSpillUnderMemoryLimit(t *testing.T) {
	dir := t.TempDir()
	tbl := makeTestTable(3000)
	plan := &Plan{SortKeys: []SortKey{{Col: 2}, {Col: 0}}}
	// budget for only a handful of rows: forces many spill runs
	o := Options{BatchSize: 50, Partitions: 3, Concurrency: 2, MemLimitBytes: 4096, TempDir: dir}
	res, err := CrossCheck(context.Background(), tbl, plan, o)
	if err != nil {
		t.Fatalf("sort with spill failed: %v", err)
	}
	if len(res) != 3000 {
		t.Fatalf("got %d rows", len(res))
	}
	less := makeLess(plan.SortKeys, new(error))
	for i := 1; i < len(res); i++ {
		if less(res[i], res[i-1]) {
			t.Fatalf("not sorted at %d", i)
		}
	}
	if left := spillFiles(t, dir); len(left) != 0 {
		t.Fatalf("spill files left behind: %v", left)
	}
}

func TestSortMemoryLimitTooSmall(t *testing.T) {
	tbl := makeTestTable(100)
	plan := &Plan{SortKeys: []SortKey{{Col: 0}}}
	// less than one row: cannot make progress -> defined failure
	o := Options{MemLimitBytes: 8, TempDir: t.TempDir()}
	_, err := CrossCheck(context.Background(), tbl, plan, o)
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit, got %v", err)
	}
}

func TestAggregateMemoryLimit(t *testing.T) {
	// 5000 distinct groups with a small budget -> ErrMemoryLimit on both paths
	n := 5000
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{IntVal(int64(i)), StringVal("x"), FloatVal(1), BoolVal(true)}
	}
	tbl := NewTable(testSchema(), rows)
	plan := &Plan{GroupBy: []int{0}, Aggs: []AggSpec{{Func: AggSum, Col: 2}}}
	o := Options{BatchSize: 100, Partitions: 2, Concurrency: 2, MemLimitBytes: 16 * 1024}
	_, err := CrossCheck(context.Background(), tbl, plan, o)
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("expected ErrMemoryLimit, got %v", err)
	}
	// unlimited memory: same plan succeeds
	res, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 100, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != n {
		t.Fatalf("expected %d groups, got %d", n, len(res))
	}
}

func TestCancelBeforeStart(t *testing.T) {
	tbl := makeTestTable(1000)
	plan := &Plan{SortKeys: []SortKey{{Col: 0}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancel() // repeated cancel must be harmless
	res, err := ExecuteVec(ctx, tbl, plan, Options{BatchSize: 10, Partitions: 4, Concurrency: 2})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("expected ErrCanceled, got %v", err)
	}
	if res != nil {
		t.Fatalf("partial results leaked after cancel: %d rows", len(res))
	}
	res, err = ExecuteRow(ctx, tbl, plan, Options{Partitions: 4})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("row path: expected ErrCanceled, got %v", err)
	}
	if res != nil {
		t.Fatal("partial results leaked after cancel")
	}
}

func TestCancelMidFlight(t *testing.T) {
	tbl := makeTestTable(400000)
	plan := &Plan{
		Filter:   Binary{Op: OpGE, L: Column{0}, R: Literal{IntVal(0)}},
		SortKeys: []SortKey{{Col: 2}, {Col: 1}},
	}
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	go func() {
		time.Sleep(5 * time.Millisecond)
		once.Do(cancel)
		once.Do(cancel) // idempotent
	}()
	start := time.Now()
	res, err := ExecuteVec(ctx, tbl, plan, Options{BatchSize: 128, Partitions: 8, Concurrency: 4, MemLimitBytes: 64 * 1024, TempDir: dir})
	if errors.Is(err, ErrCanceled) {
		if res != nil {
			t.Fatal("partial results leaked after cancel")
		}
		if left := spillFiles(t, dir); len(left) != 0 {
			t.Fatalf("spill files left after cancel: %v", left)
		}
		t.Logf("canceled after %v", time.Since(start))
		return
	}
	// The machine may be fast enough to finish first; then results must be
	// complete and correct.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 400000 {
		t.Fatalf("got %d rows", len(res))
	}
}

func TestTimeout(t *testing.T) {
	tbl := makeTestTable(400000)
	plan := &Plan{SortKeys: []SortKey{{Col: 2}, {Col: 1}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := ExecuteVec(ctx, tbl, plan, Options{BatchSize: 128, Partitions: 8, Concurrency: 4, MemLimitBytes: 64 * 1024, TempDir: t.TempDir()})
	if err == nil {
		t.Skip("execution finished before deadline")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestConcurrentCancel(t *testing.T) {
	tbl := makeTestTable(200000)
	plan := &Plan{SortKeys: []SortKey{{Col: 0}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			cancel() // concurrent, repeated cancels
		}()
	}
	_, err := ExecuteVec(ctx, tbl, plan, Options{BatchSize: 64, Partitions: 8, Concurrency: 4, MemLimitBytes: 32 * 1024, TempDir: t.TempDir()})
	wg.Wait()
	if err != nil && !errors.Is(err, ErrCanceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNoGoroutineLeak(t *testing.T) {
	tbl := makeTestTable(20000)
	plan := &Plan{SortKeys: []SortKey{{Col: 0}}}
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		if _, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 100, Partitions: 4, Concurrency: 3, MemLimitBytes: 8192, TempDir: t.TempDir()}); err != nil {
			t.Fatal(err)
		}
	}
	// canceled executions must also clean up
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = ExecuteVec(ctx, tbl, plan, Options{Partitions: 4})
	}
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestDeterministicAcrossRuns(t *testing.T) {
	tbl := makeTestTable(2000)
	plan := &Plan{
		Filter:   Binary{Op: OpGT, L: Column{0}, R: Literal{IntVal(3)}},
		GroupBy:  []int{0},
		Aggs:     []AggSpec{{Func: AggCount, Col: -1}, {Func: AggAvg, Col: 2}},
		SortKeys: []SortKey{{Col: 2, Desc: true}, {Col: 0}},
	}
	var first []Row
	for i, o := range []Options{
		{BatchSize: 1, Partitions: 1, Concurrency: 1},
		{BatchSize: 7, Partitions: 5, Concurrency: 3},
		{BatchSize: 1024, Partitions: 16, Concurrency: 8},
	} {
		res, err := CrossCheck(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = res
			continue
		}
		if !EqualRows(first, res) {
			t.Fatalf("run %d diverged: %v", i, DiffRows(first, res))
		}
	}
}

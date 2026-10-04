package executor

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"sync"
)

// ExecCtx carries per-execution shared state.
type ExecCtx struct {
	Ctx       context.Context
	Mem       *MemTracker
	BatchSize int
	TempDir   string
}

// CheckCancel maps context cancellation/timeout to ErrCanceled/ErrTimeout.
func (e *ExecCtx) CheckCancel() error {
	if e.Ctx == nil {
		return nil
	}
	select {
	case <-e.Ctx.Done():
		if e.Ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ErrCanceled
	default:
		return nil
	}
}

// SortKey orders by output column Col (0-based), ascending unless Desc.
type SortKey struct {
	Col  int
	Desc bool
}

// AggFunc enumerates aggregate functions.
type AggFunc int

const (
	AggCount AggFunc = iota // COUNT(*) when Col<0 else COUNT(col) non-NULL
	AggSum
	AggMin
	AggMax
	AggAvg
)

// AggSpec describes one aggregate output column.
type AggSpec struct {
	Func AggFunc
	Col  int // input column index; -1 means COUNT(*)
}

// Plan combines scan -> filter -> project -> aggregate -> sort.
// SortKeys refer to final output columns.
type Plan struct {
	Filter   Expr
	Proj     []Expr
	GroupBy  []int
	Aggs     []AggSpec
	SortKeys []SortKey
}

func (p *Plan) hasAgg() bool  { return len(p.Aggs) > 0 || len(p.GroupBy) > 0 }
func (p *Plan) hasSort() bool { return len(p.SortKeys) > 0 }

// Options configures an execution.
type Options struct {
	BatchSize     int    // vectorized batch size (rows)
	Partitions    int    // input partitions
	Concurrency   int    // max partitions processed in parallel
	MemLimitBytes int64  // memory budget per execution (<=0: unlimited)
	TempDir       string // spill directory (empty: os.TempDir)
}

func (o Options) withDefaults() Options {
	if o.BatchSize <= 0 {
		o.BatchSize = 1024
	}
	if o.Partitions <= 0 {
		o.Partitions = 1
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	return o
}

// ExecuteRow runs the plan on the row-at-a-time path and returns all rows.
func ExecuteRow(ctx context.Context, t *Table, p *Plan, o Options) ([]Row, error) {
	return run(ctx, t, p, o, false)
}

// ExecuteVec runs the plan on the vectorized path and returns all rows.
func ExecuteVec(ctx context.Context, t *Table, p *Plan, o Options) ([]Row, error) {
	return run(ctx, t, p, o, true)
}

// rowSource is anything that yields rows.
type rowSource interface {
	Next() (Row, error)
}

// vecRowAdapter adapts a VecOp into a rowSource, materializing only the
// selected rows of each batch.
type vecRowAdapter struct {
	op  VecOp
	b   *Batch
	sel []int
	pos int
}

func (a *vecRowAdapter) Next() (Row, error) {
	for {
		if a.b != nil && a.pos < len(a.sel) {
			i := a.sel[a.pos]
			a.pos++
			row := make(Row, len(a.b.Cols))
			for c, v := range a.b.Cols {
				row[c] = v.ValueAt(i)
			}
			return row, nil
		}
		b, err := a.op.Next()
		if err != nil {
			return nil, err
		}
		a.b = b
		if b.Sel != nil {
			a.sel = b.Sel
		} else {
			a.sel = make([]int, b.N)
			for i := range a.sel {
				a.sel[i] = i
			}
		}
		a.pos = 0
	}
}

// aggregatorHolder is implemented by aggregate operators so the pipeline can
// merge partition-level partial aggregates.
type aggregatorHolder interface {
	Aggregator() *aggregator
}

// Aggregator exposes the loaded partial aggregate state.
func (a *rowAggregate) Aggregator() *aggregator { return a.agg }

// Aggregator exposes the loaded partial aggregate state.
func (a *vecAggregate) Aggregator() *aggregator { return a.agg }

// partResult holds one partition's outcome; exactly one field is used
// depending on the plan shape.
type partResult struct {
	rows []Row       // plain scan/filter/project
	src  rowSource   // sorted per-partition stream
	agg  *aggregator // partial aggregate state
}

// run executes the plan over n partitions with bounded concurrency. Output
// ordering is deterministic: partition order for plain plans, key order for
// sorted plans, group-key order for aggregations.
func run(ctx context.Context, t *Table, p *Plan, o Options, vectorized bool) ([]Row, error) {
	o = o.withDefaults()
	if err := t.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	n := o.Partitions
	if len(t.Rows) == 0 {
		n = 1
	} else if n > len(t.Rows) {
		n = len(t.Rows)
	}
	// Memory model: the budget is divided equally among partitions so
	// concurrent partitions cannot starve each other; the combine phase
	// (aggregate merge, final sort) gets its own full budget.
	perPart := int64(0)
	if o.MemLimitBytes > 0 {
		perPart = o.MemLimitBytes / int64(n)
		if perPart <= 0 {
			perPart = 1
		}
	}
	newPartCtx := func() *ExecCtx {
		return &ExecCtx{Ctx: ctx, Mem: NewMemTracker(perPart), BatchSize: o.BatchSize, TempDir: o.TempDir}
	}
	ectx := &ExecCtx{Ctx: ctx, Mem: NewMemTracker(o.MemLimitBytes), BatchSize: o.BatchSize, TempDir: o.TempDir}

	results := make([]partResult, n)
	closers := make([]io.Closer, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	sem := make(chan struct{}, o.Concurrency)
	for pi := 0; pi < n; pi++ {
		wg.Add(1)
		go func(pi int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res, cl, err := runPartition(t, p, newPartCtx(), pi, n, vectorized)
			if err != nil {
				if cl != nil {
					cl.Close()
				}
				errs[pi] = err
				cancel() // stop sibling partitions promptly
				return
			}
			results[pi] = res
			closers[pi] = cl
		}(pi)
	}
	wg.Wait()
	// Cleanup is unconditional: spill files and reserved memory are released
	// even on failure, and no goroutine outlives this function.
	defer func() {
		for _, cl := range closers {
			if cl != nil {
				cl.Close()
			}
		}
	}()
	// Prefer the root-cause error: a partition failure cancels its siblings,
	// which then report ErrCanceled/ErrTimeout. Only surface a cancel error
	// when it is the actual cause (e.g. the caller canceled).
	var firstErr error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
		if !errors.Is(err, ErrCanceled) && !errors.Is(err, ErrTimeout) {
			return nil, err // no partial results escape
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	switch {
	case p.hasAgg():
		return combineAggs(p, ectx, results)
	case p.hasSort():
		return mergeSorted(p, ectx, results)
	default:
		var out []Row
		for _, r := range results {
			out = append(out, r.rows...)
		}
		return out, nil
	}
}

// runPartition builds and runs one partition's operator chain. The returned
// closer must be called by the caller once the result is consumed.
func runPartition(t *Table, p *Plan, ectx *ExecCtx, pi, n int, vectorized bool) (partResult, io.Closer, error) {
	start, end := PartitionRange(len(t.Rows), pi, n)
	rows := t.Rows[start:end]
	if vectorized {
		return runPartitionVec(t.Schema, rows, p, ectx)
	}
	return runPartitionRow(t.Schema, rows, p, ectx)
}

func runPartitionRow(schema *Schema, rows []Row, p *Plan, ectx *ExecCtx) (partResult, io.Closer, error) {
	var top RowOp = &rowScan{rows: rows, ectx: ectx}
	if p.Filter != nil {
		top = &rowFilter{child: top, pred: p.Filter}
	}
	if p.Proj != nil {
		top = &rowProject{child: top, exprs: p.Proj}
	}
	switch {
	case p.hasAgg():
		aggOp := &rowAggregate{child: top, groupBy: p.GroupBy, aggs: p.Aggs, ectx: ectx}
		if err := drainRows(aggOp); err != nil {
			aggOp.Close()
			return partResult{}, nil, err
		}
		return partResult{agg: aggOp.Aggregator()}, aggOp, nil
	case p.hasSort():
		sortOp := &rowSort{child: top, keys: p.SortKeys, ectx: ectx}
		if err := sortOp.load(); err != nil {
			sortOp.Close()
			return partResult{}, nil, err
		}
		return partResult{src: sortOp}, sortOp, nil
	default:
		out, err := collectRows(top)
		if err != nil {
			top.Close()
			return partResult{}, nil, err
		}
		return partResult{rows: out}, top, nil
	}
}

func runPartitionVec(schema *Schema, rows []Row, p *Plan, ectx *ExecCtx) (partResult, io.Closer, error) {
	var top VecOp = &vecScan{schema: schema, rows: rows, ectx: ectx}
	if p.Filter != nil {
		top = &vecFilter{child: top, pred: p.Filter}
	}
	if p.Proj != nil {
		top = &vecProject{child: top, exprs: p.Proj}
	}
	switch {
	case p.hasAgg():
		aggOp := &vecAggregate{child: top, groupBy: p.GroupBy, aggs: p.Aggs, ectx: ectx}
		if err := drainBatches(aggOp); err != nil {
			aggOp.Close()
			return partResult{}, nil, err
		}
		return partResult{agg: aggOp.Aggregator()}, aggOp, nil
	case p.hasSort():
		sortOp := &vecSort{child: top, keys: p.SortKeys, ectx: ectx}
		if err := sortOp.load(); err != nil {
			sortOp.Close()
			return partResult{}, nil, err
		}
		return partResult{src: &vecRowAdapter{op: sortOp}}, sortOp, nil
	default:
		out, err := collectRows(&vecRowAdapter{op: top})
		if err != nil {
			top.Close()
			return partResult{}, nil, err
		}
		return partResult{rows: out}, top, nil
	}
}

func drainRows(op RowOp) error {
	for {
		_, err := op.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func drainBatches(op VecOp) error {
	for {
		_, err := op.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func collectRows(src rowSource) ([]Row, error) {
	var out []Row
	for {
		r, err := src.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
}

// combineAggs merges partition partial aggregates in partition order and
// applies the optional final sort.
func combineAggs(p *Plan, ectx *ExecCtx, results []partResult) ([]Row, error) {
	global := newAggregator(p.GroupBy, p.Aggs, ectx)
	defer global.close()
	for _, r := range results {
		if r.agg == nil {
			continue
		}
		if err := global.merge(r.agg); err != nil {
			return nil, err
		}
	}
	rows, err := global.results()
	if err != nil {
		return nil, err
	}
	if !p.hasSort() {
		return rows, nil
	}
	return sortRows(rows, p.SortKeys, ectx)
}

// sortRows sorts an in-memory slice through a sortBuffer (bounded memory).
func sortRows(rows []Row, keys []SortKey, ectx *ExecCtx) ([]Row, error) {
	buf := newSortBuffer(keys, ectx)
	defer buf.close()
	for _, r := range rows {
		if err := ectx.CheckCancel(); err != nil {
			return nil, err
		}
		if err := buf.add(r); err != nil {
			return nil, err
		}
	}
	if err := buf.finish(); err != nil {
		return nil, err
	}
	return collectRows(sourceFunc(buf.next))
}

// sourceFunc adapts a function to rowSource.
type sourceFunc func() (Row, error)

func (f sourceFunc) Next() (Row, error) { return f() }

// mergeSorted k-way merges the per-partition sorted streams. Ties break
// toward the lower partition index, keeping the merge stable and
// deterministic.
func mergeSorted(p *Plan, ectx *ExecCtx, results []partResult) ([]Row, error) {
	srcs := make([]rowSource, 0, len(results))
	for _, r := range results {
		if r.src != nil {
			srcs = append(srcs, r.src)
		}
	}
	merged := newMergedSource(srcs, p.SortKeys)
	var out []Row
	for {
		if err := ectx.CheckCancel(); err != nil {
			return nil, err
		}
		r, err := merged.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
}

// mergedSource k-way merges sorted rowSources.
type mergedSource struct {
	srcs   []rowSource
	h      *mergeHeap2
	cmpErr *error
}

type mergeItem2 struct {
	row Row
	src int
}

type mergeHeap2 struct {
	items []mergeItem2
	less  lessFn
}

func (h *mergeHeap2) Len() int { return len(h.items) }
func (h *mergeHeap2) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.less(a.row, b.row) {
		return true
	}
	if h.less(b.row, a.row) {
		return false
	}
	return a.src < b.src
}
func (h *mergeHeap2) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap2) Push(x interface{}) { h.items = append(h.items, x.(mergeItem2)) }
func (h *mergeHeap2) Pop() interface{} {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	return it
}

func newMergedSource(srcs []rowSource, keys []SortKey) *mergedSource {
	m := &mergedSource{srcs: srcs, cmpErr: new(error)}
	m.h = &mergeHeap2{less: makeLess(keys, m.cmpErr)}
	for i, s := range srcs {
		r, err := s.Next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			*m.cmpErr = err
			return m
		}
		heap.Push(m.h, mergeItem2{row: r, src: i})
	}
	return m
}

// Next pulls the smallest row and refills from the same source.
func (m *mergedSource) Next() (Row, error) {
	if *m.cmpErr != nil {
		return nil, *m.cmpErr
	}
	if m.h.Len() == 0 {
		return nil, io.EOF
	}
	it := heap.Pop(m.h).(mergeItem2)
	nxt, err := m.srcs[it.src].Next()
	switch {
	case err == nil:
		heap.Push(m.h, mergeItem2{row: nxt, src: it.src})
	case err != io.EOF:
		return nil, err
	}
	return it.row, nil
}

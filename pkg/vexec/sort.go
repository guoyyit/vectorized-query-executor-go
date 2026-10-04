package vexec

import (
	"container/heap"
	"context"
	"sort"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// SortKey is an ORDER BY key expression.
type SortKey struct {
	Expr expr.Expr
	Desc bool
}

// cmpKeys compares two key tuples. Returns -1/0/1.
func cmpKeys(a, b []types.Datum, keys []SortKey) (int, error) {
	for i, k := range keys {
		cmp, err := types.Compare(a[i], b[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if k.Desc {
				return -cmp, nil
			}
			return cmp, nil
		}
	}
	return 0, nil
}

// ---- Sort operator (one child pipeline / one partition) ----

// SortOp materializes, sorts and (if needed) spills its input, then produces a
// single sorted Run. Memory usage is bounded by the accountant: when buffered
// batches exceed the budget, the current buffer is sorted and spilled to disk.
type SortOp struct {
	child     BatchOperator
	keys      []SortKey
	batchSize int
	acc       *Accountant
	spill     *SpillDir

	run      Run
	rows     int64
	reserved int64
	ctx      context.Context
}

func NewSort(child BatchOperator, keys []SortKey, batchSize int, acc *Accountant, spill *SpillDir) *SortOp {
	if batchSize <= 0 {
		batchSize = 1024
	}
	return &SortOp{child: child, keys: keys, batchSize: batchSize, acc: acc, spill: spill}
}

func (o *SortOp) Open(ctx context.Context) error {
	o.ctx = ctx
	if err := o.child.Open(ctx); err != nil {
		return err
	}
	var batches []*vector.Batch
	var spillRuns []Run
	var reserved int64

	flush := func() error {
		if len(batches) == 0 {
			return nil
		}
		sorted, rows, err := o.sortBatches(batches)
		if err != nil {
			return err
		}
		sw, err := newSpillWriter(o.spill)
		if err != nil {
			return err
		}
		for _, b := range sorted {
			if err := sw.append(b); err != nil {
				return err
			}
		}
		if err := sw.close(); err != nil {
			return err
		}
		spillRuns = append(spillRuns, newSpillRun(sw.path, rows))
		o.acc.Release(reserved)
		reserved = 0
		batches = nil
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := o.child.Next()
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		if b.N == 0 {
			continue
		}
		est := EstimateBatchBytes(b)
		if !o.acc.Reserve(est) {
			if err := flush(); err != nil {
				return err
			}
			// If a single batch exceeds the budget, allow it; spilling still
			// bounds total memory, and the oversize batch is a soft target.
			if o.acc.Reserve(est) {
				reserved += est
			}
		} else {
			reserved += est
		}
		batches = append(batches, b)
	}

	if len(spillRuns) == 0 {
		// Everything fit in memory.
		sorted, rows, err := o.sortBatches(batches)
		if err != nil {
			return err
		}
		o.run = newMemRun(sorted, rows)
		o.rows = rows
		o.reserved = reserved
		return nil
	}
	// Spill the final buffer, then merge all spilled runs into one.
	if err := flush(); err != nil {
		return err
	}
	o.reserved = 0
	merged, err := MergeRuns(ctx, spillRuns, o.keys, o.batchSize, o.acc, o.spill)
	if err != nil {
		return err
	}
	o.run = merged
	o.rows = merged.Rows()
	return nil
}

func (o *SortOp) Next() (*vector.Batch, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	if o.run == nil {
		return nil, nil
	}
	return o.run.NextBatch()
}

// NextBatch lets SortOp satisfy the Run interface, so a partition's sorted
// output can be merged directly.
func (o *SortOp) NextBatch() (*vector.Batch, error) { return o.Next() }

func (o *SortOp) Rows() int64 { return o.rows }

func (o *SortOp) Close() error {
	if o.run != nil {
		o.run.Close()
		o.run = nil
	}
	if o.reserved > 0 {
		o.acc.Release(o.reserved)
		o.reserved = 0
	}
	return o.child.Close()
}

// sortBatches sorts a collection of batches by the sort keys and re-batches
// the result into batchSize batches.
func (o *SortOp) sortBatches(batches []*vector.Batch) ([]*vector.Batch, int64, error) {
	type item struct {
		b    *vector.Batch
		row  int
		keys []types.Datum
		seq  int64
	}
	var items []item
	var seq int64
	for _, b := range batches {
		keyCols := make([]*vector.Column, len(o.keys))
		for i, k := range o.keys {
			c, err := k.Expr.EvalBatch(b)
			if err != nil {
				return nil, 0, err
			}
			keyCols[i] = c
		}
		for ri := 0; ri < b.N; ri++ {
			kv := make([]types.Datum, len(o.keys))
			for ki, c := range keyCols {
				kv[ki] = c.Get(ri)
			}
			items = append(items, item{b: b, row: ri, keys: kv, seq: seq})
			seq++
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		cmp, err := cmpKeys(items[i].keys, items[j].keys, o.keys)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return cmp < 0
		}
		return items[i].seq < items[j].seq
	})
	// Re-batch sorted rows.
	var out []*vector.Batch
	var cur *vector.Batch
	var rows int64
	for _, it := range items {
		if cur == nil {
			cur = vector.NewBatch(it.b.Schema(), o.batchSize)
		}
		cur.AppendRow(it.b.Row(it.row))
		rows++
		if cur.N >= o.batchSize {
			out = append(out, cur)
			cur = nil
		}
	}
	if cur != nil {
		out = append(out, cur)
	}
	return out, rows, nil
}

// ---- K-way merge of sorted runs ----

type mergeItem struct {
	runIdx int
	seq    int64
	batch  *vector.Batch
	rowIdx int
	keys   []types.Datum
}

type mergeHeap struct {
	items []*mergeItem
	keys  []SortKey
	err   error
}

func (h *mergeHeap) Len() int { return len(h.items) }
func (h *mergeHeap) Less(i, j int) bool {
	if h.err != nil {
		return false
	}
	cmp, err := cmpKeys(h.items[i].keys, h.items[j].keys, h.keys)
	if err != nil {
		h.err = err
		return false
	}
	if cmp != 0 {
		return cmp < 0
	}
	// Stable tie-break: run order, then position within run.
	if h.items[i].runIdx != h.items[j].runIdx {
		return h.items[i].runIdx < h.items[j].runIdx
	}
	return h.items[i].seq < h.items[j].seq
}
func (h *mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap) Push(x any)   { h.items = append(h.items, x.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	n := len(h.items)
	it := h.items[n-1]
	h.items = h.items[:n-1]
	return it
}

// MergeRuns merges sorted runs into one sorted Run using a k-way heap merge.
// Runs are merged hierarchically in groups so that memory stays bounded: at
// most a constant number of input batches plus one output batch is resident.
func MergeRuns(ctx context.Context, runs []Run, keys []SortKey, batchSize int, acc *Accountant, spill *SpillDir) (Run, error) {
	if len(runs) == 1 {
		return runs[0], nil
	}
	current := runs
	for len(current) > 1 {
		var next []Run
		for start := 0; start < len(current); {
			w := mergeWidth(current[start:], batchSize, acc)
			merged, err := mergeGroup(ctx, current[start:start+w], keys, batchSize, acc, spill)
			if err != nil {
				return nil, err
			}
			next = append(next, merged)
			start += w
		}
		current = next
	}
	return current[0], nil
}

// mergeWidth decides how many runs to merge in one group given the budget.
func mergeWidth(remaining []Run, batchSize int, acc *Accountant) int {
	const maxWidth = 8
	if acc.Budget() <= 0 {
		if len(remaining) > maxWidth {
			return maxWidth
		}
		return len(remaining)
	}
	// Rough estimate: each run needs one resident batch; output needs one.
	perBatch := int64(batchSize) * 64
	width := int((acc.Budget() - perBatch) / perBatch)
	if width < 2 {
		width = 2
	}
	if width > maxWidth {
		width = maxWidth
	}
	if width > len(remaining) {
		width = len(remaining)
	}
	return width
}

func mergeGroup(ctx context.Context, runs []Run, keys []SortKey, batchSize int, acc *Accountant, spill *SpillDir) (Run, error) {
	width := len(runs)
	// Reserve resident batches for all input runs plus one output batch. The
	// merge's memory is inherently bounded by (width+1)*batchSize rows, so a
	// failed reservation is a soft condition: we proceed anyway rather than
	// failing the query.
	perBatch := int64(batchSize) * 64
	reserved := int64(width+1) * perBatch
	if acc.Budget() > 0 {
		acc.Reserve(reserved)
	}
	defer acc.Release(reserved)

	h := &mergeHeap{keys: keys}
	heap.Init(h)
	cursors := make([]*mergeCursor, width)
	for i, r := range runs {
		c := &mergeCursor{run: r, runIdx: i}
		if err := c.advance(ctx, keys); err != nil {
			return nil, err
		}
		cursors[i] = c
		if c.item != nil {
			heap.Push(h, c.item)
		}
	}

	var outBatches []*vector.Batch
	var outRows int64
	var outBytes int64
	var out *vector.Batch
	var spillWriter *spillWriter
	spilling := false

	flushOutput := func() error {
		if out == nil || out.N == 0 {
			return nil
		}
		if spilling {
			if err := spillWriter.append(out); err != nil {
				return err
			}
		} else {
			est := EstimateBatchBytes(out)
			if acc.Budget() > 0 && outBytes+est > acc.Budget() {
				// Start spilling: write everything buffered so far, then keep
				// writing directly.
				sw, err := newSpillWriter(spill)
				if err != nil {
					return err
				}
				for _, b := range outBatches {
					if err := sw.append(b); err != nil {
						return err
					}
				}
				if err := sw.append(out); err != nil {
					return err
				}
				outBatches = nil
				spillWriter = sw
				spilling = true
			} else {
				outBatches = append(outBatches, out)
				outBytes += est
			}
		}
		out = nil
		return nil
	}

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		it := heap.Pop(h).(*mergeItem)
		if out == nil {
			out = vector.NewBatch(it.batch.Schema(), batchSize)
		}
		out.AppendRow(it.batch.Row(it.rowIdx))
		outRows++
		if out.N >= batchSize {
			if err := flushOutput(); err != nil {
				return nil, err
			}
		}
		// Advance the run this item came from.
		c := cursors[it.runIdx]
		if err := c.advance(ctx, keys); err != nil {
			return nil, err
		}
		if c.item != nil {
			heap.Push(h, c.item)
		}
		if h.err != nil {
			return nil, h.err
		}
	}
	if err := flushOutput(); err != nil {
		return nil, err
	}
	if spilling {
		if err := spillWriter.close(); err != nil {
			return nil, err
		}
		return newSpillRun(spillWriter.path, outRows), nil
	}
	return newMemRun(outBatches, outRows), nil
}

// mergeCursor tracks the current position within one run during a merge.
type mergeCursor struct {
	run    Run
	runIdx int
	item   *mergeItem
	seq    int64
}

func (c *mergeCursor) advance(ctx context.Context, keys []SortKey) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.item != nil && c.item.rowIdx+1 < c.item.batch.N {
			c.item.rowIdx++
			c.item.seq = c.seq
			c.seq++
			kv, err := keyValuesAt(keys, c.item.batch, c.item.rowIdx)
			if err != nil {
				return err
			}
			c.item.keys = kv
			return nil
		}
		b, err := c.run.NextBatch()
		if err != nil {
			return err
		}
		if b == nil {
			c.item = nil
			return nil
		}
		if b.N == 0 {
			continue
		}
		kv, err := keyValuesAt(keys, b, 0)
		if err != nil {
			return err
		}
		c.item = &mergeItem{
			runIdx: c.runIdx,
			batch:  b,
			rowIdx: 0,
			keys:   kv,
			seq:    c.seq,
		}
		c.seq++
		return nil
	}
}

// keyValuesAt evaluates sort keys for a specific row in a batch.
func keyValuesAt(keys []SortKey, b *vector.Batch, ri int) ([]types.Datum, error) {
	r := &row.Row{Vals: b.Row(ri)}
	kv := make([]types.Datum, len(keys))
	for i, k := range keys {
		v, err := k.Expr.Eval(r)
		if err != nil {
			return nil, err
		}
		kv[i] = v
	}
	return kv, nil
}

// rowKeyValues evaluates sort keys over a single row (used by tests and the
// row-path comparator helper).
func rowKeyValues(r *row.Row, keys []SortKey) ([]types.Datum, error) {
	kv := make([]types.Datum, len(keys))
	for i, k := range keys {
		v, err := k.Expr.Eval(r)
		if err != nil {
			return nil, err
		}
		kv[i] = v
	}
	return kv, nil
}

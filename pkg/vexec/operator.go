package vexec

import (
	"context"
	"fmt"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// BatchOperator is the vectorized iterator interface. Next returns (nil, nil)
// at end of stream. A non-nil but empty batch (N==0) means "no rows passed this
// batch, but the stream continues": downstream operators must keep draining.
type BatchOperator interface {
	Open(ctx context.Context) error
	Next() (*vector.Batch, error)
	Close() error
}

// ---- Scan ----

// ScanOp scans one partition of a table, producing columnar batches.
type ScanOp struct {
	table     *catalog.Table
	part      int
	batchSize int
	off       int
	ctx       context.Context
}

func NewScan(table *catalog.Table, part int, batchSize int) *ScanOp {
	if batchSize <= 0 {
		batchSize = 1024
	}
	return &ScanOp{table: table, part: part, batchSize: batchSize}
}

func (o *ScanOp) Open(ctx context.Context) error {
	o.ctx = ctx
	o.off = 0
	return nil
}

func (o *ScanOp) Next() (*vector.Batch, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	rows := o.table.Parts[o.part]
	if o.off >= len(rows) {
		return nil, nil
	}
	end := o.off + o.batchSize
	if end > len(rows) {
		end = len(rows)
	}
	b := vector.NewBatch(o.table.Schema.Types(), end-o.off)
	for i := o.off; i < end; i++ {
		b.AppendRow(rows[i])
	}
	o.off = end
	return b, nil
}

func (o *ScanOp) Close() error { return nil }

// ---- Filter ----

// FilterOp evaluates the predicate per batch and physically compacts the batch
// to the selected rows. Skipped rows never reach downstream operators.
type FilterOp struct {
	child BatchOperator
	pred  expr.Expr
	ctx   context.Context
}

func NewFilter(child BatchOperator, pred expr.Expr) *FilterOp {
	return &FilterOp{child: child, pred: pred}
}

func (o *FilterOp) Open(ctx context.Context) error {
	o.ctx = ctx
	return o.child.Open(ctx)
}

func (o *FilterOp) Next() (*vector.Batch, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	b, err := o.child.Next()
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	col, err := o.pred.EvalBatch(b)
	if err != nil {
		return nil, err
	}
	if col.Type != types.Bool {
		return nil, fmt.Errorf("filter predicate must yield bool, got %s", col.Type)
	}
	sel := make([]int, 0, b.N)
	for i := 0; i < b.N; i++ {
		if !col.Null[i] && col.Bool[i] {
			sel = append(sel, i)
		}
	}
	// Compact even when sel is empty: return an empty (non-nil) batch so the
	// downstream operator keeps making progress.
	return b.Compact(sel), nil
}

func (o *FilterOp) Close() error { return o.child.Close() }

// ---- Project ----

// ProjectOp evaluates expressions per batch to produce output columns.
type ProjectOp struct {
	child BatchOperator
	exprs []expr.Expr
	ctx   context.Context
}

func NewProject(child BatchOperator, exprs []expr.Expr) *ProjectOp {
	return &ProjectOp{child: child, exprs: exprs}
}

func (o *ProjectOp) Open(ctx context.Context) error {
	o.ctx = ctx
	return o.child.Open(ctx)
}

func (o *ProjectOp) Next() (*vector.Batch, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	b, err := o.child.Next()
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	cols := make([]*vector.Column, len(o.exprs))
	for i, e := range o.exprs {
		c, err := e.EvalBatch(b)
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}
	out := &vector.Batch{Cols: cols, N: b.N}
	return out, nil
}

func (o *ProjectOp) Close() error { return o.child.Close() }

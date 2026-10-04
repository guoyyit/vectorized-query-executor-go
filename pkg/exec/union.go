package exec

import (
	"context"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
)

// UnionScanOp scans all partitions of a table in order, presenting them as a
// single row stream to the row-by-row path.
type UnionScanOp struct {
	table *catalog.Table
	part  int
	off   int
	ctx   context.Context
}

func NewUnionScan(table *catalog.Table) *UnionScanOp {
	return &UnionScanOp{table: table, part: 0, off: 0}
}

func (o *UnionScanOp) Open(ctx context.Context) error {
	o.ctx = ctx
	o.part = 0
	o.off = 0
	return nil
}

func (o *UnionScanOp) Next() (*row.Row, error) {
	for {
		if err := o.ctx.Err(); err != nil {
			return nil, err
		}
		if o.part >= len(o.table.Parts) {
			return nil, nil
		}
		if o.off >= len(o.table.Parts[o.part]) {
			o.part++
			o.off = 0
			continue
		}
		r := &row.Row{Vals: o.table.Parts[o.part][o.off]}
		o.off++
		return r, nil
	}
}

func (o *UnionScanOp) Close() error { return nil }

// Package exec implements the row-by-row Volcano-style reference execution
// path. Every operator pulls one row at a time; this is the semantic baseline
// that the vectorized path must match exactly.
package exec

import (
	"context"
	"sort"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

// Operator is the Volcano iterator interface. Next returns (nil, nil) at EOF.
type Operator interface {
	Open(ctx context.Context) error
	Next() (*row.Row, error)
	Close() error
}

// ---- Scan ----

// ScanOp scans one partition of a table.
type ScanOp struct {
	table *catalog.Table
	part  int
	off   int
	ctx   context.Context
}

func NewScan(table *catalog.Table, part int) *ScanOp {
	return &ScanOp{table: table, part: part}
}

func (o *ScanOp) Open(ctx context.Context) error {
	o.ctx = ctx
	o.off = 0
	return nil
}

func (o *ScanOp) Next() (*row.Row, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	if o.off >= len(o.table.Parts[o.part]) {
		return nil, nil
	}
	r := &row.Row{Vals: o.table.Parts[o.part][o.off]}
	o.off++
	return r, nil
}

func (o *ScanOp) Close() error { return nil }

// ---- Filter ----

// FilterOp passes rows for which the predicate evaluates to TRUE. NULL and
// FALSE both skip the row; skipped rows are never pulled downstream.
type FilterOp struct {
	child Operator
	pred  expr.Expr
	ctx   context.Context
}

func NewFilter(child Operator, pred expr.Expr) *FilterOp {
	return &FilterOp{child: child, pred: pred}
}

func (o *FilterOp) Open(ctx context.Context) error {
	o.ctx = ctx
	return o.child.Open(ctx)
}

func (o *FilterOp) Next() (*row.Row, error) {
	for {
		if err := o.ctx.Err(); err != nil {
			return nil, err
		}
		r, err := o.child.Next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, nil
		}
		v, err := o.pred.Eval(r)
		if err != nil {
			return nil, err
		}
		if v.Truthy() {
			return r, nil
		}
		// row skipped: loop continues; the child never sees this row again.
	}
}

func (o *FilterOp) Close() error { return o.child.Close() }

// ---- Project ----

// ProjectOp evaluates expressions to produce output columns.
type ProjectOp struct {
	child Operator
	exprs []expr.Expr
	ctx   context.Context
}

func NewProject(child Operator, exprs []expr.Expr) *ProjectOp {
	return &ProjectOp{child: child, exprs: exprs}
}

func (o *ProjectOp) Open(ctx context.Context) error {
	o.ctx = ctx
	return o.child.Open(ctx)
}

func (o *ProjectOp) Next() (*row.Row, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	r, err := o.child.Next()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	out := row.NewRow(len(o.exprs))
	for i, e := range o.exprs {
		v, err := e.Eval(r)
		if err != nil {
			return nil, err
		}
		out.Vals[i] = v
	}
	return out, nil
}

func (o *ProjectOp) Close() error { return o.child.Close() }

// ---- Sort ----

// SortKey is an ORDER BY column.
type SortKey struct {
	Expr expr.Expr
	Desc bool
}

// SortOp materializes all input rows and sorts them with a stable comparator.
type SortOp struct {
	child Operator
	keys  []SortKey
	rows  []*row.Row
	pos   int
	ctx   context.Context
}

func NewSort(child Operator, keys []SortKey) *SortOp {
	return &SortOp{child: child, keys: keys}
}

func (o *SortOp) Open(ctx context.Context) error {
	o.ctx = ctx
	if err := o.child.Open(ctx); err != nil {
		return err
	}
	o.rows = nil
	o.pos = 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := o.child.Next()
		if err != nil {
			return err
		}
		if r == nil {
			break
		}
		o.rows = append(o.rows, r.Clone())
	}
	sort.SliceStable(o.rows, func(i, j int) bool {
		for _, k := range o.keys {
			vi, err := k.Expr.Eval(o.rows[i])
			if err != nil {
				return false
			}
			vj, err := k.Expr.Eval(o.rows[j])
			if err != nil {
				return false
			}
			cmp, err := types.Compare(vi, vj)
			if err != nil {
				// Type errors should have been caught at planning time; keep
				// the comparator total-ordered and surface the error later.
				return false
			}
			if cmp != 0 {
				if k.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
	return nil
}

func (o *SortOp) Next() (*row.Row, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	if o.pos >= len(o.rows) {
		return nil, nil
	}
	r := o.rows[o.pos]
	o.pos++
	return r, nil
}

func (o *SortOp) Close() error {
	o.rows = nil
	return o.child.Close()
}

// ---- Aggregate ----

// AggregateOp groups rows by key expressions and computes aggregates. Groups
// are emitted in deterministic (encoded-key) order.
type AggregateOp struct {
	child    Operator
	groupBy  []expr.Expr
	aggs     []agg.Def
	inputSch []types.T
	groups   map[string]*agg.State
	keys     []string
	keyTypes []types.T
	pos      int
	ctx      context.Context
}

func NewAggregate(child Operator, groupBy []expr.Expr, aggs []agg.Def, inputSch []types.T) *AggregateOp {
	return &AggregateOp{child: child, groupBy: groupBy, aggs: aggs, inputSch: inputSch}
}

func (o *AggregateOp) Open(ctx context.Context) error {
	o.ctx = ctx
	if err := o.child.Open(ctx); err != nil {
		return err
	}
	o.groups = make(map[string]*agg.State)
	o.keys = nil
	o.pos = 0
	for _, e := range o.groupBy {
		t, err := e.InferType(o.inputSch)
		if err != nil {
			return err
		}
		o.keyTypes = append(o.keyTypes, t)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := o.child.Next()
		if err != nil {
			return err
		}
		if r == nil {
			break
		}
		keyVals := make([]types.Datum, len(o.groupBy))
		for i, e := range o.groupBy {
			v, err := e.Eval(r)
			if err != nil {
				return err
			}
			keyVals[i] = v
		}
		key := agg.EncodeKey(keyVals)
		st, ok := o.groups[key]
		if !ok {
			st = &agg.State{}
			o.groups[key] = st
			o.keys = append(o.keys, key)
		}
		if err := st.AddRow(r, o.aggs); err != nil {
			return err
		}
	}
	sort.Strings(o.keys)
	return nil
}

func (o *AggregateOp) Next() (*row.Row, error) {
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	if o.pos >= len(o.keys) {
		return nil, nil
	}
	key := o.keys[o.pos]
	o.pos++
	keyVals, err := agg.DecodeKey(key, o.keyTypes)
	if err != nil {
		return nil, err
	}
	st := o.groups[key]
	aggVals := st.Result(o.aggs, o.inputSch)
	out := row.NewRow(len(keyVals) + len(aggVals))
	copy(out.Vals, keyVals)
	copy(out.Vals[len(keyVals):], aggVals)
	return out, nil
}

func (o *AggregateOp) Close() error {
	o.groups = nil
	o.keys = nil
	return o.child.Close()
}

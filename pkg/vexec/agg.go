package vexec

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// ErrMemoryQuotaExceeded is returned when the aggregate hash table cannot
// reserve memory within the configured budget. It is a distinguishable failure
// cause: data is not silently dropped and the query fails fast.
var ErrMemoryQuotaExceeded = errors.New("memory quota exceeded")

// groupOverhead estimates the per-group map/state overhead in bytes.
const groupOverhead = 128

// LocalAgg is a single-partition aggregate table. It is not safe for
// concurrent use; each partition worker owns one. After all partitions finish,
// partial states are merged in partition order (see MergePartials) so that
// floating-point sums accumulate in the same order as the row-by-row path.
type LocalAgg struct {
	m        map[string]*agg.State
	defs     []agg.Def
	inputSch []types.T
	acc      *Accountant
	reserved int64
}

// NewLocalAgg creates a local aggregate table.
func NewLocalAgg(defs []agg.Def, inputSch []types.T, acc *Accountant) *LocalAgg {
	return &LocalAgg{
		m:        make(map[string]*agg.State),
		defs:     defs,
		inputSch: inputSch,
		acc:      acc,
	}
}

// AddBatch accumulates one columnar batch. Key and aggregate expressions are
// evaluated in batch (vectorized); only the hash-table probe is per-row.
func (a *LocalAgg) AddBatch(ctx context.Context, b *vector.Batch, groupBy []expr.Expr) error {
	if b.N == 0 {
		return nil
	}
	keys, err := agg.EvalBatchKeys(groupBy, b)
	if err != nil {
		return err
	}
	// Evaluate aggregate expressions once per batch.
	aggCols := make([]*vector.Column, len(a.defs))
	for i, d := range a.defs {
		if d.Func == agg.CountStar {
			continue
		}
		c, err := d.Expr.EvalBatch(b)
		if err != nil {
			return err
		}
		aggCols[i] = c
	}

	for ri := 0; ri < b.N; ri++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := keys[ri]
		st, ok := a.m[key]
		if !ok {
			if a.acc.Budget() > 0 {
				est := int64(len(key)) + groupOverhead
				if !a.acc.Reserve(est) {
					return fmt.Errorf("%w: aggregate group table (budget %d bytes)", ErrMemoryQuotaExceeded, a.acc.Budget())
				}
				a.reserved += est
			}
			st = &agg.State{}
			a.m[key] = st
		}
		for i, d := range a.defs {
			if d.Func == agg.CountStar {
				st.CountStar++
				continue
			}
			if err := st.AddDatum(i, d, aggCols[i].Get(ri)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Release returns all reserved memory to the accountant.
func (a *LocalAgg) Release() {
	if a.reserved > 0 {
		a.acc.Release(a.reserved)
		a.reserved = 0
	}
}

// MergePartials merges per-partition partial aggregates in partition order and
// returns the final groups in deterministic encoded-key order. Merging in
// partition order guarantees that floating-point sums accumulate in the same
// order as the row-by-row path (float addition is not associative).
func MergePartials(partials []*LocalAgg, groupBy []expr.Expr) ([]*row.Row, error) {
	final := make(map[string]*agg.State)
	for _, p := range partials {
		for key, st := range p.m {
			dst, ok := final[key]
			if !ok {
				dst = &agg.State{}
				final[key] = dst
			}
			if err := dst.Merge(st, p.defs, p.inputSch); err != nil {
				return nil, err
			}
		}
	}

	// Deterministic output order.
	keys := make([]string, 0, len(final))
	for k := range final {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	keyTypes := make([]types.T, len(groupBy))
	for i, e := range groupBy {
		t, err := e.InferType(partials[0].inputSch)
		if err != nil {
			return nil, err
		}
		keyTypes[i] = t
	}

	out := make([]*row.Row, 0, len(keys))
	for _, k := range keys {
		keyVals, err := agg.DecodeKey(k, keyTypes)
		if err != nil {
			return nil, err
		}
		st := final[k]
		aggVals := st.Result(partials[0].defs, partials[0].inputSch)
		r := row.NewRow(len(keyVals) + len(aggVals))
		copy(r.Vals, keyVals)
		copy(r.Vals[len(keyVals):], aggVals)
		out = append(out, r)
	}
	return out, nil
}

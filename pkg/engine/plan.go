package engine

import (
	"fmt"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/agg"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/catalog"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

// Plan describes a query: scan -> filter -> project -> (aggregate | sort).
// All fields are optional except Table.
type Plan struct {
	Table       *catalog.Table
	Filter      expr.Expr    // optional predicate
	Projections []expr.Expr  // optional; nil means all table columns
	GroupBy     []expr.Expr  // optional; presence enables aggregation
	Aggregates  []agg.Def    // optional aggregate definitions
	OrderBy     []vexec.SortKey // optional ORDER BY (non-aggregate output)
	OrderByOut  []OutSortKey    // optional ORDER BY on aggregate output columns
}

// OutSortKey sorts aggregate output by column index.
type OutSortKey struct {
	Index int
	Desc  bool
}

// HasAggregate reports whether the plan contains an aggregate stage.
func (p *Plan) HasAggregate() bool { return len(p.GroupBy) > 0 || len(p.Aggregates) > 0 }

// validate checks the plan against the table schema and returns the output
// schema.
func (p *Plan) validate() ([]types.T, error) {
	if p.Table == nil {
		return nil, fmt.Errorf("plan: table is required")
	}
	inputSch := p.Table.Schema.Types()
	sch := inputSch

	if p.Filter != nil {
		t, err := p.Filter.InferType(sch)
		if err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}
		if t != types.Bool {
			return nil, fmt.Errorf("filter predicate must yield bool, got %s", t)
		}
	}

	if p.Projections != nil {
		sch = make([]types.T, len(p.Projections))
		for i, e := range p.Projections {
			t, err := e.InferType(inputSch)
			if err != nil {
				return nil, fmt.Errorf("projection %d: %w", i, err)
			}
			sch[i] = t
		}
	}

	if p.HasAggregate() {
		outSch := make([]types.T, 0, len(p.GroupBy)+len(p.Aggregates))
		for i, e := range p.GroupBy {
			t, err := e.InferType(sch)
			if err != nil {
				return nil, fmt.Errorf("group key %d: %w", i, err)
			}
			outSch = append(outSch, t)
		}
		for i, d := range p.Aggregates {
			t, err := d.ResultType(sch)
			if err != nil {
				return nil, fmt.Errorf("aggregate %d: %w", i, err)
			}
			outSch = append(outSch, t)
		}
		for _, k := range p.OrderByOut {
			if k.Index < 0 || k.Index >= len(outSch) {
				return nil, fmt.Errorf("order by output index %d out of range [0,%d)", k.Index, len(outSch))
			}
		}
		return outSch, nil
	}

	for _, k := range p.OrderBy {
		t, err := k.Expr.InferType(sch)
		if err != nil {
			return nil, fmt.Errorf("order by: %w", err)
		}
		_ = t
	}
	return sch, nil
}

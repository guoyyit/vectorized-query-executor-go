package engine

import (
	"context"
	"sort"
	"sync"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/exec"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vexec"
)

// Result is the output of a query.
type Result struct {
	Schema []types.T
	Rows   []*row.Row
}

// ---- Row-by-row reference path ----

// ExecuteRow runs the plan through the Volcano-style row-at-a-time path.
func ExecuteRow(ctx context.Context, cfg Config, plan *Plan) (*Result, error) {
	cfg.normalize()
	outSch, err := plan.validate()
	if err != nil {
		return nil, err
	}
	inputSch := plan.Table.Schema.Types()

	var op exec.Operator = exec.NewUnionScan(plan.Table)
	if plan.Filter != nil {
		op = exec.NewFilter(op, plan.Filter)
	}
	if plan.Projections != nil {
		op = exec.NewProject(op, plan.Projections)
	}
	if plan.HasAggregate() {
		op = exec.NewAggregate(op, plan.GroupBy, plan.Aggregates, inputSch)
	}
	if len(plan.OrderBy) > 0 {
		keys := make([]exec.SortKey, len(plan.OrderBy))
		for i, k := range plan.OrderBy {
			keys[i] = exec.SortKey{Expr: k.Expr, Desc: k.Desc}
		}
		op = exec.NewSort(op, keys)
	}
	if len(plan.OrderByOut) > 0 {
		keys := make([]exec.SortKey, len(plan.OrderByOut))
		for i, k := range plan.OrderByOut {
			keys[i] = exec.SortKey{Expr: expr.Col(k.Index), Desc: k.Desc}
		}
		op = exec.NewSort(op, keys)
	}

	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	defer op.Close()

	var rows []*row.Row
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r, err := op.Next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		rows = append(rows, r)
	}
	return &Result{Schema: outSch, Rows: rows}, nil
}

// ---- Vectorized path ----

// ExecuteVector runs the plan through the batch-at-a-time vectorized path.
// Partitions are processed concurrently by a worker pool; sorted runs are
// merged with a k-way heap merge; aggregates use per-partition partial states
// merged in partition order for deterministic float sums.
func ExecuteVector(ctx context.Context, cfg Config, plan *Plan) (*Result, error) {
	cfg.normalize()
	outSch, err := plan.validate()
	if err != nil {
		return nil, err
	}
	inputSch := plan.Table.Schema.Types()

	acc := vexec.NewAccountant(cfg.MemoryBudget)
	spill, err := vexec.NewSpillDir(cfg.SpillDir)
	if err != nil {
		return nil, err
	}
	defer spill.Cleanup()

	if plan.HasAggregate() {
		return executeVectorAgg(ctx, cfg, plan, inputSch, outSch, acc, spill)
	}
	if len(plan.OrderBy) > 0 {
		return executeVectorSort(ctx, cfg, plan, inputSch, acc, spill)
	}
	return executeVectorPlain(ctx, cfg, plan, inputSch)
}

// buildPartitionChain constructs the per-partition vectorized pipeline
// (scan -> filter -> project).
func buildPartitionChain(plan *Plan, inputSch []types.T, part int, batchSize int) (vexec.BatchOperator, error) {
	var op vexec.BatchOperator = vexec.NewScan(plan.Table, part, batchSize)
	if plan.Filter != nil {
		op = vexec.NewFilter(op, plan.Filter)
	}
	if plan.Projections != nil {
		op = vexec.NewProject(op, plan.Projections)
	}
	return op, nil
}

// startWorkers runs fn for each partition using a worker pool of
// cfg.Parallelism workers. Partitions are claimed in order, which avoids the
// deadlock where a sequencer waits for an early partition whose worker is
// blocked on a semaphore held by a later partition. It returns a wait function
// that blocks until all workers finish. The caller must run any concurrent
// consumer (e.g. the plain-path sequencer) BEFORE calling wait.
func startWorkers(cfg Config, numParts int, fn func(p int)) func() {
	var nextPart int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < cfg.Parallelism; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				p := nextPart
				nextPart++
				mu.Unlock()
				if p >= numParts {
					return
				}
				fn(p)
			}
		}()
	}
	return wg.Wait
}

func sendErr(errCh chan error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

// drainRun collects all rows from a sorted run.
func drainRun(ctx context.Context, run vexec.Run) ([]*row.Row, error) {
	var rows []*row.Row
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := run.NextBatch()
		if err != nil {
			return nil, err
		}
		if b == nil {
			break
		}
		for i := 0; i < b.N; i++ {
			rows = append(rows, &row.Row{Vals: b.Row(i)})
		}
	}
	return rows, nil
}

// executeVectorSort: each partition produces a sorted run (spilling to disk
// when the budget is exceeded); runs are merged with a k-way heap merge.
func executeVectorSort(ctx context.Context, cfg Config, plan *Plan, inputSch []types.T, acc *vexec.Accountant, spill *vexec.SpillDir) (*Result, error) {
	numParts := plan.Table.NumPartitions()
	runs := make([]vexec.Run, numParts)
	errCh := make(chan error, 1)

	wait := startWorkers(cfg, numParts, func(p int) {
		child, err := buildPartitionChain(plan, inputSch, p, cfg.BatchSize)
		if err != nil {
			sendErr(errCh, err)
			return
		}
		sortOp := vexec.NewSort(child, plan.OrderBy, cfg.BatchSize, acc, spill)
		if err := sortOp.Open(ctx); err != nil {
			sendErr(errCh, err)
			return
		}
		runs[p] = sortOp
	})
	wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	runList := make([]vexec.Run, 0, numParts)
	for _, r := range runs {
		if r != nil {
			runList = append(runList, r)
		}
	}
	merged, err := vexec.MergeRuns(ctx, runList, plan.OrderBy, cfg.BatchSize, acc, spill)
	if err != nil {
		return nil, err
	}
	defer merged.Close()

	rows, err := drainRun(ctx, merged)
	if err != nil {
		return nil, err
	}
	return &Result{Schema: outSchema(plan, inputSch), Rows: rows}, nil
}

// executeVectorPlain: filter/project only. Partitions run concurrently and
// feed per-partition channels; a sequencer reads partitions in order so the
// output matches the row-by-row path.
func executeVectorPlain(ctx context.Context, cfg Config, plan *Plan, inputSch []types.T) (*Result, error) {
	numParts := plan.Table.NumPartitions()
	chs := make([]chan *vector.Batch, numParts)
	for p := 0; p < numParts; p++ {
		chs[p] = make(chan *vector.Batch, 4)
	}
	errCh := make(chan error, 1)

	wait := startWorkers(cfg, numParts, func(p int) {
		defer close(chs[p])
		op, err := buildPartitionChain(plan, inputSch, p, cfg.BatchSize)
		if err != nil {
			sendErr(errCh, err)
			return
		}
		if err := op.Open(ctx); err != nil {
			sendErr(errCh, err)
			return
		}
		defer op.Close()
		for {
			if err := ctx.Err(); err != nil {
				sendErr(errCh, err)
				return
			}
			b, err := op.Next()
			if err != nil {
				sendErr(errCh, err)
				return
			}
			if b == nil {
				return
			}
			select {
			case chs[p] <- b:
			case <-ctx.Done():
				return
			}
		}
	})

	// Sequencer: read partitions in order while workers are running.
	var rows []*row.Row
seq:
	for p := 0; p < numParts; p++ {
		for {
			select {
			case <-ctx.Done():
				wait()
				return nil, ctx.Err()
			case b, ok := <-chs[p]:
				if !ok {
					continue seq
				}
				for i := 0; i < b.N; i++ {
					rows = append(rows, &row.Row{Vals: b.Row(i)})
				}
			}
		}
	}
	wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	return &Result{Schema: outSchema(plan, inputSch), Rows: rows}, nil
}

// executeVectorAgg: concurrent per-partition aggregation with ordered merge.
// Each partition accumulates its own partial aggregate; partials are merged in
// partition order so floating-point sums match the row-by-row path exactly.
func executeVectorAgg(ctx context.Context, cfg Config, plan *Plan, inputSch []types.T, outSch []types.T, acc *vexec.Accountant, spill *vexec.SpillDir) (*Result, error) {
	numParts := plan.Table.NumPartitions()
	partials := make([]*vexec.LocalAgg, numParts)
	errCh := make(chan error, 1)

	wait := startWorkers(cfg, numParts, func(p int) {
		local := vexec.NewLocalAgg(plan.Aggregates, inputSch, acc)
		partials[p] = local
		defer local.Release()

		op, err := buildPartitionChain(plan, inputSch, p, cfg.BatchSize)
		if err != nil {
			sendErr(errCh, err)
			return
		}
		if err := op.Open(ctx); err != nil {
			sendErr(errCh, err)
			return
		}
		defer op.Close()
		for {
			if err := ctx.Err(); err != nil {
				sendErr(errCh, err)
				return
			}
			b, err := op.Next()
			if err != nil {
				sendErr(errCh, err)
				return
			}
			if b == nil {
				return
			}
			if err := local.AddBatch(ctx, b, plan.GroupBy); err != nil {
				sendErr(errCh, err)
				return
			}
		}
	})
	wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	rows, err := vexec.MergePartials(partials, plan.GroupBy)
	if err != nil {
		return nil, err
	}
	if len(plan.OrderByOut) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			for _, k := range plan.OrderByOut {
				cmp, err := types.Compare(rows[i].Vals[k.Index], rows[j].Vals[k.Index])
				if err != nil {
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
	}
	return &Result{Schema: outSch, Rows: rows}, nil
}

// outSchema returns the output schema for non-aggregate plans.
func outSchema(plan *Plan, inputSch []types.T) []types.T {
	if plan.Projections != nil {
		sch := make([]types.T, len(plan.Projections))
		for i, e := range plan.Projections {
			t, err := e.InferType(inputSch)
			if err != nil {
				return nil
			}
			sch[i] = t
		}
		return sch
	}
	return inputSch
}

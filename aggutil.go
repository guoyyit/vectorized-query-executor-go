package executor

import (
	"fmt"
	"sort"
)

// aggGroup holds the partial aggregate state for one group.
type aggGroup struct {
	key    []Value
	states []aggState
}

// aggState is the mergeable state of one aggregate function.
type aggState struct {
	count int64 // rows seen (COUNT) / non-null values seen (AVG)
	sum   Value // numeric accumulator (SUM, AVG); Null until first value
	min   Value
	max   Value
	has   bool // any non-null value seen (SUM/MIN/MAX)
}

// groupMemEstimate approximates bytes held by one group for accounting.
func groupMemEstimate(key []Value, nAggs int) int64 {
	n := int64(64 + 48*nAggs)
	for _, v := range key {
		n += int64(24 + len(v.Str))
	}
	return n
}

// keyString encodes a group key into a comparable string.
func keyString(vals []Value) string {
	var sb []byte
	for _, v := range vals {
		switch {
		case v.Null:
			sb = append(sb, 'n', ';')
		case v.Typ == TypeInt64:
			sb = append(sb, fmt.Sprintf("i:%d;", v.I64)...)
		case v.Typ == TypeFloat64:
			sb = append(sb, fmt.Sprintf("f:%x;", v.F64)...)
		case v.Typ == TypeString:
			sb = append(sb, fmt.Sprintf("s:%d:%s;", len(v.Str), v.Str)...)
		case v.Typ == TypeBool:
			if v.B {
				sb = append(sb, "b:1;"...)
			} else {
				sb = append(sb, "b:0;"...)
			}
		}
	}
	return string(sb)
}

// aggregator maintains hash-group state. Memory policy: the number of groups
// is bounded by the memory budget; exceeding it fails with ErrMemoryLimit
// (no spill for aggregation). It is NOT safe for concurrent use; callers
// serialize access.
type aggregator struct {
	groupBy []int
	aggs    []AggSpec
	ectx    *ExecCtx

	groups  map[string]*aggGroup
	memUsed int64
}

func newAggregator(groupBy []int, aggs []AggSpec, ectx *ExecCtx) *aggregator {
	return &aggregator{
		groupBy: groupBy,
		aggs:    aggs,
		ectx:    ectx,
		groups:  make(map[string]*aggGroup),
	}
}

// groupOf extracts the group key of a row.
func (a *aggregator) groupOf(r Row) ([]Value, error) {
	key := make([]Value, len(a.groupBy))
	for i, c := range a.groupBy {
		if c < 0 || c >= len(r) {
			return nil, ErrSchema
		}
		key[i] = r[c]
	}
	return key, nil
}

// addRow folds one row into its group.
func (a *aggregator) addRow(r Row) error {
	key, err := a.groupOf(r)
	if err != nil {
		return err
	}
	ks := keyString(key)
	g, ok := a.groups[ks]
	if !ok {
		mem := groupMemEstimate(key, len(a.aggs))
		if err := a.ectx.Mem.Reserve(mem); err != nil {
			return err
		}
		a.memUsed += mem
		g = &aggGroup{key: key, states: make([]aggState, len(a.aggs))}
		a.groups[ks] = g
	}
	for i, spec := range a.aggs {
		if err := updateAggState(&g.states[i], spec, r); err != nil {
			return err
		}
	}
	return nil
}

// updateAggState folds one input row into one aggregate state.
func updateAggState(st *aggState, spec AggSpec, r Row) error {
	var v Value
	isCountStar := spec.Func == AggCount && spec.Col < 0
	if !isCountStar {
		if spec.Col < 0 || spec.Col >= len(r) {
			return ErrSchema
		}
		v = r[spec.Col]
	}
	switch spec.Func {
	case AggCount:
		if isCountStar || !v.Null {
			st.count++
		}
	case AggSum, AggAvg:
		if v.Null {
			return nil
		}
		if v.Typ != TypeInt64 && v.Typ != TypeFloat64 {
			return ErrTypeMismatch
		}
		st.count++
		if !st.has {
			st.sum = v
			st.has = true
			return nil
		}
		s, err := Add(st.sum, v)
		if err != nil {
			return err
		}
		st.sum = s
	case AggMin, AggMax:
		if v.Null {
			return nil
		}
		if !st.has {
			st.min, st.max = v, v
			st.has = true
			return nil
		}
		c, err := Compare(v, st.min)
		if err != nil {
			return err
		}
		if spec.Func == AggMin && c < 0 {
			st.min = v
		}
		c, err = Compare(v, st.max)
		if err != nil {
			return err
		}
		if spec.Func == AggMax && c > 0 {
			st.max = v
		}
	}
	return nil
}

// mergeState folds another partial state into st (used to combine partition
// partials). Deterministic regardless of input row order.
func mergeState(st *aggState, spec AggSpec, o *aggState) error {
	switch spec.Func {
	case AggCount:
		st.count += o.count
	case AggSum, AggAvg:
		st.count += o.count
		if !o.has {
			return nil
		}
		if !st.has {
			st.sum, st.has = o.sum, true
			return nil
		}
		s, err := Add(st.sum, o.sum)
		if err != nil {
			return err
		}
		st.sum = s
	case AggMin, AggMax:
		if !o.has {
			return nil
		}
		if !st.has {
			st.min, st.max, st.has = o.min, o.max, true
			return nil
		}
		c, err := Compare(o.min, st.min)
		if err != nil {
			return err
		}
		if spec.Func == AggMin && c < 0 {
			st.min = o.min
		}
		c, err = Compare(o.max, st.max)
		if err != nil {
			return err
		}
		if spec.Func == AggMax && c > 0 {
			st.max = o.max
		}
	}
	return nil
}

// merge folds all groups of o into a. Partition partials are merged in
// partition index order, which keeps results deterministic.
func (a *aggregator) merge(o *aggregator) error {
	for ks, og := range o.groups {
		g, ok := a.groups[ks]
		if !ok {
			mem := groupMemEstimate(og.key, len(a.aggs))
			if err := a.ectx.Mem.Reserve(mem); err != nil {
				return err
			}
			a.memUsed += mem
			g = &aggGroup{key: og.key, states: make([]aggState, len(a.aggs))}
			a.groups[ks] = g
		}
		for i, spec := range a.aggs {
			if err := mergeState(&g.states[i], spec, &og.states[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// finalize renders one aggregate state into an output value.
func finalizeAgg(st *aggState, spec AggSpec) (Value, error) {
	switch spec.Func {
	case AggCount:
		return IntVal(st.count), nil
	case AggSum:
		if !st.has {
			return NullOf(TypeInt64), nil
		}
		return st.sum, nil
	case AggAvg:
		if st.count == 0 {
			return NullOf(TypeFloat64), nil
		}
		return FloatVal(st.sum.asFloat() / float64(st.count)), nil
	case AggMin:
		if !st.has {
			return NullOf(TypeInt64), nil
		}
		return st.min, nil
	case AggMax:
		if !st.has {
			return NullOf(TypeInt64), nil
		}
		return st.max, nil
	}
	return Value{}, ErrTypeMismatch
}

// results emits one row per group: group key columns followed by aggregate
// results. Groups are sorted by key (NULLs first) so output is deterministic
// regardless of partition/concurrency. With no GROUP BY and no input rows a
// single row with empty-key aggregates is produced (SQL semantics).
func (a *aggregator) results() ([]Row, error) {
	if len(a.groups) == 0 && len(a.groupBy) > 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(a.groups))
	for ks := range a.groups {
		keys = append(keys, ks)
	}
	// Deterministic order: sort groups by key values via Compare.
	sort.Slice(keys, func(i, j int) bool {
		ka, kb := a.groups[keys[i]].key, a.groups[keys[j]].key
		for c := range ka {
			cmp, err := Compare(ka[c], kb[c])
			if err != nil {
				return keys[i] < keys[j]
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	if len(a.groups) == 0 {
		// single empty group (no GROUP BY, empty input)
		g := &aggGroup{key: nil, states: make([]aggState, len(a.aggs))}
		row, err := emitGroup(g, a.aggs)
		if err != nil {
			return nil, err
		}
		return []Row{row}, nil
	}
	out := make([]Row, 0, len(keys))
	for _, ks := range keys {
		row, err := emitGroup(a.groups[ks], a.aggs)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func emitGroup(g *aggGroup, aggs []AggSpec) (Row, error) {
	row := make(Row, 0, len(g.key)+len(aggs))
	row = append(row, g.key...)
	for i, spec := range aggs {
		v, err := finalizeAgg(&g.states[i], spec)
		if err != nil {
			return nil, err
		}
		row = append(row, v)
	}
	return row, nil
}

// close releases the aggregator's reserved memory. Idempotent.
func (a *aggregator) close() {
	if a.memUsed > 0 {
		a.ectx.Mem.Release(a.memUsed)
		a.memUsed = 0
	}
	a.groups = nil
}

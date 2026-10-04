package executor

import "io"

// VecOp is a batch-at-a-time operator. Next returns io.EOF when exhausted.
// Returned batches always contain at least one selected row; when a whole
// input batch is filtered out the operator keeps pulling instead of
// stalling or terminating early.
type VecOp interface {
	Next() (*Batch, error)
	Close() error
}

// rowsToBatch packs rows into a fresh batch; column types come from types.
func rowsToBatch(rows []Row, types []Type) *Batch {
	n := len(rows)
	cols := make([]*Vector, len(types))
	for c, t := range types {
		cols[c] = NewVector(t, n)
	}
	for i, r := range rows {
		for c := range cols {
			cols[c].SetValue(i, r[c])
		}
	}
	return &Batch{Cols: cols, N: n}
}

// typesOfRow derives column types from a row's values.
func typesOfRow(r Row) []Type {
	ts := make([]Type, len(r))
	for i, v := range r {
		ts[i] = v.Typ
	}
	return ts
}

// vecScan emits table rows as batches of the configured size.
type vecScan struct {
	schema *Schema
	rows   []Row
	pos    int
	ectx   *ExecCtx
}

func (s *vecScan) Next() (*Batch, error) {
	if err := s.ectx.CheckCancel(); err != nil {
		return nil, err
	}
	if s.pos >= len(s.rows) {
		return nil, io.EOF
	}
	end := s.pos + s.ectx.BatchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}
	b := rowsToBatch(s.rows[s.pos:end], s.schema.Types)
	s.pos = end
	return b, nil
}
func (s *vecScan) Close() error { return nil }

// vecFilter evaluates pred per batch and installs a selection vector. Rows
// whose predicate is FALSE or NULL are deselected and never observed
// downstream. Batches with zero surviving rows are skipped internally.
type vecFilter struct {
	child VecOp
	pred  Expr
}

func (f *vecFilter) Next() (*Batch, error) {
	for {
		b, err := f.child.Next()
		if err != nil {
			return nil, err
		}
		pv, err := f.pred.evalVec(b)
		if err != nil {
			return nil, err
		}
		if pv.Typ != TypeBool {
			return nil, ErrTypeMismatch
		}
		var sel []int
		eachSelected(b, func(i int) {
			if !pv.Nulls[i] && pv.Bs[i] {
				sel = append(sel, i)
			}
		})
		if len(sel) == 0 {
			continue // whole batch filtered out: keep advancing
		}
		if len(sel) == b.N && b.Sel == nil {
			return b, nil // nothing filtered: avoid allocation
		}
		return &Batch{Cols: b.Cols, Sel: sel, N: b.N}, nil
	}
}
func (f *vecFilter) Close() error { return f.child.Close() }

// vecProject evaluates exprs over selected rows only; unselected rows are
// never evaluated and their output slots stay NULL.
type vecProject struct {
	child VecOp
	exprs []Expr
}

func (p *vecProject) Next() (*Batch, error) {
	b, err := p.child.Next()
	if err != nil {
		return nil, err
	}
	cols := make([]*Vector, len(p.exprs))
	for i, e := range p.exprs {
		v, err := e.evalVec(b)
		if err != nil {
			return nil, err
		}
		cols[i] = v
	}
	return &Batch{Cols: cols, Sel: b.Sel, N: b.N}, nil
}
func (p *vecProject) Close() error { return p.child.Close() }

// vecSort buffers selected input rows in a sortBuffer (spilling under memory
// pressure) and emits sorted rows in batches (stable).
type vecSort struct {
	child VecOp
	keys  []SortKey
	ectx  *ExecCtx

	buf    *sortBuffer
	loaded bool
	types  []Type
	err    error
}

// load drains the child into the sort buffer and prepares sorted output.
// It is called eagerly by the pipeline inside the partition goroutine so
// per-partition sorts advance concurrently.
func (s *vecSort) load() error {
	if s.loaded {
		return nil
	}
	s.buf = newSortBuffer(s.keys, s.ectx)
	for {
		if err := s.ectx.CheckCancel(); err != nil {
			return err
		}
		b, err := s.child.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		eachSelected(b, func(i int) {
			if s.bufErr() != nil {
				return
			}
			row := make(Row, len(b.Cols))
			for c, v := range b.Cols {
				row[c] = v.ValueAt(i)
			}
			s.setErr(s.buf.add(row))
		})
		if err := s.bufErr(); err != nil {
			return err
		}
	}
	if err := s.buf.finish(); err != nil {
		return err
	}
	s.loaded = true
	return nil
}

func (s *vecSort) Next() (*Batch, error) {
	if err := s.load(); err != nil {
		return nil, err
	}
	// emit one batch of sorted rows
	rows := make([]Row, 0, s.ectx.BatchSize)
	for len(rows) < s.ectx.BatchSize {
		if err := s.ectx.CheckCancel(); err != nil {
			return nil, err
		}
		r, err := s.buf.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if s.types == nil {
			s.types = typesOfRow(r)
		}
		rows = append(rows, r)
	}
	if len(rows) == 0 {
		return nil, io.EOF
	}
	return rowsToBatch(rows, s.types), nil
}

func (s *vecSort) bufErr() error { return s.err }
func (s *vecSort) setErr(err error) {
	if err != nil && s.err == nil {
		s.err = err
	}
}

func (s *vecSort) Close() error {
	err := s.child.Close()
	if s.buf != nil {
		s.buf.close()
	}
	return err
}

// vecAggregate maintains hash-group state over selected rows only; groups
// beyond the memory budget fail with ErrMemoryLimit (documented policy: no
// spill for agg).
type vecAggregate struct {
	child   VecOp
	groupBy []int
	aggs    []AggSpec
	ectx    *ExecCtx

	agg    *aggregator
	out    []Row
	pos    int
	loaded bool
	err    error
}

func (a *vecAggregate) Next() (*Batch, error) {
	if !a.loaded {
		a.agg = newAggregator(a.groupBy, a.aggs, a.ectx)
		for {
			if err := a.ectx.CheckCancel(); err != nil {
				return nil, err
			}
			b, err := a.child.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			var aggErr error
			eachSelected(b, func(i int) {
				if aggErr != nil {
					return
				}
				row := make(Row, len(b.Cols))
				for c, v := range b.Cols {
					row[c] = v.ValueAt(i)
				}
				aggErr = a.agg.addRow(row)
			})
			if aggErr != nil {
				return nil, aggErr
			}
		}
		out, err := a.agg.results()
		if err != nil {
			return nil, err
		}
		a.out = out
		a.loaded = true
	}
	if a.pos >= len(a.out) {
		return nil, io.EOF
	}
	end := a.pos + a.ectx.BatchSize
	if end > len(a.out) {
		end = len(a.out)
	}
	b := rowsToBatch(a.out[a.pos:end], typesOfRow(a.out[0]))
	a.pos = end
	return b, nil
}

func (a *vecAggregate) Close() error {
	err := a.child.Close()
	if a.agg != nil {
		a.agg.close()
	}
	return err
}

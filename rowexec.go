package executor

import "io"

// RowOp is a row-at-a-time (interpreted) operator. Next returns io.EOF when
// the stream is exhausted.
type RowOp interface {
	Next() (Row, error)
	Close() error
}

// rowScan iterates a slice of rows, checking cancellation per row so every
// downstream row pull stays responsive to cancel/timeout.
type rowScan struct {
	rows []Row
	pos  int
	ectx *ExecCtx
}

func (s *rowScan) Next() (Row, error) {
	if err := s.ectx.CheckCancel(); err != nil {
		return nil, err
	}
	if s.pos >= len(s.rows) {
		return nil, io.EOF
	}
	r := s.rows[s.pos]
	s.pos++
	return r, nil
}
func (s *rowScan) Close() error { return nil }

// rowFilter keeps rows for which pred evaluates to TRUE (NULL/FALSE drop).
type rowFilter struct {
	child RowOp
	pred  Expr
}

func (f *rowFilter) Next() (Row, error) {
	for {
		r, err := f.child.Next()
		if err != nil {
			return nil, err
		}
		v, err := f.pred.Eval(r)
		if err != nil {
			return nil, err
		}
		if v.Typ != TypeBool {
			return nil, ErrTypeMismatch
		}
		if !v.Null && v.B {
			return r, nil
		}
	}
}
func (f *rowFilter) Close() error { return f.child.Close() }

// rowProject evaluates exprs per input row.
type rowProject struct {
	child RowOp
	exprs []Expr
}

func (p *rowProject) Next() (Row, error) {
	r, err := p.child.Next()
	if err != nil {
		return nil, err
	}
	out := make(Row, len(p.exprs))
	for i, e := range p.exprs {
		v, err := e.Eval(r)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}
func (p *rowProject) Close() error { return p.child.Close() }

// rowSort buffers all input via a sortBuffer (spilling under memory
// pressure) and emits rows in key order (stable).
type rowSort struct {
	child  RowOp
	keys   []SortKey
	ectx   *ExecCtx
	buf    *sortBuffer
	loaded bool
}

// load drains the child into the sort buffer and prepares sorted output.
// It is called eagerly by the pipeline inside the partition goroutine so
// per-partition sorts advance concurrently.
func (s *rowSort) load() error {
	if s.loaded {
		return nil
	}
	s.buf = newSortBuffer(s.keys, s.ectx)
	for {
		if err := s.ectx.CheckCancel(); err != nil {
			return err
		}
		r, err := s.child.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := s.buf.add(r); err != nil {
			return err
		}
	}
	if err := s.buf.finish(); err != nil {
		return err
	}
	s.loaded = true
	return nil
}

func (s *rowSort) Next() (Row, error) {
	if err := s.load(); err != nil {
		return nil, err
	}
	if err := s.ectx.CheckCancel(); err != nil {
		return nil, err
	}
	return s.buf.next()
}

func (s *rowSort) Close() error {
	err := s.child.Close()
	if s.buf != nil {
		s.buf.close()
	}
	return err
}

// rowAggregate buffers group state in a hash map; exceeding the memory
// budget fails with ErrMemoryLimit (documented policy: no spill for agg).
type rowAggregate struct {
	child   RowOp
	groupBy []int
	aggs    []AggSpec
	ectx    *ExecCtx

	agg    *aggregator
	out    []Row
	pos    int
	loaded bool
}

func (a *rowAggregate) Next() (Row, error) {
	if !a.loaded {
		a.agg = newAggregator(a.groupBy, a.aggs, a.ectx)
		for {
			if err := a.ectx.CheckCancel(); err != nil {
				return nil, err
			}
			r, err := a.child.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if err := a.agg.addRow(r); err != nil {
				return nil, err
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
	r := a.out[a.pos]
	a.pos++
	return r, nil
}

func (a *rowAggregate) Close() error {
	err := a.child.Close()
	if a.agg != nil {
		a.agg.close()
	}
	return err
}

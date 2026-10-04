package executor

import (
	"container/heap"
	"encoding/gob"
	"io"
	"os"
	"sort"
)

// rowSize estimates the in-memory footprint of a row for accounting.
func rowSize(r Row) int64 {
	n := int64(16+len(r))*8 + 16
	for _, v := range r {
		n += int64(len(v.Str))
	}
	return n
}

// lessFn compares two rows by sort keys; it is strict-weak and leaves ties
// equal so sorts stay stable.
type lessFn func(a, b Row) bool

func makeLess(keys []SortKey, errOut *error) lessFn {
	return func(a, b Row) bool {
		if *errOut != nil {
			return false
		}
		for _, k := range keys {
			if k.Col < 0 || k.Col >= len(a) || k.Col >= len(b) {
				*errOut = ErrSchema
				return false
			}
			c, err := Compare(a[k.Col], b[k.Col])
			if err != nil {
				*errOut = err
				return false
			}
			if c == 0 {
				continue
			}
			if k.Desc {
				return c > 0
			}
			return c < 0
		}
		return false
	}
}

// sortBuffer buffers rows under a memory budget, spilling sorted runs to
// disk when the budget is exhausted, and merges runs on read. Spill files
// are removed by close.
type sortBuffer struct {
	keys []SortKey
	ectx *ExecCtx
	less lessFn
	err  error

	rows     []Row
	reserved int64
	runs     []string
	tmpFiles []*os.File

	// merge state
	out     []Row // in-memory case
	pos     int
	readers []*runReader
	h       mergeHeap
}

func newSortBuffer(keys []SortKey, ectx *ExecCtx) *sortBuffer {
	s := &sortBuffer{keys: keys, ectx: ectx}
	s.less = makeLess(keys, &s.err)
	return s
}

// add appends one row, spilling the current buffer if the memory budget is
// exhausted. A single row larger than the whole budget yields ErrMemoryLimit.
func (s *sortBuffer) add(r Row) error {
	sz := rowSize(r)
	if err := s.ectx.Mem.Reserve(sz); err != nil {
		if len(s.rows) == 0 {
			return ErrMemoryLimit
		}
		if err := s.spill(); err != nil {
			return err
		}
		if err := s.ectx.Mem.Reserve(sz); err != nil {
			return err
		}
	}
	s.rows = append(s.rows, r)
	s.reserved += sz
	return nil
}

// spill sorts the buffered rows and writes them to a new run file.
func (s *sortBuffer) spill() error {
	if len(s.rows) == 0 {
		return nil
	}
	sort.SliceStable(s.rows, func(i, j int) bool { return s.less(s.rows[i], s.rows[j]) })
	if s.err != nil {
		return s.err
	}
	dir := s.ectx.TempDir
	if dir == "" {
		dir = os.TempDir()
	}
	f, err := os.CreateTemp(dir, "vqsort-run-*")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	for _, r := range s.rows {
		if err := enc.Encode([]Value(r)); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	s.tmpFiles = append(s.tmpFiles, f)
	s.runs = append(s.runs, f.Name())
	s.ectx.Mem.Release(s.reserved)
	s.rows = s.rows[:0]
	s.reserved = 0
	return nil
}

// finish prepares the buffer for reading: sorts the final run and, if any
// runs were spilled, spills the tail and opens all runs for merging.
func (s *sortBuffer) finish() error {
	if s.err != nil {
		return s.err
	}
	if len(s.runs) == 0 {
		sort.SliceStable(s.rows, func(i, j int) bool { return s.less(s.rows[i], s.rows[j]) })
		if s.err != nil {
			return s.err
		}
		s.out = s.rows
		s.rows = nil
		return nil
	}
	if err := s.spill(); err != nil {
		return err
	}
	for _, f := range s.tmpFiles {
		s.readers = append(s.readers, &runReader{dec: gob.NewDecoder(f)})
	}
	s.h = mergeHeap{less: s.less}
	for i, r := range s.readers {
		row, err := r.next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		heap.Push(&s.h, mergeItem{row: row, run: i})
	}
	return nil
}

// next returns the next row in sorted order, or io.EOF.
func (s *sortBuffer) next() (Row, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.out != nil {
		if s.pos >= len(s.out) {
			return nil, io.EOF
		}
		r := s.out[s.pos]
		s.pos++
		return r, nil
	}
	if s.h.Len() == 0 {
		return nil, io.EOF
	}
	it := heap.Pop(&s.h).(mergeItem)
	nxt, err := s.readers[it.run].next()
	if err == nil {
		heap.Push(&s.h, mergeItem{row: nxt, run: it.run})
	} else if err != io.EOF {
		return nil, err
	}
	return it.row, nil
}

// close releases memory and removes spill files. Idempotent.
func (s *sortBuffer) close() error {
	if s.reserved > 0 {
		s.ectx.Mem.Release(s.reserved)
		s.reserved = 0
	}
	s.rows = nil
	s.out = nil
	for _, f := range s.tmpFiles {
		f.Close()
	}
	for _, p := range s.runs {
		os.Remove(p)
	}
	s.runs = nil
	s.tmpFiles = nil
	s.readers = nil
	s.h = mergeHeap{}
	return nil
}

// runReader streams rows from one spill file.
type runReader struct {
	dec *gob.Decoder
}

func (r *runReader) next() (Row, error) {
	var vals []Value
	if err := r.dec.Decode(&vals); err != nil {
		return nil, err
	}
	return Row(vals), nil
}

// mergeItem is one heap entry; ties break toward the earlier run so the
// merge is stable (runs contain rows in original arrival order).
type mergeItem struct {
	row Row
	run int
}

type mergeHeap struct {
	items []mergeItem
	less  lessFn
}

func (h *mergeHeap) Len() int { return len(h.items) }
func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.less(a.row, b.row) {
		return true
	}
	if h.less(b.row, a.row) {
		return false
	}
	return a.run < b.run
}
func (h *mergeHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap) Push(x interface{}) { h.items = append(h.items, x.(mergeItem)) }
func (h *mergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	return it
}

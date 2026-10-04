package vexec

import (
	"sync"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// Accountant tracks memory usage against a configurable budget. Operators
// reserve before buffering rows and release when buffers are freed. A failed
// reservation means the operator must spill (sort) or fail with a
// distinguishable quota error (aggregate).
type Accountant struct {
	mu     sync.Mutex
	budget int64
	used   int64
}

// NewAccountant creates an accountant with the given byte budget. budget <= 0
// disables accounting (unbounded).
func NewAccountant(budget int64) *Accountant {
	return &Accountant{budget: budget}
}

// Budget returns the configured budget.
func (a *Accountant) Budget() int64 { return a.budget }

// Used returns the currently reserved bytes.
func (a *Accountant) Used() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}

// Reserve reserves n bytes. Returns false when the budget would be exceeded.
func (a *Accountant) Reserve(n int64) bool {
	if a.budget <= 0 {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.used+n > a.budget {
		return false
	}
	a.used += n
	return true
}

// Release returns n bytes to the budget.
func (a *Accountant) Release(n int64) {
	if a.budget <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.used -= n
	if a.used < 0 {
		a.used = 0
	}
}

// EstimateBatchBytes estimates the in-memory footprint of a batch, including
// fixed column storage and actual string data.
func EstimateBatchBytes(b *vector.Batch) int64 {
	var n int64
	for _, c := range b.Cols {
		n += int64(len(c.Null)) // null bitmap
		switch c.Type {
		case types.Int64:
			n += int64(len(c.I64)) * 8
		case types.Float64:
			n += int64(len(c.F64)) * 8
		case types.Bool:
			n += int64(len(c.Bool))
		case types.String:
			n += int64(len(c.Str)) * 16 // string header
			for _, s := range c.Str {
				n += int64(len(s))
			}
		}
	}
	return n + 64 // batch overhead
}

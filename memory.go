package executor

import "sync/atomic"

// MemTracker accounts bytes against a configurable limit. A limit <= 0 means
// unlimited. It is safe for concurrent use.
type MemTracker struct {
	limit int64
	used  atomic.Int64
}

// NewMemTracker creates a tracker with the given byte limit (<=0: unlimited).
func NewMemTracker(limit int64) *MemTracker { return &MemTracker{limit: limit} }

// Reserve adds n bytes, failing with ErrMemoryLimit if the limit would be
// exceeded. On failure nothing is reserved.
func (m *MemTracker) Reserve(n int64) error {
	if n <= 0 {
		return nil
	}
	for {
		cur := m.used.Load()
		if m.limit > 0 && cur+n > m.limit {
			return ErrMemoryLimit
		}
		if m.used.CompareAndSwap(cur, cur+n) {
			return nil
		}
	}
}

// Release returns n bytes.
func (m *MemTracker) Release(n int64) {
	if n <= 0 {
		return
	}
	m.used.Add(-n)
}

// Used reports the currently reserved bytes.
func (m *MemTracker) Used() int64 { return m.used.Load() }

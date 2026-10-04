package engine

import (
	"context"
	"sync"
)

// Handle is a cancellation handle for a query. Cancellation is idempotent:
// calling Cancel (or Close) multiple times is safe. All execution functions
// observe the context and stop promptly; goroutines and spill files are
// reclaimed before they return.
type Handle struct {
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
}

// NewHandle creates a handle. If cfg.Timeout > 0 the context is bounded by a
// timeout; otherwise it is a plain cancellable context.
func NewHandle(cfg Config) *Handle {
	var ctx context.Context
	var cancel context.CancelFunc
	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	return &Handle{ctx: ctx, cancel: cancel}
}

// Ctx returns the handle's context.
func (h *Handle) Ctx() context.Context { return h.ctx }

// Cancel requests cancellation. It is safe to call multiple times and
// concurrently.
func (h *Handle) Cancel() {
	h.once.Do(func() { h.cancel() })
}

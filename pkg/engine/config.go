// Package engine wires the row-by-row and vectorized execution paths into
// complete query pipelines: planning, parallel partition scheduling, memory
// accounting, spill lifecycle, cancellation and cross-path validation.
package engine

import (
	"runtime"
	"time"
)

// Config controls execution behavior.
type Config struct {
	// BatchSize is the vectorized batch size (rows per batch).
	BatchSize int
	// MemoryBudget is the soft byte budget for in-memory buffers (sort runs,
	// merge buffers, aggregate groups). 0 disables accounting (unbounded).
	MemoryBudget int64
	// Parallelism is the number of concurrent partition workers. <=0 means
	// runtime.NumCPU.
	Parallelism int
	// SpillDir is the directory for sort spill files. Empty means a temp
	// directory is created and cleaned up automatically.
	SpillDir string
	// Timeout, if >0, applies a context timeout to the whole query.
	Timeout time.Duration
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:    1024,
		MemoryBudget: 64 << 20, // 64 MiB
		Parallelism:  runtime.NumCPU(),
	}
}

func (c *Config) normalize() {
	if c.BatchSize <= 0 {
		c.BatchSize = 1024
	}
	if c.Parallelism <= 0 {
		c.Parallelism = runtime.NumCPU()
	}
}

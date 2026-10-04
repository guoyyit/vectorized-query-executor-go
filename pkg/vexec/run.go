package vexec

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// Run is a stream of sorted batches. A run is either fully in-memory or
// backed by a spilled file.
type Run interface {
	// NextBatch returns the next batch, or (nil, nil) at end of stream.
	NextBatch() (*vector.Batch, error)
	// Close releases resources (including spill files).
	Close() error
	// Rows returns the total number of rows in the run.
	Rows() int64
}

// ---- In-memory run ----

type memRun struct {
	batches []*vector.Batch
	pos     int
	rows    int64
}

func newMemRun(batches []*vector.Batch, rows int64) *memRun {
	return &memRun{batches: batches, rows: rows}
}

func (r *memRun) NextBatch() (*vector.Batch, error) {
	if r.pos >= len(r.batches) {
		return nil, nil
	}
	b := r.batches[r.pos]
	r.pos++
	return b, nil
}

func (r *memRun) Close() error {
	r.batches = nil
	return nil
}

func (r *memRun) Rows() int64 { return r.rows }

// ---- Spill file management ----

var spillCounter uint64

// SpillDir manages temporary spill files.
type SpillDir struct {
	dir     string
	created bool
}

// NewSpillDir creates a spill directory. If path is empty, a temp directory is
// created and removed on Cleanup.
func NewSpillDir(path string) (*SpillDir, error) {
	if path == "" {
		d, err := os.MkdirTemp("", "vqexec-spill-*")
		if err != nil {
			return nil, fmt.Errorf("spill: create temp dir: %w", err)
		}
		return &SpillDir{dir: d, created: true}, nil
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("spill: create dir %s: %w", path, err)
	}
	return &SpillDir{dir: path, created: false}, nil
}

// Dir returns the directory path.
func (s *SpillDir) Dir() string { return s.dir }

// Cleanup removes the directory if it was created by us (temp dir), and
// always removes any .spill files in the directory so that spill files are
// reclaimed even when the query is cancelled or fails.
func (s *SpillDir) Cleanup() error {
	if s.created && s.dir != "" {
		return os.RemoveAll(s.dir)
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var firstErr error
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".spill" {
			if err := os.Remove(filepath.Join(s.dir, e.Name())); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (s *SpillDir) newFile() (*os.File, string, error) {
	n := atomic.AddUint64(&spillCounter, 1)
	path := filepath.Join(s.dir, fmt.Sprintf("run-%d-%d.spill", os.Getpid(), n))
	f, err := os.Create(path)
	if err != nil {
		return nil, "", fmt.Errorf("spill: create %s: %w", path, err)
	}
	return f, path, nil
}

// ---- Spill writer/reader (gob-framed batches) ----

type spillWriter struct {
	f    *os.File
	enc  *gob.Encoder
	path string
	rows int64
}

func newSpillWriter(sd *SpillDir) (*spillWriter, error) {
	f, path, err := sd.newFile()
	if err != nil {
		return nil, err
	}
	return &spillWriter{f: f, enc: gob.NewEncoder(f), path: path}, nil
}

func (w *spillWriter) append(b *vector.Batch) error {
	if err := w.enc.Encode(b); err != nil {
		return fmt.Errorf("spill: write batch: %w", err)
	}
	w.rows += int64(b.N)
	return nil
}

func (w *spillWriter) close() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("spill: close: %w", err)
	}
	return nil
}

type spillReader struct {
	f   *os.File
	dec *gob.Decoder
}

func newSpillReader(path string) (*spillReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("spill: open %s: %w", path, err)
	}
	return &spillReader{f: f, dec: gob.NewDecoder(f)}, nil
}

func (r *spillReader) next() (*vector.Batch, error) {
	var b vector.Batch
	err := r.dec.Decode(&b)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("spill: read batch: %w", err)
	}
	return &b, nil
}

func (r *spillReader) close() error { return r.f.Close() }

// ---- Spilled run ----

type spillRun struct {
	path string
	r    *spillReader
	rows int64
}

func newSpillRun(path string, rows int64) *spillRun {
	return &spillRun{path: path, rows: rows}
}

func (r *spillRun) NextBatch() (*vector.Batch, error) {
	if r.r == nil {
		rd, err := newSpillReader(r.path)
		if err != nil {
			return nil, err
		}
		r.r = rd
	}
	return r.r.next()
}

func (r *spillRun) Close() error {
	if r.r != nil {
		if err := r.r.close(); err != nil {
			return err
		}
		r.r = nil
	}
	return os.Remove(r.path)
}

func (r *spillRun) Rows() int64 { return r.rows }

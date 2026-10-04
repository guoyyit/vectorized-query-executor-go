// Package catalog defines in-memory tables: schemas, partitioned row storage,
// a deterministic generator and a simple CSV loader for local data files.
package catalog

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

// Column describes a table column.
type Column struct {
	Name string
	Type types.T
}

// Schema is an ordered list of columns.
type Schema struct {
	Cols []Column
}

// Types returns the column types.
func (s Schema) Types() []types.T {
	t := make([]types.T, len(s.Cols))
	for i, c := range s.Cols {
		t[i] = c.Type
	}
	return t
}

// Table is a partitioned in-memory table. Each partition is an ordered list of
// rows; partitions are scanned independently and may be processed concurrently.
type Table struct {
	Name   string
	Schema Schema
	Parts  [][][]types.Datum // partition -> row -> column
}

// NumPartitions returns the number of partitions.
func (t *Table) NumPartitions() int { return len(t.Parts) }

// NumRows returns the total number of rows across all partitions.
func (t *Table) NumRows() int {
	n := 0
	for _, p := range t.Parts {
		n += len(p)
	}
	return n
}

// Row returns the global row at index (part, offset).
func (t *Table) Row(part, off int) []types.Datum { return t.Parts[part][off] }

// NewTable builds a table from raw rows, splitting them into numPartitions
// contiguous, roughly equal partitions.
func NewTable(name string, schema Schema, rows [][]types.Datum, numPartitions int) *Table {
	if numPartitions < 1 {
		numPartitions = 1
	}
	if numPartitions > len(rows) {
		numPartitions = len(rows)
	}
	if numPartitions == 0 {
		numPartitions = 1
	}
	parts := make([][][]types.Datum, numPartitions)
	base := len(rows) / numPartitions
	rem := len(rows) % numPartitions
	start := 0
	for p := 0; p < numPartitions; p++ {
		size := base
		if p < rem {
			size++
		}
		parts[p] = rows[start : start+size]
		start += size
	}
	return &Table{Name: name, Schema: schema, Parts: parts}
}

// GeneratorConfig controls the deterministic table generator.
type GeneratorConfig struct {
	NumRows       int
	NumPartitions int
	Seed          int64
	NullFrac      float64 // fraction of values that are NULL (0..1)
	// StringCard limits generated string values to a small alphabet so that
	// grouping/filtering has realistic selectivity. 0 -> random strings.
	StringCard int
	// IntRange, if >0, caps generated int64 values to [0, IntRange) so that
	// sums do not overflow. 0 -> full int64 range (occasional large values).
	IntRange int64
}

// GenerateTable deterministically generates a table from the given schema.
// The same (seed, schema, config) always produces the same rows, which makes
// cross-path comparison reproducible.
func GenerateTable(name string, schema Schema, cfg GeneratorConfig) *Table {
	rng := rand.New(rand.NewSource(cfg.Seed))
	rows := make([][]types.Datum, cfg.NumRows)
	for i := 0; i < cfg.NumRows; i++ {
		row := make([]types.Datum, len(schema.Cols))
		for ci, col := range schema.Cols {
			if rng.Float64() < cfg.NullFrac {
				row[ci] = types.NewNull(col.Type)
				continue
			}
			switch col.Type {
			case types.Int64:
				// Mix small values (good for joins/groups) with occasional
				// large ones (exercises overflow checks), unless IntRange is
				// set to keep sums bounded.
				var v int64
				if cfg.IntRange > 0 {
					v = rng.Int63n(cfg.IntRange)
				} else {
					v = rng.Int63n(1000)
					if rng.Intn(20) == 0 {
						v = rng.Int63()
					}
				}
				row[ci] = types.NewInt64(v)
			case types.Float64:
				row[ci] = types.NewFloat64(rng.Float64() * 1000)
			case types.String:
				if cfg.StringCard > 0 {
					row[ci] = types.NewString(fmt.Sprintf("k%d", rng.Intn(cfg.StringCard)))
				} else {
					row[ci] = types.NewString(fmt.Sprintf("s%d", rng.Int63()))
				}
			case types.Bool:
				row[ci] = types.NewBool(rng.Intn(2) == 0)
			}
		}
		rows[i] = row
	}
	return NewTable(name, schema, rows, cfg.NumPartitions)
}

// LoadCSV loads a table from a local CSV file. Format:
//
//	col1,col2,...      (header with column names)
//	type1,type2,...    (types: int64/float64/string/bool)
//	v1,v2,...          (empty field means NULL)
//
// All rows go into a single partition; use NewTable to repartition.
func LoadCSV(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("catalog: open csv: %w", err)
	}
	defer f.Close()
	return ReadCSV(f)
}

// ReadCSV parses a CSV stream (see LoadCSV for the format).
func ReadCSV(r io.Reader) (*Table, error) {
	cr := csv.NewReader(bufio.NewReader(r))
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("catalog: read csv header: %w", err)
	}
	typeRow, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("catalog: read csv types: %w", err)
	}
	if len(header) != len(typeRow) {
		return nil, fmt.Errorf("catalog: csv header has %d columns but type row has %d", len(header), len(typeRow))
	}
	schema := Schema{Cols: make([]Column, len(header))}
	for i, name := range header {
		t, err := types.ParseType(strings.TrimSpace(typeRow[i]))
		if err != nil {
			return nil, err
		}
		schema.Cols[i] = Column{Name: strings.TrimSpace(name), Type: t}
	}
	var rows [][]types.Datum
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("catalog: read csv row: %w", err)
		}
		if len(rec) != len(header) {
			return nil, fmt.Errorf("catalog: csv row has %d fields, want %d", len(rec), len(header))
		}
		row := make([]types.Datum, len(header))
		for i, field := range rec {
			field = strings.TrimSpace(field)
			if field == "" {
				row[i] = types.NewNull(schema.Cols[i].Type)
				continue
			}
			d, err := parseDatum(schema.Cols[i].Type, field)
			if err != nil {
				return nil, err
			}
			row[i] = d
		}
		rows = append(rows, row)
	}
	return NewTable("csv", schema, rows, 1), nil
}

func parseDatum(t types.T, s string) (types.Datum, error) {
	switch t {
	case types.Int64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return types.Datum{}, fmt.Errorf("%w: invalid int64 %q", types.ErrTypeMismatch, s)
		}
		return types.NewInt64(v), nil
	case types.Float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return types.Datum{}, fmt.Errorf("%w: invalid float64 %q", types.ErrTypeMismatch, s)
		}
		return types.NewFloat64(v), nil
	case types.String:
		return types.NewString(s), nil
	case types.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return types.Datum{}, fmt.Errorf("%w: invalid bool %q", types.ErrTypeMismatch, s)
		}
		return types.NewBool(v), nil
	default:
		return types.Datum{}, fmt.Errorf("%w: unsupported csv type %s", types.ErrTypeMismatch, t)
	}
}

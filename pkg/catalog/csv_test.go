package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

func TestReadCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	content := "id,name,score,active\nint64,string,float64,bool\n1,alice,1.5,true\n2,bob,,false\n3,carol,3.5,\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	tbl, err := LoadCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", tbl.NumRows())
	}
	// Row 1: id=1, name=alice, score=1.5, active=true.
	r := tbl.Row(0, 0)
	if r[0].I != 1 || r[1].S != "alice" || r[2].F != 1.5 || !r[3].B {
		t.Fatalf("unexpected row 0: %v", r)
	}
	// Row 2: score is NULL.
	r = tbl.Row(0, 1)
	if !r[2].Null {
		t.Fatalf("expected NULL score, got %v", r[2])
	}
	// Row 3: active is NULL.
	r = tbl.Row(0, 2)
	if !r[3].Null {
		t.Fatalf("expected NULL active, got %v", r[3])
	}
}

func TestReadCSVEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.csv")
	content := "id,name\nint64,string\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	tbl, err := LoadCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.NumRows() != 0 {
		t.Fatalf("expected 0 rows, got %d", tbl.NumRows())
	}
}

func TestReadCSVTypeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.csv")
	content := "id\nint64\nabc\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadCSV(path)
	if err == nil {
		t.Fatal("expected type mismatch error, got nil")
	}
}

func TestGenerateTableDeterministic(t *testing.T) {
	schema := Schema{Cols: []Column{
		{Name: "id", Type: types.Int64},
		{Name: "v", Type: types.Float64},
	}}
	cfg := GeneratorConfig{NumRows: 100, NumPartitions: 4, Seed: 42, NullFrac: 0.1}
	t1 := GenerateTable("t", schema, cfg)
	t2 := GenerateTable("t", schema, cfg)
	if t1.NumRows() != t2.NumRows() {
		t.Fatal("row count differs")
	}
	for i := range t1.Parts {
		for j := range t1.Parts[i] {
			for k := range t1.Parts[i][j] {
				d1, d2 := t1.Parts[i][j][k], t2.Parts[i][j][k]
				if d1.Null != d2.Null || d1.I != d2.I || d1.F != d2.F || d1.S != d2.S || d1.B != d2.B {
					t.Fatalf("non-deterministic at (%d,%d,%d): %v vs %v", i, j, k, d1, d2)
				}
			}
		}
	}
}

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadJSONLTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.jsonl")
	content := `[1,"a",1.5,true]
[2,null,null,false]
[null,"c",2.5,true]
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	tbl, err := LoadJSONLTable(path, testSchema())
	if err != nil {
		t.Fatal(err)
	}
	if len(tbl.Rows) != 3 {
		t.Fatalf("got %d rows", len(tbl.Rows))
	}
	if !tbl.Rows[1][1].Null || !tbl.Rows[1][2].Null || !tbl.Rows[2][0].Null {
		t.Fatalf("NULL mapping wrong: %v", tbl.Rows)
	}
	// run a plan over the file-backed table on both paths
	plan := &Plan{
		Filter:   IsNull{X: Column{2}, Negated: true},
		GroupBy:  []int{3},
		Aggs:     []AggSpec{{Func: AggCount, Col: -1}, {Func: AggSum, Col: 2}},
		SortKeys: []SortKey{{Col: 0}},
	}
	res, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 1, Partitions: 2, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0][1].I64 != 2 || res[0][2].F64 != 4.0 {
		t.Fatalf("got %v", res)
	}
	// type mismatch in file
	bad := filepath.Join(dir, "bad.jsonl")
	if err := os.WriteFile(bad, []byte(`["notanum","a",1.5,true]`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadJSONLTable(bad, testSchema()); err == nil {
		t.Fatal("expected type mismatch error")
	}
}

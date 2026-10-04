package executor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// LoadJSONLTable loads a table from a local JSON-lines file. Each line is a
// JSON array with one element per column, in schema order. JSON null maps to
// NULL; numbers map to int64 when integral and the column type is TypeInt64,
// otherwise float64; strings and booleans map to their types. A value whose
// JSON kind does not fit the column type fails with ErrTypeMismatch.
func LoadJSONLTable(path string, schema *Schema) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t := &Table{Schema: schema}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		var raw []interface{}
		if err := json.Unmarshal(sc.Bytes(), &raw); err != nil {
			return nil, fmt.Errorf("executor: %s:%d: %w", path, line, err)
		}
		if len(raw) != len(schema.Types) {
			return nil, fmt.Errorf("executor: %s:%d: %w: got %d fields, want %d", path, line, ErrSchema, len(raw), len(schema.Types))
		}
		row := make(Row, len(raw))
		for i, v := range raw {
			val, err := jsonValue(v, schema.Types[i])
			if err != nil {
				return nil, fmt.Errorf("executor: %s:%d col %d: %w", path, line, i, err)
			}
			row[i] = val
		}
		t.Rows = append(t.Rows, row)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return t, nil
}

func jsonValue(v interface{}, typ Type) (Value, error) {
	if v == nil {
		return NullOf(typ), nil
	}
	switch typ {
	case TypeInt64:
		f, ok := v.(float64)
		if !ok || f != float64(int64(f)) {
			return Value{}, ErrTypeMismatch
		}
		return IntVal(int64(f)), nil
	case TypeFloat64:
		f, ok := v.(float64)
		if !ok {
			return Value{}, ErrTypeMismatch
		}
		return FloatVal(f), nil
	case TypeString:
		s, ok := v.(string)
		if !ok {
			return Value{}, ErrTypeMismatch
		}
		return StringVal(s), nil
	case TypeBool:
		b, ok := v.(bool)
		if !ok {
			return Value{}, ErrTypeMismatch
		}
		return BoolVal(b), nil
	}
	return Value{}, ErrTypeMismatch
}

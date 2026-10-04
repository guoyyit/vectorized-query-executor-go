package executor

import (
	"errors"
	"testing"
)

func TestTableValidate(t *testing.T) {
	good := NewTable(testSchema(), []Row{
		{IntVal(1), NullOf(TypeInt64), FloatVal(1), BoolVal(true)}, // NULL of any type ok
	})
	if err := good.Validate(); err != nil {
		t.Fatalf("valid table rejected: %v", err)
	}
	badType := NewTable(testSchema(), []Row{
		{StringVal("oops"), StringVal("a"), FloatVal(1), BoolVal(true)},
	})
	if err := badType.Validate(); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
	badWidth := NewTable(testSchema(), []Row{{IntVal(1)}})
	if err := badWidth.Validate(); !errors.Is(err, ErrSchema) {
		t.Fatalf("expected ErrSchema, got %v", err)
	}
}

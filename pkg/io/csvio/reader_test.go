package csvio

import (
	j "github.com/wdm0006/janitor/pkg/janitor"
	"path/filepath"
	"testing"
)

func TestInferAndRead(t *testing.T) {
	p := filepath.FromSlash("../../../examples/data/iris_nulls.csv")
	r, f, err := Open(p, ReaderOptions{HasHeader: true})
	if err != nil {
		t.Fatal(err)
	}
    defer func() { _ = f.Close() }()
	schema, _, err := r.InferSchema()
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(schema.Columns))
	}
	// last column should be string (species)
	if schema.Columns[4].Type != j.KindString {
		t.Fatalf("expected last column to be string kind, got %d", schema.Columns[4].Type)
	}
	fr, err := r.ReadAll(schema)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Rows() <= 0 {
		t.Fatalf("expected some rows, got %d", fr.Rows())
	}
}

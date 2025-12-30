package jsonlio

import (
	"path/filepath"
	"testing"
)

func TestJSONLInferAndRead(t *testing.T) {
	p := filepath.FromSlash("../../../examples/data/sample.jsonl")
	r, f, err := Open(p, ReaderOptions{SampleRows: 10})
	if err != nil {
		t.Fatal(err)
	}
    defer func() { _ = f.Close() }()
	schema, err := r.InferSchema()
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Columns) == 0 {
		t.Fatal("no columns inferred")
	}
	fr, err := r.ReadAll(schema)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Rows() != 3 {
		t.Fatalf("expected 3 rows, got %d", fr.Rows())
	}
}

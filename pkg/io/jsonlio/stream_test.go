package jsonlio

import (
	"io"
	"path/filepath"
	"testing"
)

func TestStreamReadJSONL(t *testing.T) {
	p := filepath.FromSlash("../../../examples/data/sample.jsonl")
	sr, f, err := NewStreamReader(p, 2)
	if err != nil {
		t.Fatal(err)
	}
    defer func() { _ = f.Close() }()
	total := 0
	for {
		fr, err := sr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total += fr.Rows()
	}
	if total != 3 {
		t.Fatalf("expected 3 rows, got %d", total)
	}
}

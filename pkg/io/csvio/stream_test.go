package csvio

import (
	"io"
	"path/filepath"
	"testing"
)

func TestStreamReadCSV(t *testing.T) {
	p := filepath.FromSlash("../../../examples/data/iris_nulls.csv")
	sr, f, err := NewStreamReader(p, ReaderOptions{HasHeader: true}, 10)
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
	if total <= 0 {
		t.Fatal("expected rows from stream reader")
	}
}

package jsonlio

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

func idLines(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "{\"id\":%d}\n", i)
	}
	return b.String()
}

func readIDs(t *testing.T, content string, sample int) []int64 {
	t.Helper()
	p := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	r, f, err := Open(p, ReaderOptions{SampleRows: sample})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	schema, err := r.InferSchema()
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.ReadAll(schema)
	if err != nil {
		return nil
	}
	col, _ := fr.ColumnByName("id")
	ic := col.(*j.IntColumn)
	ids := make([]int64, fr.Rows())
	for i := range ids {
		ids[i], _ = ic.Get(i)
	}
	return ids
}

func TestReadAllKeepsRowsAfterSample(t *testing.T) {
	// 5000 compact rows exceed the decoder's read-ahead buffer many times over.
	for _, tc := range []struct {
		name         string
		rows, sample int
	}{
		{"explicit sample", 5000, 3},
		{"default sample", 5000, 0},
		{"just past default sample", 101, 0},
		{"within sample", 5, 100},
		{"exactly sample", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids := readIDs(t, idLines(tc.rows), tc.sample)
			if len(ids) != tc.rows {
				t.Fatalf("got %d rows, want %d", len(ids), tc.rows)
			}
			for i, id := range ids {
				if id != int64(i+1) {
					t.Fatalf("row %d id = %d, want %d", i, id, i+1)
				}
			}
		})
	}
}

func TestReadAllErrorsOnMalformedAfterSample(t *testing.T) {
	p := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(p, []byte(idLines(10)+"{\"id\":\n"+idLines(3)), 0o600); err != nil {
		t.Fatal(err)
	}
	r, f, err := Open(p, ReaderOptions{SampleRows: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	schema, err := r.InferSchema()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAll(schema); err == nil {
		t.Fatal("expected error for malformed object after sample")
	}
}

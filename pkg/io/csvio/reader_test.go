package csvio

import (
	j "github.com/wdm0006/janitor/pkg/janitor"
	"path/filepath"
	"strings"
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

// stringCells renders a frame of string columns as rows of cells, using "" for nulls.
func stringCells(t *testing.T, fr *j.Frame, schema j.Schema) [][]string {
	t.Helper()
	out := make([][]string, fr.Rows())
	for row := range out {
		cells := make([]string, len(schema.Columns))
		for i, cs := range schema.Columns {
			col, ok := fr.ColumnByName(cs.Name)
			if !ok {
				t.Fatalf("column %q missing from frame", cs.Name)
			}
			sc, ok := col.(*j.StringColumn)
			if !ok {
				t.Fatalf("column %q is not a string column", cs.Name)
			}
			if v, ok := sc.Get(row); ok {
				cells[i] = v
			}
		}
		out[row] = cells
	}
	return out
}

func assertCells(t *testing.T, got, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d (%v)", len(want), len(got), got)
	}
	for r := range want {
		if len(got[r]) != len(want[r]) {
			t.Fatalf("row %d: expected %d cells, got %d (%v)", r, len(want[r]), len(got[r]), got[r])
		}
		for c := range want[r] {
			if got[r][c] != want[r][c] {
				t.Fatalf("row %d cell %d: expected %q, got %q", r, c, want[r][c], got[r][c])
			}
		}
	}
}

// batchCases exercise field-count mismatches both inside the inference sample
// and after it. SampleRows is 1 so the mismatched record is the only buffered
// one in the "buffered during inference" cases.
var batchCases = []struct {
	name     string
	csv      string
	wantRows [][]string
	wantWarn string
	wantErr  string
	wantAt   string
}{
	{
		name:     "short buffered during inference",
		csv:      "a,b,c\nx,y\np,q,r\n",
		wantRows: [][]string{{"x", "y", ""}, {"p", "q", "r"}},
		wantWarn: "short_records=1",
		wantErr:  "short record",
		wantAt:   "buffered read",
	},
	{
		name:     "short after buffer",
		csv:      "a,b,c\nx,y,z\np,q\n",
		wantRows: [][]string{{"x", "y", "z"}, {"p", "q", ""}},
		wantWarn: "short_records=1",
		wantErr:  "short record",
		wantAt:   "row",
	},
	{
		name:     "long buffered during inference",
		csv:      "a,b,c\nw,x,y,z\np,q,r\n",
		wantRows: [][]string{{"w", "x", "y"}, {"p", "q", "r"}},
		wantWarn: "long_records=1",
		wantErr:  "long record",
		wantAt:   "buffered read",
	},
	{
		name:     "long after buffer",
		csv:      "a,b,c\nx,y,z\np,q,r,s\n",
		wantRows: [][]string{{"x", "y", "z"}, {"p", "q", "r"}},
		wantWarn: "long_records=1",
		wantErr:  "long record",
		wantAt:   "row",
	},
}

func TestBatchNonStrictRepairsAndWarns(t *testing.T) {
	for _, tc := range batchCases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeTempCSV(t, tc.csv)
			r, f, err := Open(p, ReaderOptions{HasHeader: true, SampleRows: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			schema, _, err := r.InferSchema()
			if err != nil {
				t.Fatalf("non-strict inference should not error, got %v", err)
			}
			fr, err := r.ReadAll(schema)
			if err != nil {
				t.Fatalf("non-strict read should not error, got %v", err)
			}
			assertCells(t, stringCells(t, fr, schema), tc.wantRows)
			if got := r.Warnings(); !strings.Contains(got, tc.wantWarn) {
				t.Fatalf("expected warnings to contain %q, got %q", tc.wantWarn, got)
			}
		})
	}
}

func TestBatchStrictRejectsMalformed(t *testing.T) {
	for _, tc := range batchCases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeTempCSV(t, tc.csv)
			r, f, err := Open(p, ReaderOptions{HasHeader: true, SampleRows: 1, Strict: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			schema, _, err := r.InferSchema()
			if err != nil {
				t.Fatal(err)
			}
			_, err = r.ReadAll(schema)
			if err == nil {
				t.Fatal("expected strict error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
			}
			if !strings.Contains(err.Error(), tc.wantAt) {
				t.Fatalf("expected error to mention %q, got %q", tc.wantAt, err.Error())
			}
		})
	}
}

func TestNewReaderFromRepairsMalformed(t *testing.T) {
	r := NewReaderFrom(strings.NewReader("a,b,c\nx,y,z\np,q\nu,v,w,zz\n"), ReaderOptions{HasHeader: true, SampleRows: 1})
	schema, _, err := r.InferSchema()
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.ReadAll(schema)
	if err != nil {
		t.Fatalf("non-strict read should not error, got %v", err)
	}
	assertCells(t, stringCells(t, fr, schema), [][]string{
		{"x", "y", "z"},
		{"p", "q", ""},
		{"u", "v", "w"},
	})
	if got := r.Warnings(); !strings.Contains(got, "short_records=1") || !strings.Contains(got, "long_records=1") {
		t.Fatalf("expected short and long warnings, got %q", got)
	}
}

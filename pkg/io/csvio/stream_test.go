package csvio

import (
	"io"
	"os"
	"path/filepath"
	"strings"
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

// writeTempCSV writes contents to a temp file and returns its path.
func writeTempCSV(t *testing.T, contents string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// drainStream pulls every chunk until EOF or the first non-EOF error.
func drainStream(sr *StreamReader) error {
	for {
		_, err := sr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestStreamStrictRejectsMalformed(t *testing.T) {
	cases := []struct {
		name    string
		csv     string
		sample  int // ReaderOptions.SampleRows
		wantErr string
		wantAt  string
	}{
		{
			// short record read after the sample buffer (SampleRows=1 buffers
			// only the first data row).
			name:    "short after buffer",
			csv:     "a,b,c\n1,2,3\n4,5\n",
			sample:  1,
			wantErr: "short record",
			wantAt:  "row",
		},
		{
			// short record buffered during inference (large sample includes it).
			name:    "short buffered during inference",
			csv:     "a,b,c\n1,2,3\n4,5\n6,7,8\n",
			sample:  100,
			wantErr: "short record",
			wantAt:  "buffered read",
		},
		{
			name:    "long after buffer",
			csv:     "a,b,c\n1,2,3\n4,5,6,7\n",
			sample:  1,
			wantErr: "long record",
			wantAt:  "row",
		},
		{
			name:    "long buffered during inference",
			csv:     "a,b,c\n1,2,3\n4,5,6,7\n8,9,10\n",
			sample:  100,
			wantErr: "long record",
			wantAt:  "buffered read",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeTempCSV(t, tc.csv)
			sr, f, err := NewStreamReader(p, ReaderOptions{HasHeader: true, SampleRows: tc.sample, Strict: true}, 100)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			err = drainStream(sr)
			if err == nil {
				t.Fatalf("expected strict error, got nil")
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

func TestStreamNonStrictRepairsAndWarns(t *testing.T) {
	cases := []struct {
		name     string
		csv      string
		sample   int
		wantWarn string
	}{
		{
			name:     "short after buffer",
			csv:      "a,b,c\n1,2,3\n4,5\n",
			sample:   1,
			wantWarn: "short_records=1",
		},
		{
			name:     "short buffered during inference",
			csv:      "a,b,c\n1,2,3\n4,5\n6,7,8\n",
			sample:   100,
			wantWarn: "short_records=1",
		},
		{
			name:     "long after buffer",
			csv:      "a,b,c\n1,2,3\n4,5,6,7\n",
			sample:   1,
			wantWarn: "long_records=1",
		},
		{
			name:     "long buffered during inference",
			csv:      "a,b,c\n1,2,3\n4,5,6,7\n8,9,10\n",
			sample:   100,
			wantWarn: "long_records=1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeTempCSV(t, tc.csv)
			sr, f, err := NewStreamReader(p, ReaderOptions{HasHeader: true, SampleRows: tc.sample}, 100)
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
					t.Fatalf("non-strict stream should not error, got %v", err)
				}
				total += fr.Rows()
			}
			if total == 0 {
				t.Fatal("expected repaired rows in non-strict mode")
			}
			if got := sr.Warnings(); !strings.Contains(got, tc.wantWarn) {
				t.Fatalf("expected warnings to contain %q, got %q", tc.wantWarn, got)
			}
		})
	}
}

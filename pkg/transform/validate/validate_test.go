package validate

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"testing"
)

func ptr(v float64) *float64 { return &v }

// floatFrame builds a single-column float frame; a nil entry stays null.
func floatFrame(name string, vals []*float64) *j.Frame {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: name, Type: j.KindFloat, Nullable: true}}}
	f := j.NewFrame(s)
	for range vals {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName(name)
	c := col.(*j.FloatColumn)
	for i, v := range vals {
		if v != nil {
			c.Set(i, *v)
		}
	}
	return f
}

// intFrame builds a single-column int frame; a nil entry stays null.
func intFrame(name string, vals []*int64) *j.Frame {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: name, Type: j.KindInt, Nullable: true}}}
	f := j.NewFrame(s)
	for range vals {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName(name)
	c := col.(*j.IntColumn)
	for i, v := range vals {
		if v != nil {
			c.Set(i, *v)
		}
	}
	return f
}

// stringFrame builds a single-column string frame; a nil entry stays null.
func stringFrame(name string, vals []*string) *j.Frame {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: name, Type: j.KindString, Nullable: true}}}
	f := j.NewFrame(s)
	for range vals {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName(name)
	c := col.(*j.StringColumn)
	for i, v := range vals {
		if v != nil {
			c.Set(i, *v)
		}
	}
	return f
}

func iptr(v int64) *int64   { return &v }
func sptr(v string) *string { return &v }

func TestRange(t *testing.T) {
	cases := []struct {
		name  string
		frame *j.Frame
		tform *Range
		want  string // empty means no error expected
	}{
		{
			// Boundary values must pass: min/max are inclusive.
			name:  "float in range at bounds",
			frame: floatFrame("x", []*float64{ptr(1), ptr(5), ptr(10)}),
			tform: &Range{Column: "x", Min: ptr(1), Max: ptr(10)},
		},
		{
			name:  "float out of range both ends",
			frame: floatFrame("x", []*float64{ptr(0.5), ptr(5), ptr(10.5)}),
			tform: &Range{Column: "x", Min: ptr(1), Max: ptr(10)},
			want:  "validate_range: column x has 2 out-of-range values",
		},
		{
			name:  "float min only",
			frame: floatFrame("x", []*float64{ptr(-1), ptr(0.5), ptr(2), ptr(1000)}),
			tform: &Range{Column: "x", Min: ptr(1)},
			want:  "validate_range: column x has 2 out-of-range values",
		},
		{
			name:  "float max only",
			frame: floatFrame("x", []*float64{ptr(-1000), ptr(2), ptr(11)}),
			tform: &Range{Column: "x", Max: ptr(10)},
			want:  "validate_range: column x has 1 out-of-range values",
		},
		{
			name:  "int in range at bounds",
			frame: intFrame("n", []*int64{iptr(1), iptr(5), iptr(10)}),
			tform: &Range{Column: "n", Min: ptr(1), Max: ptr(10)},
		},
		{
			name:  "int out of range both ends",
			frame: intFrame("n", []*int64{iptr(0), iptr(5), iptr(11), iptr(12)}),
			tform: &Range{Column: "n", Min: ptr(1), Max: ptr(10)},
			want:  "validate_range: column n has 3 out-of-range values",
		},
		{
			// The bounds exclude 0, so an unskipped null (whose backing value
			// is 0) would be counted as out of range.
			name:  "float nulls skipped",
			frame: floatFrame("x", []*float64{ptr(5), nil, ptr(7)}),
			tform: &Range{Column: "x", Min: ptr(1), Max: ptr(10)},
		},
		{
			name:  "int nulls skipped",
			frame: intFrame("n", []*int64{iptr(5), nil, iptr(7)}),
			tform: &Range{Column: "n", Min: ptr(1), Max: ptr(10)},
		},
		{
			name:  "missing column no-ops",
			frame: floatFrame("x", []*float64{ptr(-100)}),
			tform: &Range{Column: "other", Min: ptr(1), Max: ptr(10)},
		},
		{
			name:  "non-numeric column no-ops",
			frame: stringFrame("s", []*string{sptr("nope")}),
			tform: &Range{Column: "s", Min: ptr(1), Max: ptr(10)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := tc.tform.Apply(context.Background(), tc.frame)
			if out != tc.frame {
				t.Fatalf("expected the same frame back")
			}
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("expected no error, got %v", err)
			case tc.want != "" && err == nil:
				t.Fatalf("expected error %q, got nil", tc.want)
			case tc.want != "" && err.Error() != tc.want:
				t.Fatalf("expected error %q, got %q", tc.want, err.Error())
			}
		})
	}
}

func TestInSet(t *testing.T) {
	allowed := []string{"red", "green", "blue"}

	cases := []struct {
		name  string
		frame *j.Frame
		tform *InSet
		want  string
	}{
		{
			name:  "all values in set",
			frame: stringFrame("color", []*string{sptr("red"), sptr("blue"), sptr("red")}),
			tform: NewInSet("color", allowed),
		},
		{
			name:  "values outside set",
			frame: stringFrame("color", []*string{sptr("red"), sptr("mauve"), sptr("teal"), sptr("blue")}),
			tform: NewInSet("color", allowed),
			want:  "validate_in: column color has 2 values outside allowed set",
		},
		{
			// "" is not in the allowed set, so an unskipped null (whose backing
			// value is "") would be counted as outside it.
			name:  "nulls skipped",
			frame: stringFrame("color", []*string{sptr("red"), nil, sptr("green")}),
			tform: NewInSet("color", allowed),
		},
		{
			// Documents the silent no-op: a non-string column never fails
			// validate_in, whatever it holds.
			name:  "non-string column no-ops",
			frame: intFrame("color", []*int64{iptr(42)}),
			tform: NewInSet("color", allowed),
		},
		{
			name:  "missing column no-ops",
			frame: stringFrame("color", []*string{sptr("mauve")}),
			tform: NewInSet("other", allowed),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := tc.tform.Apply(context.Background(), tc.frame)
			if out != tc.frame {
				t.Fatalf("expected the same frame back")
			}
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("expected no error, got %v", err)
			case tc.want != "" && err == nil:
				t.Fatalf("expected error %q, got nil", tc.want)
			case tc.want != "" && err.Error() != tc.want:
				t.Fatalf("expected error %q, got %q", tc.want, err.Error())
			}
		})
	}
}

func TestNewInSet(t *testing.T) {
	s := NewInSet("color", []string{"red", "red", "blue"})
	if s.Column != "color" {
		t.Fatalf("expected column color, got %q", s.Column)
	}
	if len(s.Values) != 2 {
		t.Fatalf("expected 2 distinct values, got %d", len(s.Values))
	}
	for _, v := range []string{"red", "blue"} {
		if _, ok := s.Values[v]; !ok {
			t.Fatalf("expected %q in the allowed set", v)
		}
	}
	if s.Name() != "validate_in" {
		t.Fatalf("unexpected name %q", s.Name())
	}
}

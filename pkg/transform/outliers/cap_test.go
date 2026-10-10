package outliers

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"reflect"
	"testing"
)

func ptr(v float64) *float64 { return &v }
func iptr(v int64) *int64    { return &v }

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

func stringFrame(name string, vals []string) *j.Frame {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: name, Type: j.KindString, Nullable: true}}}
	f := j.NewFrame(s)
	for range vals {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName(name)
	c := col.(*j.StringColumn)
	for i, v := range vals {
		c.Set(i, v)
	}
	return f
}

func TestCapFloat(t *testing.T) {
	// The null row (index 1) holds a zero value; every bound excludes 0 so a
	// missing IsNull guard would visibly move it.
	cases := []struct {
		name     string
		in       []*float64
		min, max *float64
		want     []*float64
	}{
		{"min only", []*float64{ptr(1), nil, ptr(5)}, ptr(2.5), nil, []*float64{ptr(2.5), nil, ptr(5)}},
		{"max only", []*float64{ptr(1), nil, ptr(-9)}, nil, ptr(-2.5), []*float64{ptr(-2.5), nil, ptr(-9)}},
		{"both", []*float64{ptr(1), nil, ptr(9), ptr(4)}, ptr(2.5), ptr(7.5), []*float64{ptr(2.5), nil, ptr(7.5), ptr(4)}},
		{"negative bounds", []*float64{ptr(-10), nil, ptr(-1), ptr(-4)}, ptr(-6.5), ptr(-2.5), []*float64{ptr(-6.5), nil, ptr(-2.5), ptr(-4)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := floatFrame("x", tc.in)
			if _, err := (&Cap{Column: "x", Min: tc.min, Max: tc.max}).Apply(context.Background(), f); err != nil {
				t.Fatal(err)
			}
			col, _ := f.ColumnByName("x")
			c := col.(*j.FloatColumn)
			for i, w := range tc.want {
				if w == nil {
					if !c.IsNull(i) {
						t.Errorf("row %d: want null, got non-null", i)
					}
					if v, _ := c.Get(i); v != 0 {
						t.Errorf("row %d: null cell touched, data=%v", i, v)
					}
					continue
				}
				if v, _ := c.Get(i); c.IsNull(i) || v != *w {
					t.Errorf("row %d: got %v (null=%v), want %v", i, v, c.IsNull(i), *w)
				}
			}
		})
	}
}

func TestCapInt(t *testing.T) {
	cases := []struct {
		name     string
		in       []*int64
		min, max *float64
		want     []*int64
	}{
		{"integral min", []*int64{iptr(1), nil, iptr(9)}, ptr(3), nil, []*int64{iptr(3), nil, iptr(9)}},
		{"integral max", []*int64{iptr(1), nil, iptr(9)}, nil, ptr(7), []*int64{iptr(1), nil, iptr(7)}},
		{"fractional min rounds up", []*int64{iptr(1), nil, iptr(2), iptr(3)}, ptr(2.5), nil, []*int64{iptr(3), nil, iptr(3), iptr(3)}},
		{"fractional max rounds down", []*int64{iptr(1), nil, iptr(3), iptr(9)}, nil, ptr(7.5), []*int64{iptr(1), nil, iptr(3), iptr(7)}},
		{"negative fractional min", []*int64{iptr(-9), nil, iptr(-3)}, ptr(-5.5), nil, []*int64{iptr(-5), nil, iptr(-3)}},
		{"negative fractional max", []*int64{iptr(1), nil, iptr(-9)}, nil, ptr(-2.5), []*int64{iptr(-3), nil, iptr(-9)}},
		{"both fractional", []*int64{iptr(0), nil, iptr(20), iptr(5)}, ptr(2.5), ptr(7.5), []*int64{iptr(3), nil, iptr(7), iptr(5)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := intFrame("x", tc.in)
			if _, err := (&Cap{Column: "x", Min: tc.min, Max: tc.max}).Apply(context.Background(), f); err != nil {
				t.Fatal(err)
			}
			col, _ := f.ColumnByName("x")
			c := col.(*j.IntColumn)
			for i, w := range tc.want {
				if w == nil {
					if !c.IsNull(i) {
						t.Errorf("row %d: want null, got non-null", i)
					}
					if v, _ := c.Get(i); v != 0 {
						t.Errorf("row %d: null cell touched, data=%v", i, v)
					}
					continue
				}
				if v, _ := c.Get(i); c.IsNull(i) || v != *w {
					t.Errorf("row %d: got %v (null=%v), want %v", i, v, c.IsNull(i), *w)
				}
			}
		})
	}
}

func TestCapNoOp(t *testing.T) {
	ctx := context.Background()
	s := stringFrame("s", []string{"a", "b"})
	if _, err := (&Cap{Column: "s", Min: ptr(1), Max: ptr(2)}).Apply(ctx, s); err != nil {
		t.Fatal(err)
	}
	col, _ := s.ColumnByName("s")
	sc := col.(*j.StringColumn)
	got := []string{}
	for i := 0; i < sc.Len(); i++ {
		v, _ := sc.Get(i)
		got = append(got, v)
	}
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("string column changed: %v", got)
	}

	f := floatFrame("x", []*float64{ptr(100)})
	if _, err := (&Cap{Column: "missing", Min: ptr(1), Max: ptr(2)}).Apply(ctx, f); err != nil {
		t.Fatal(err)
	}
	col, _ = f.ColumnByName("x")
	if v, _ := col.(*j.FloatColumn).Get(0); v != 100 {
		t.Errorf("missing-column cap changed data: %v", v)
	}
}

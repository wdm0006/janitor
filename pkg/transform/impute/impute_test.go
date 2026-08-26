package impute

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"testing"
)

func makeFloatFrame() *j.Frame {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: "x", Type: j.KindFloat, Nullable: true}}}
	f := j.NewFrame(s)
	for i := 0; i < 5; i++ {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName("x")
	c := col.(*j.FloatColumn)
	c.Set(0, 1.0)
	c.Set(2, 3.0)
	// rows 1,3,4 remain null
	return f
}

func TestConstant(t *testing.T) {
	f := makeFloatFrame()
	tform := &Constant{Column: "x", Value: 2.5}
	out, err := tform.Apply(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := out.ColumnByName("x")
	c := col.(*j.FloatColumn)
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			t.Fatalf("constant imputer left null at row %d", i)
		}
	}
}

func TestMean(t *testing.T) {
	f := makeFloatFrame()
	tform := &Mean{Column: "x"}
	out, err := tform.Apply(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := out.ColumnByName("x")
	c := col.(*j.FloatColumn)
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			t.Fatalf("mean imputer left null at row %d", i)
		}
	}
}

func TestMeanIntNegativeRounding(t *testing.T) {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: "x", Type: j.KindInt, Nullable: true}}}
	f := j.NewFrame(s)
	for i := 0; i < 4; i++ {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName("x")
	c := col.(*j.IntColumn)
	c.Set(0, -3)
	c.Set(1, -2)
	c.Set(2, -2)
	// row 3 remains null; non-null mean = -7/3 = -2.333..., nearest int is -2

	tform := &Mean{Column: "x"}
	out, err := tform.Apply(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	col, _ = out.ColumnByName("x")
	c = col.(*j.IntColumn)
	if c.IsNull(3) {
		t.Fatalf("mean imputer left null at row 3")
	}
	got, _ := c.Get(3)
	if got != -2 {
		t.Fatalf("expected imputed value -2, got %d", got)
	}
}

func TestMedian(t *testing.T) {
	f := makeFloatFrame()
	tform := &Median{Column: "x"}
	out, err := tform.Apply(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := out.ColumnByName("x")
	c := col.(*j.FloatColumn)
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			t.Fatalf("median imputer left null at row %d", i)
		}
	}
}

func TestMode(t *testing.T) {
	t.Run("string column", func(t *testing.T) {
		s := j.Schema{Columns: []j.ColumnSchema{{Name: "s", Type: j.KindString, Nullable: true}}}
		f := j.NewFrame(s)
		for i := 0; i < 6; i++ {
			f.AppendNullRow()
		}
		col, _ := f.ColumnByName("s")
		c := col.(*j.StringColumn)
		c.Set(0, "a")
		c.Set(1, "a")
		c.Set(2, "a")
		c.Set(3, "b")
		// rows 4,5 remain null; "a" wins 3 to 1

		out, err := (&Mode{Column: "s"}).Apply(context.Background(), f)
		if err != nil {
			t.Fatal(err)
		}
		col, _ = out.ColumnByName("s")
		c = col.(*j.StringColumn)
		want := []string{"a", "a", "a", "b", "a", "a"}
		for i, w := range want {
			if c.IsNull(i) {
				t.Fatalf("mode imputer left null at row %d", i)
			}
			got, _ := c.Get(i)
			if got != w {
				t.Fatalf("row %d: expected %q, got %q", i, w, got)
			}
		}
	})

	t.Run("int column", func(t *testing.T) {
		s := j.Schema{Columns: []j.ColumnSchema{{Name: "n", Type: j.KindInt, Nullable: true}}}
		f := j.NewFrame(s)
		for i := 0; i < 6; i++ {
			f.AppendNullRow()
		}
		col, _ := f.ColumnByName("n")
		c := col.(*j.IntColumn)
		c.Set(0, 7)
		c.Set(1, 7)
		c.Set(2, 7)
		c.Set(3, 9)
		// rows 4,5 remain null; 7 wins 3 to 1

		out, err := (&Mode{Column: "n"}).Apply(context.Background(), f)
		if err != nil {
			t.Fatal(err)
		}
		col, _ = out.ColumnByName("n")
		c = col.(*j.IntColumn)
		want := []int64{7, 7, 7, 9, 7, 7}
		for i, w := range want {
			if c.IsNull(i) {
				t.Fatalf("mode imputer left null at row %d", i)
			}
			got, _ := c.Get(i)
			if got != w {
				t.Fatalf("row %d: expected %d, got %d", i, w, got)
			}
		}
	})

	// Documents current behavior: with nothing to compute a mode from, the
	// accumulator stays at its zero value and every null is filled with it.
	t.Run("all-null string column fills with empty string", func(t *testing.T) {
		s := j.Schema{Columns: []j.ColumnSchema{{Name: "s", Type: j.KindString, Nullable: true}}}
		f := j.NewFrame(s)
		for i := 0; i < 3; i++ {
			f.AppendNullRow()
		}
		out, err := (&Mode{Column: "s"}).Apply(context.Background(), f)
		if err != nil {
			t.Fatal(err)
		}
		col, _ := out.ColumnByName("s")
		c := col.(*j.StringColumn)
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				t.Fatalf("expected row %d to be filled, got null", i)
			}
			got, _ := c.Get(i)
			if got != "" {
				t.Fatalf("row %d: expected %q, got %q", i, "", got)
			}
		}
	})

	t.Run("all-null int column fills with zero", func(t *testing.T) {
		s := j.Schema{Columns: []j.ColumnSchema{{Name: "n", Type: j.KindInt, Nullable: true}}}
		f := j.NewFrame(s)
		for i := 0; i < 3; i++ {
			f.AppendNullRow()
		}
		out, err := (&Mode{Column: "n"}).Apply(context.Background(), f)
		if err != nil {
			t.Fatal(err)
		}
		col, _ := out.ColumnByName("n")
		c := col.(*j.IntColumn)
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				t.Fatalf("expected row %d to be filled, got null", i)
			}
			got, _ := c.Get(i)
			if got != 0 {
				t.Fatalf("row %d: expected 0, got %d", i, got)
			}
		}
	})

	t.Run("missing column no-ops", func(t *testing.T) {
		f := makeFloatFrame()
		out, err := (&Mode{Column: "nope"}).Apply(context.Background(), f)
		if err != nil {
			t.Fatal(err)
		}
		col, _ := out.ColumnByName("x")
		c := col.(*j.FloatColumn)
		if !c.IsNull(1) {
			t.Fatalf("expected row 1 to still be null")
		}
	})
}

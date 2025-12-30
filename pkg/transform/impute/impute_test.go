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

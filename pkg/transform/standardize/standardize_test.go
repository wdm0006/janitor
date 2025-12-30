package standardize

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"testing"
)

func TestTrimAndLower(t *testing.T) {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: "s", Type: j.KindString, Nullable: true}}}
	f := j.NewFrame(s)
	for i := 0; i < 3; i++ {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName("s")
	c := col.(*j.StringColumn)
	c.Set(0, "  Foo  ")
	c.Set(1, "BAR")
	// row 2 null

	tf1 := &Trim{Column: "s"}
	if _, err := tf1.Apply(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	v, _ := c.Get(0)
	if v != "Foo" {
		t.Fatalf("trim failed, got %q", v)
	}

	tf2 := &Lower{Column: "s"}
	if _, err := tf2.Apply(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	v0, _ := c.Get(0)
	v1, _ := c.Get(1)
	if v0 != "foo" || v1 != "bar" {
		t.Fatalf("lower failed, got %q %q", v0, v1)
	}

	tf3 := &RegexReplace{Column: "s", Pattern: "o+", Replace: "O"}
	if _, err := tf3.Apply(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	v0, _ = c.Get(0)
	if v0 != "fO" {
		t.Fatalf("regex replace failed, got %q", v0)
	}

	tf4 := &MapValues{Column: "s", Map: map[string]string{"bar": "baz"}}
	if _, err := tf4.Apply(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	v1, _ = c.Get(1)
	if v1 != "baz" {
		t.Fatalf("map values failed, got %q", v1)
	}
}

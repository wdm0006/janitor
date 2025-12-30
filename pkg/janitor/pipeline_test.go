package janitor_test

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	imp "github.com/wdm0006/janitor/pkg/transform/impute"
	std "github.com/wdm0006/janitor/pkg/transform/standardize"
	"testing"
)

func TestPipeline(t *testing.T) {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: "x", Type: j.KindFloat, Nullable: true}, {Name: "s", Type: j.KindString, Nullable: true}}}
	f := j.NewFrame(s)
	for i := 0; i < 2; i++ {
		f.AppendNullRow()
	}
	_ = f.SetCell(0, "x", 1.0)
	_ = f.SetCell(0, "s", " Foo ")
	// row 1 left nulls

	p := j.NewPipeline().Add(&imp.Mean{Column: "x"}).Add(&std.Trim{Column: "s"})
	out, err := p.Run(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	colX, _ := out.ColumnByName("x")
	fx := colX.(*j.FloatColumn)
	if fx.IsNull(1) {
		t.Fatal("imputer failed to fill null")
	}
	colS, _ := out.ColumnByName("s")
	ss := colS.(*j.StringColumn)
	s0, _ := ss.Get(0)
	if s0 != "Foo" {
		t.Fatalf("trim failed, got %q", s0)
	}
}

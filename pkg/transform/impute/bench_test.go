package impute

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"testing"
)

func makeLargeFloatFrame(n int) *j.Frame {
	s := j.Schema{Columns: []j.ColumnSchema{{Name: "x", Type: j.KindFloat, Nullable: true}}}
	f := j.NewFrame(s)
	for i := 0; i < n; i++ {
		f.AppendNullRow()
	}
	col, _ := f.ColumnByName("x")
	c := col.(*j.FloatColumn)
	for i := 0; i < n; i += 2 {
		c.Set(i, float64(i%10))
	}
	return f
}

func BenchmarkImputeMean(b *testing.B) {
	base := makeLargeFloatFrame(10000)
	for n := 0; n < b.N; n++ {
		// copy shallowly; operate in place for benchmark simplicity
		f := base
		tform := &Mean{Column: "x"}
		if _, err := tform.Apply(context.Background(), f); err != nil {
			b.Fatal(err)
		}
	}
}

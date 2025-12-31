package janitor

import (
    "context"
    "testing"
)

func makeFrame(rows int) *Frame {
    s := Schema{Columns: []ColumnSchema{{Name: "a", Type: KindFloat, Nullable: true}, {Name: "b", Type: KindInt, Nullable: true}, {Name: "s", Type: KindString, Nullable: true}}}
    f := NewFrame(s)
    for i := 0; i < rows; i++ {
        f.AppendNullRow()
        _ = f.SetCell(i, "a", float64(i%100))
        _ = f.SetCell(i, "b", int64(i%10))
        _ = f.SetCell(i, "s", "x")
    }
    return f
}

type noopTransform struct{}
func (n *noopTransform) Name() string { return "noop" }
func (n *noopTransform) Apply(ctx context.Context, f *Frame) (*Frame, error) { return f, nil }

func BenchmarkPipeline(b *testing.B) {
    f := makeFrame(100000)
    p := NewPipeline().Add(&noopTransform{}).Add(&noopTransform{})
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = p.Run(context.Background(), f)
    }
}


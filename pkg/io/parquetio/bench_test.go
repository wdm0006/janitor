package parquetio

import (
    j "github.com/wdm0006/janitor/pkg/janitor"
    "os"
    "testing"
)

func makeFrame(rows int) *j.Frame {
    s := j.Schema{Columns: []j.ColumnSchema{{Name: "a", Type: j.KindFloat, Nullable: true}, {Name: "b", Type: j.KindInt, Nullable: true}}}
    f := j.NewFrame(s)
    for i := 0; i < rows; i++ {
        f.AppendNullRow()
        _ = f.SetCell(i, "a", float64(i%100))
        _ = f.SetCell(i, "b", int64(i%10))
    }
    return f
}

func BenchmarkParquetWrite(b *testing.B) {
    f := makeFrame(50000)
    path := "bench.parquet"
    b.Cleanup(func() { _ = os.Remove(path) })
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = WriteAll(path, f)
    }
}


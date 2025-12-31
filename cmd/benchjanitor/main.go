package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "math/rand"
    "os"
    "runtime"
    "time"

    j "github.com/wdm0006/janitor/pkg/janitor"
    imp "github.com/wdm0006/janitor/pkg/transform/impute"
    std "github.com/wdm0006/janitor/pkg/transform/standardize"
)

type genSource struct {
    schema j.Schema
    remain int
    chunk  int
    missp  float64
    rnd    *rand.Rand
}

func (g *genSource) Next() (*j.Frame, error) {
    if g.remain <= 0 {
        return nil, ioEOF{}
    }
    n := g.chunk
    if n > g.remain { n = g.remain }
    g.remain -= n
    f := j.NewFrame(g.schema)
    frows := n
    for i := 0; i < frows; i++ {
        f.AppendNullRow()
        for _, cs := range g.schema.Columns {
            switch cs.Type {
            case j.KindFloat:
                if g.rnd.Float64() < g.missp { continue }
                _ = f.SetCell(i, cs.Name, g.rnd.Float64()*100)
            case j.KindInt:
                if g.rnd.Float64() < g.missp { continue }
                _ = f.SetCell(i, cs.Name, int64(g.rnd.Intn(100)))
            case j.KindBool:
                if g.rnd.Float64() < g.missp { continue }
                _ = f.SetCell(i, cs.Name, g.rnd.Intn(2) == 0)
            case j.KindString:
                if g.rnd.Float64() < g.missp { continue }
                _ = f.SetCell(i, cs.Name, "Alpha ")
            }
        }
    }
    return f, nil
}

type ioEOF struct{}
func (ioEOF) Error() string { return "EOF" }

type blackholeSink struct{ rows int }
func (b *blackholeSink) Write(f *j.Frame) error { b.rows += f.Rows(); return nil }
func (b *blackholeSink) Close() error { return nil }

func main() {
    var (
        rows       = flag.Int("rows", 5_000_000, "total rows to generate")
        chunk      = flag.Int("chunk", 100_000, "rows per chunk")
        fcols      = flag.Int("float-cols", 4, "number of float columns")
        icols      = flag.Int("int-cols", 2, "number of int columns")
        scols      = flag.Int("string-cols", 2, "number of string columns")
        missp      = flag.Float64("missing", 0.05, "probability of missing values in each cell")
        jsonOut    = flag.Bool("json", false, "emit JSON summary")
        seed       = flag.Int64("seed", 42, "random seed")
    )
    flag.Parse()

    // Build schema
    var cols []j.ColumnSchema
    for i := 0; i < *fcols; i++ { cols = append(cols, j.ColumnSchema{Name: fmt.Sprintf("f%d", i), Type: j.KindFloat, Nullable: true}) }
    for i := 0; i < *icols; i++ { cols = append(cols, j.ColumnSchema{Name: fmt.Sprintf("i%d", i), Type: j.KindInt, Nullable: true}) }
    for i := 0; i < *scols; i++ { cols = append(cols, j.ColumnSchema{Name: fmt.Sprintf("s%d", i), Type: j.KindString, Nullable: true}) }
    schema := j.Schema{Columns: cols}

    // Pipeline: simple imputations and trims
    p := j.NewPipeline().
        Add(&imp.Mean{Column: "f0"}).
        Add(&imp.Median{Column: "i0"}).
        Add(&std.Trim{Column: "s0"}).
        Add(&std.Lower{Column: "s0"})

    src := &genSource{schema: schema, remain: *rows, chunk: *chunk, missp: *missp, rnd: rand.New(rand.NewSource(*seed))}
    sink := &blackholeSink{}

    // Warm up
    runtime.GC()
    time.Sleep(100 * time.Millisecond)

    var msBefore, msAfter runtime.MemStats
    runtime.ReadMemStats(&msBefore)
    start := time.Now()
    err := j.RunStream(context.Background(), p, src, sink)
    if err != nil && err.Error() != "EOF" { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    elapsed := time.Since(start)
    runtime.ReadMemStats(&msAfter)

    // Summary
    rowsPerSec := float64(*rows) / elapsed.Seconds()
    summary := map[string]any{
        "rows": *rows,
        "elapsed_ms": elapsed.Milliseconds(),
        "rows_per_sec": rowsPerSec,
        "mem_alloc_bytes": msAfter.Alloc,
        "mem_total_alloc_bytes": msAfter.TotalAlloc - msBefore.TotalAlloc,
        "gc_num": msAfter.NumGC - msBefore.NumGC,
        "cols": map[string]int{"float": *fcols, "int": *icols, "string": *scols},
        "chunk": *chunk,
        "missing_prob": *missp,
    }

    if *jsonOut {
        b, _ := json.MarshalIndent(summary, "", "  ")
        fmt.Println(string(b))
        return
    }
    fmt.Printf("Rows: %d\n", *rows)
    fmt.Printf("Elapsed: %s\n", elapsed)
    fmt.Printf("Throughput: %.0f rows/s\n", rowsPerSec)
    fmt.Printf("Current Alloc: %d MB\n", msAfter.Alloc/1024/1024)
    fmt.Printf("Total Alloc (delta): %d MB\n", (msAfter.TotalAlloc-msBefore.TotalAlloc)/1024/1024)
    fmt.Printf("GC cycles (delta): %d\n", msAfter.NumGC-msBefore.NumGC)
}


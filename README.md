Janitor
=======

High‑performance data cleaning for Go — as a library and a CLI.

Janitor provides a streaming‑friendly pipeline API, strong typing with a columnar `Frame`, robust CSV/JSONL IO, and adapters for popular ML libraries like golearn.

What It Is
----------
- Library and CLI for cleaning messy tabular data at speed.
- Columnar core with typed, nullable columns and vectorized ops.
- Streaming execution to process large files in bounded memory.
- Batteries included transforms: imputers, text normalization, validation, and outlier handling.
- Adapters to/from golearn `DenseInstances`.

Quick Start
-----------
Demo
- ![Janitor CLI demo](docs/assets/demo.gif)

CLI
- Install (once available): `go install github.com/wdm0006/janitor/cmd/janitor@latest`
- Run (CSV): `janitor --config examples/config/rules.json`
- Run (JSONL): `janitor --config examples/config/rules_jsonl.json`
- Stream large files: `janitor --config <file> --chunk-size 10000`

Minimal JSON config
```json
{
  "input": {"type": "csv", "path": "data.csv", "has_header": true},
  "output": {"type": "csv", "path": "clean.csv"},
  "steps": [
    {"impute_mean": {"column": "age"}},
    {"trim": {"column": "name"}},
    {"lower": {"column": "email"}},
    {"validate_range": {"column": "age", "min": 0}}
  ]
}
```

Library (Go)
```go
import (
  "context"
  csvio "github.com/wdm0006/janitor/pkg/io/csvio"
  j "github.com/wdm0006/janitor/pkg/janitor"
  imp "github.com/wdm0006/janitor/pkg/transform/impute"
  std "github.com/wdm0006/janitor/pkg/transform/standardize"
)

r, f, _ := csvio.Open("data.csv", csvio.ReaderOptions{HasHeader: true})
defer f.Close()
schema, _, _ := r.InferSchema()
frame, _ := r.ReadAll(schema)

p := j.NewPipeline().
  Add(&imp.Mean{Column: "age"}).
  Add(&std.Trim{Column: "name"})

out, _ := p.Run(context.Background(), frame)
_ = csvio.WriteAll("clean.csv", out, csvio.WriterOptions{})
```

Features
--------
- IO: CSV (headers, custom delimiter), JSONL; Parquet planned (build tag).
- Transforms: impute (constant/mean/median/mode), trim/lower, regex replace, value maps, range checks, in‑set validation, capping.
- Streaming: chunked readers/writers for CSV and JSONL; backpressure‑friendly pipeline entrypoint.
- Columnar core: typed, nullable columns; minimal allocations; vector‑style loops.
- Compatibility: adapters to/from golearn `DenseInstances`.

Performance
-----------
- Designed for throughput:
  - Streaming avoids loading entire files; fixed memory footprint per chunk.
  - CSV uses `Reader.ReuseRecord` and fast parsers; JSONL uses buffered decoders.
  - Column‑wise transforms reduce per‑cell overhead and GC pressure.
- Benchmarks included; run with:
  - `go test -bench . ./pkg/io/csvio`
  - `go test -bench . ./pkg/transform/impute`
- Tune `--chunk-size` to match your workload and machine.

Integrations
------------
- Adapters: `adapters/golearn` converts to/from golearn `DenseInstances`.

Roadmap
-------
- See ROADMAP.md for milestones and upcoming features.

Contributing
------------
- See CONTRIBUTING.md for development environment, style, testing, and release info.

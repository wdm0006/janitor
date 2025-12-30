Janitor Cookbook
================

Practical, copy‑pasteable snippets for common cleaning tasks.

Basics
------
- Load CSV and write cleaned CSV:
  - Config (JSON):
```
{
  "input": {"type": "csv", "path": "data.csv", "has_header": true},
  "output": {"type": "csv", "path": "clean.csv"},
  "steps": [
    {"impute_mean": {"column": "age"}},
    {"trim": {"column": "name"}},
    {"lower": {"column": "email"}}
  ]
}
```
- Stream large CSVs: add `--chunk-size 10000` to the CLI.

Missing Data
------------
- Constant imputation:
```
{"impute_constant": {"column": "price", "value": 0}}
```
- Mean/median/mode:
```
{"impute_mean": {"column": "age"}}
{"impute_median": {"column": "income"}}
{"impute_mode": {"column": "category"}}
```

Text Cleanup
------------
- Trim whitespace and lowercase:
```
{"trim": {"column": "name"}},
{"lower": {"column": "email"}}
```
- Regex replace (collapse multiple spaces):
```
{"regex_replace": {"column": "name", "pattern": "\\s+", "replace": " "}}
```
- Map values (normalize labels):
```
{"map_values": {"column": "status", "map": {"OK": "ok", "Ok": "ok", "okay": "ok"}}}
```

Validation
----------
- Allowed set:
```
{"validate_in": {"column": "species", "values": ["setosa", "versicolor", "virginica"]}}
```
- Range check:
```
{"validate_range": {"column": "age", "min": 0, "max": 120}}
```

Outliers
--------
- Cap values to range:
```
{"cap_range": {"column": "price", "min": 0, "max": 10000}}
```

Formats
-------
- CSV → JSONL:
```
{
  "input": {"type": "csv", "path": "in.csv", "has_header": true},
  "output": {"type": "jsonl", "path": "out.jsonl"},
  "steps": []
}
```
- JSONL → CSV (with basic normalization):
```
{
  "input": {"type": "jsonl", "path": "in.jsonl"},
  "output": {"type": "csv", "path": "out.csv"},
  "steps": [{"trim": {"column": "name"}}]
}
```

Programmatic Pipelines (Go)
---------------------------
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

p := j.NewPipeline().Add(&imp.Median{Column: "price"}).Add(&std.Trim{Column: "name"})
out, _ := p.Run(context.Background(), frame)
_ = csvio.WriteAll("clean.csv", out, csvio.WriterOptions{})
```

Streaming Pipelines (Go)
------------------------
```go
sr, f, _ := csvio.NewStreamReader("big.csv", csvio.ReaderOptions{HasHeader: true}, 10000)
defer f.Close()
sw, _ := csvio.NewStreamWriter("clean.csv", sr.Schema(), csvio.WriterOptions{})
defer sw.Close()
p := j.NewPipeline().Add(&imp.Mean{Column: "x"})
_ = j.RunStream(context.Background(), p, sr, sw)
```


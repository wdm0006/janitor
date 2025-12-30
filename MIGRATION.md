Migration Guide
===============

The project evolved from a small golearn-oriented helper to a general-purpose data cleaning library and CLI.

Old packages
------------
- `dataio.ParseDirtyCSVToInstances(path, hasHeaders, nSamples)`
- `imputation.ConstantImputer`

Status
------
- These are still present for backward compatibility but are marked Deprecated.
- New code should use the columnar `Frame` API, IO connectors, and `Pipeline` transforms.

New equivalents
---------------
- Reading CSV into a Frame:
  - `r, f, _ := csvio.Open(path, csvio.ReaderOptions{HasHeader: true})`
  - `schema, _, _ := r.InferSchema()`
  - `frame, _ := r.ReadAll(schema)`
- Building a cleaning pipeline:
  - `p := janitor.NewPipeline().Add(&impute.Mean{Column: "x"}).Add(&standardize.Trim{Column: "name"})`
  - `out, _ := p.Run(ctx, frame)`
- Adapting to golearn (if needed):
  - `inst, _ := adapters_golearn.ToDenseInstances(out)`

Streaming
---------
- For large files, prefer streaming:
  - CSV: `sr, f, _ := csvio.NewStreamReader(path, opts, 10000)`; `sw, _ := csvio.NewStreamWriter(outPath, schema, opts)`
  - `janitor.RunStream(ctx, p, sr, sw)`
- JSONL has analogous stream readers/writers.

CLI
---
- Use `cmd/janitor` to run config-driven pipelines for CSV/JSONL (see `examples/config`).

Notes
-----
- Parquet support is behind a build tag and requires selecting a library.
- The old APIs will be removed in a future major version after a deprecation period.


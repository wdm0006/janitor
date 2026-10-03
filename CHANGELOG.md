# Changelog

## v0.1.0 (Unreleased)

- Add Go modules and CI (GitHub Actions), linting, Makefile.
- Introduce columnar Frame, Schema, Pipeline core.
- CSV IO: inference, batch reader/writer, streaming reader/writer.
- JSONL IO: inference, batch reader/writer, streaming reader/writer.
- Transforms: impute (constant/mean/median/mode), standardize (trim/lower/regex_replace/map_values), validate (in-set/range), outliers (cap).
- CLI: config-driven pipelines, CSV/JSONL input/output, streaming via `--chunk-size`.
- Golearn adapters to/from DenseInstances.
- Migration guide and deprecations for legacy APIs.
- Parquet stubs behind build tag.
- CLI: wire `impute_mode` into the step dispatcher; `impute_mode` leaves an all-null column null.
- CLI: an unknown step key is now a configuration error (exit 2, names the key and step index), including under `--dry-run`, instead of a warning.


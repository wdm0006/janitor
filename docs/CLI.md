Janitor CLI Reference
======================

Install
-------
- Go 1.22+: `go install github.com/wdm0006/janitor/cmd/janitor@latest`

Synopsis
--------
- Single command: `janitor --config <file> [flags]`
- Reads a JSON/YAML/TOML config describing input/output and cleaning steps, then runs in batch or streaming mode.

Global Flags
------------
- `--config <path>`: Path to JSON/YAML/TOML config (required)
- `--chunk-size <N>`: Enable streaming with chunks of N rows (default: batch)
- `--verbose`: Print progress, summaries, and repair notices
- `--expected-rows <N>`: Hint for streaming to show ETA and a progress bar
- `--dry-run`: Infer schema, print planned steps, and exit (no reads/writes)
- `--profile`: Print column stats and exit (streamed for CSV/JSONL; batch for Parquet)
- `--profile-topk <N>`: Number of top values to show for strings/time (default 5)
- `--profile-json`: Print profile as JSON
- `--version`: Print version and exit

Config Schema
-------------
- Top‑level keys: `input`, `output`, `steps` (array)

Input
- `type`: `csv` (default) | `jsonl` | `parquet`
- `path`: file path, `-` (stdin), or a glob for CSV/JSONL (e.g., `data/*.csv`)
- `has_header` (CSV): boolean (default false)
- `delimiter` (CSV): comma by default; leave empty to enable sniffing
- `csv_strict` (CSV): boolean; true = error on short/long records; false = repair and continue

Output
- `type`: `csv` (default) | `jsonl` | `parquet`
- `path`: file path or `-` (stdout)
- `delimiter` (CSV): output delimiter (default comma)
- `partition_by`: array of column names to partition outputs (streaming only)

Placeholders
- `{basename}`: replaced with the input filename stem when using globs
- `{col:ColumnName}`: replaced with partition values when `partition_by` is set

Steps
- Examples: (all steps operate on a named column)
  - `impute_constant` `{ column, value }`
  - `impute_mean` `{ column }`
  - `impute_median` `{ column }`
  - `impute_mode` `{ column }`
  - `trim` `{ column }`
  - `lower` `{ column }`
  - `regex_replace` `{ column, pattern, replace }`
  - `map_values` `{ column, map }`
  - `validate_in` `{ column, values }`
  - `validate_range` `{ column, min?, max? }`
  - `cap_range` `{ column, min?, max? }`

Modes
-----
- Batch (default): reads input fully, applies pipeline, writes output
- Streaming (`--chunk-size`): reads/cleans/writes in fixed‑size chunks
  - CSV/JSONL: supports globs and partitioned outputs
  - Parquet: streaming input supported; partitioned outputs supported for CSV/JSONL

Progress & ETA
--------------
- `--verbose`: shows processed rows and rows/sec
- `--expected-rows N`: enables simple progress bar and ETA (rows/sec is a short rolling average)

CSV Repair vs Strict
--------------------
- Default: repairs short/long records; in verbose batch mode, prints a summary
- Strict: set `input.csv_strict` to error out on short/long records

Compression & Pipes
-------------------
- Use `-` as input/output path for stdin/stdout
- `.gz` on the path enables gzip output; gzip is auto‑detected on read

Examples
--------
- Batch CSV cleaning
```
janitor --config examples/config/rules.json
```
- Streaming with progress and ETA
```
janitor --config examples/config/rules.json --chunk-size 10000 --expected-rows 1000000 --verbose
```
- Dry‑run and profile
```
janitor --config cfg.json --dry-run
janitor --config cfg.json --profile --profile-json
```
- Multi‑file inputs with `{basename}` outputs
```
{
  "input": {"type": "csv", "path": "data/*.csv", "has_header": true},
  "output": {"type": "csv", "path": "out/{basename}.clean.csv"},
  "steps": []
}
```
- Partitioned outputs with `{col:...}`
```
{
  "input": {"type": "jsonl", "path": "data/*.jsonl"},
  "output": {"type": "jsonl", "path": "out/{basename}/{col:country}.jsonl", "partition_by": ["country"]},
  "steps": []
}
```

Exit Codes
----------
- 0: success
- 1: runtime errors (IO, parsing, pipeline)
- 2: usage/config errors (unsupported types, missing placeholders for multi‑file)


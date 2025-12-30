Contributing to Janitor
=======================

Thanks for your interest in contributing! This guide keeps contributions fast, predictable, and consistent.

Project Scope
-------------
- High‑performance data cleaning for Go: library + CLI.
- Columnar `Frame`, streaming IO (CSV/JSONL), pipeline of transforms.
- Practical transforms and validators; adapters for golearn.

Developer Setup
---------------
- Go 1.22+
- make (Xcode CLT on macOS)
- golangci-lint (optional locally; CI runs it)

Install tools (macOS/Homebrew)
```
brew install go golangci-lint
```

Common Tasks
------------
- Format: `make fmt`
- Vet: `make vet`
- Lint: `make lint`
- Test (race): `make test`
- Build CLI: `go build ./cmd/janitor`

Benchmarks
----------
- CSV IO: `go test -bench . ./pkg/io/csvio`
- Imputers: `go test -bench . ./pkg/transform/impute`

Style & Guidelines
------------------
- Prefer streaming/chunked flows for large data; avoid unbounded memory.
- Check errors; prefer `defer func(){ _ = f.Close() }()` for file closes.
- Keep transforms column‑wise and allocation‑aware.
- Avoid unnecessary abstractions; keep APIs small and predictable.
- Tests for new features; fuzz/property tests where parsers/validators are involved.

Directory Overview
------------------
- `cmd/janitor` — CLI entrypoint.
- `pkg/janitor` — core types (`Frame`, `Schema`, `Pipeline`).
- `pkg/io/{csvio,jsonlio}` — IO connectors (batch + streaming).
- `pkg/transform` — transforms (`impute`, `standardize`, `validate`, `outliers`).
- `adapters/golearn` — compatibility layer for golearn.
- `examples/` — small runnable examples and configs.

Releases
--------
- Tag `vX.Y.Z` to trigger builds (see `.github/workflows/release.yml`).
- Update `CHANGELOG.md` with notable changes.

Questions
---------
- Open an issue for discussion or proposals. PRs welcome!


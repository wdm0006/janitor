package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    _ "net/http/pprof"
    "os"
    "path/filepath"
    "runtime/pprof"
    "strings"
    "sync/atomic"
    "time"
    "unicode/utf8"

    csvio "github.com/wdm0006/janitor/pkg/io/csvio"
	jsonlio "github.com/wdm0006/janitor/pkg/io/jsonlio"
	parquetio "github.com/wdm0006/janitor/pkg/io/parquetio"
    j "github.com/wdm0006/janitor/pkg/janitor"
    profpkg "github.com/wdm0006/janitor/pkg/profile"
    imp "github.com/wdm0006/janitor/pkg/transform/impute"
    outl "github.com/wdm0006/janitor/pkg/transform/outliers"
    std "github.com/wdm0006/janitor/pkg/transform/standardize"
    val "github.com/wdm0006/janitor/pkg/transform/validate"
    toml "github.com/pelletier/go-toml/v2"
    yaml "gopkg.in/yaml.v3"
)

var (
	version = "0.1.0-dev"
)

type Config struct {
    Input struct {
        Path      string `json:"path"`
        Type      string `json:"type"` // csv|jsonl (default csv)
        HasHeader bool   `json:"has_header"`
        Delimiter string `json:"delimiter"`
        CSVStrict bool   `json:"csv_strict"`
    } `json:"input"`
    Output struct {
        Path      string `json:"path"`
        Type      string `json:"type"` // csv|jsonl (default csv)
        Delimiter string `json:"delimiter"`
        PartitionBy []string `json:"partition_by"`
    } `json:"output"`
    Steps []json.RawMessage `json:"steps"`
}

func main() {
    showVersion := flag.Bool("version", false, "Print version and exit")
    configPath := flag.String("config", "", "Path to cleaning config (JSON/YAML/TOML)")
    chunkSize := flag.Int("chunk-size", 0, "Enable streaming with chunk size (rows per chunk). 0 disables streaming.")
    verbose := flag.Bool("verbose", false, "Print progress and a summary")
    prof := flag.Bool("profile", false, "Profile the input: print column stats and exit")
    profTopK := flag.Int("profile-topk", 5, "Top-K frequent values to show for string/time columns")
    profJSON := flag.Bool("profile-json", false, "Emit profile in JSON format")
    expectedRows := flag.Int("expected-rows", 0, "Optional expected total rows for ETA in streaming progress")
    cpuProfile := flag.String("cpu-profile", "", "Write CPU profile to file (pprof)")
    memProfile := flag.String("mem-profile", "", "Write heap profile to file on exit (pprof)")
    pprofAddr := flag.String("pprof-addr", "", "Serve net/http/pprof on this address (e.g., :6060)")
    metricsAddr := flag.String("metrics-addr", "", "Serve expvar metrics and /healthz on this address (e.g., :9090)")
    logJSON := flag.Bool("log-json", false, "Emit progress logs as JSON lines")
    dryRun := flag.Bool("dry-run", false, "Infer schema and print planned steps, without reading/writing data")
    flag.Parse()

	if *showVersion {
		fmt.Println("janitor", version)
		return
	}

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "no config provided; nothing to do. try --config <file> or --version")
		os.Exit(2)
	}

    b, err := os.ReadFile(*configPath)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    var cfg Config
    if err := parseConfig(*configPath, b, &cfg); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    inputDelimiter, err := parseDelimiter(cfg.Input.Delimiter, rune(0))
    if err != nil {
        fmt.Fprintf(os.Stderr, "invalid input delimiter: %v\n", err)
        os.Exit(1)
    }
    outputDelimiter, err := parseDelimiter(cfg.Output.Delimiter, ',')
    if err != nil {
        fmt.Fprintf(os.Stderr, "invalid output delimiter: %v\n", err)
        os.Exit(1)
    }

    p, stepNames, err := buildPipeline(cfg.Steps)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(2)
    }

    // Observability: pprof + expvar servers
    if *pprofAddr != "" {
        go func() {
            log.Printf("pprof listening on %s", *pprofAddr)
            _ = http.ListenAndServe(*pprofAddr, nil)
        }()
    }
    if *metricsAddr != "" {
        go func() {
            http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
            log.Printf("metrics listening on %s", *metricsAddr)
            _ = http.ListenAndServe(*metricsAddr, nil)
        }()
    }
    if *cpuProfile != "" {
        f, err := os.Create(*cpuProfile)
        if err != nil { log.Fatalf("cpu profile: %v", err) }
        _ = pprof.StartCPUProfile(f)
        defer func() { pprof.StopCPUProfile(); _ = f.Close() }()
    }
    if *memProfile != "" {
        defer func() {
            f, err := os.Create(*memProfile)
            if err != nil { log.Printf("mem profile: %v", err); return }
            defer func() { _ = f.Close() }()
            _ = pprof.WriteHeapProfile(f)
        }()
    }

    var frame *j.Frame
	useStream := *chunkSize > 0
	if !useStream {
		switch cfg.Input.Type {
		case "", "csv":
            rdr, file, err := csvio.Open(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: inputDelimiter, SampleRows: 100, Strict: cfg.Input.CSVStrict})
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            if file != nil { defer func() { _ = file.Close() }() }
            schema, _, err := rdr.InferSchema()
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            frame, err = rdr.ReadAll(schema)
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            if *verbose {
                fmt.Fprintf(os.Stderr, "read csv: rows=%d cols=%d from %s\n", frame.Rows(), len(schema.Columns), cfg.Input.Path)
                if w := rdr.Warnings(); w != "" { fmt.Fprintf(os.Stderr, "csv repair summary: %s\n", w) }
            }
		case "jsonl":
            jr, jf, err := jsonlio.Open(cfg.Input.Path, jsonlio.ReaderOptions{SampleRows: 100})
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            if jf != nil { defer func() { _ = jf.Close() }() }
			schema, err := jr.InferSchema()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
            frame, err = jr.ReadAll(schema)
            if err != nil {
                fmt.Fprintln(os.Stderr, err)
                os.Exit(1)
            }
            if *verbose {
                fmt.Fprintf(os.Stderr, "read jsonl: rows=%d cols=%d from %s\n", frame.Rows(), len(schema.Columns), cfg.Input.Path)
            }
		case "parquet":
			fmt.Fprintln(os.Stderr, "parquet input not yet supported; please use CSV/JSONL input.")
			os.Exit(2)
		default:
			fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
			os.Exit(2)
		}
	}

    // Dry-run: print inferred schema and steps, then exit
    if *dryRun {
        switch cfg.Input.Type {
        case "", "csv":
            rdr, f, err := csvio.Open(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: inputDelimiter, SampleRows: 50})
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            if f != nil { defer func() { _ = f.Close() }() }
            schema, _, err := rdr.InferSchema()
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            fmt.Fprintf(os.Stderr, "dry-run schema (csv): %v\nsteps: %v\n", schema, stepNames)
        case "jsonl":
            jr, jf, err := jsonlio.Open(cfg.Input.Path, jsonlio.ReaderOptions{SampleRows: 50})
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            if jf != nil { defer func() { _ = jf.Close() }() }
            schema, err := jr.InferSchema()
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            fmt.Fprintf(os.Stderr, "dry-run schema (jsonl): %v\nsteps: %v\n", schema, stepNames)
        case "parquet":
            pr, err := parquetio.OpenReader(cfg.Input.Path, 50)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
        defer func() { _ = pr.Close() }()
            schema := pr.Schema()
            fmt.Fprintf(os.Stderr, "dry-run schema (parquet): %v\nsteps: %v\n", schema, stepNames)
        default:
            fmt.Fprintf(os.Stderr, "unsupported input type %q for dry-run\n", cfg.Input.Type)
            os.Exit(2)
        }
        return
    }

    // Profile-only path
    if *prof {
        if *chunkSize <= 0 { *chunkSize = 10000 }
        switch cfg.Input.Type {
        case "", "csv":
            sr, f, err := csvio.NewStreamReader(cfg.Input.Path, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: inputDelimiter, SampleRows: 200}, *chunkSize)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { if f != nil { _ = f.Close() } }()
            // create collector
            col := profpkg.NewCollector(sr.Schema(), *profTopK)
            // iterate
            for {
                fr, err := sr.Next()
                if err == io.EOF { break }
                if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                col.ConsumeFrame(fr)
            }
            if *profJSON {
                out := col.ReportJSON()
                b, _ := json.MarshalIndent(out, "", "  ")
                fmt.Println(string(b))
            } else {
                fmt.Println(col.ReportText())
            }
            return
        case "jsonl":
            sr, f, err := jsonlio.NewStreamReader(cfg.Input.Path, *chunkSize)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { _ = f.Close() }()
            col := profpkg.NewCollector(sr.Schema(), *profTopK)
            for {
                fr, err := sr.Next()
                if err == io.EOF { break }
                if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                col.ConsumeFrame(fr)
            }
            if *profJSON {
                out := col.ReportJSON()
                b, _ := json.MarshalIndent(out, "", "  ")
                fmt.Println(string(b))
            } else {
                fmt.Println(col.ReportText())
            }
            return
        case "parquet":
            pr, err := parquetio.OpenReader(cfg.Input.Path, 200)
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            defer func() { _ = pr.Close() }()
            fr, err := pr.ReadAll()
            if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            col := profpkg.NewCollector(fr.Schema(), *profTopK)
            col.ConsumeFrame(fr)
            if *profJSON {
                out := col.ReportJSON()
                b, _ := json.MarshalIndent(out, "", "  ")
                fmt.Println(string(b))
            } else {
                fmt.Println(col.ReportText())
            }
            return
        default:
            fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
            os.Exit(2)
        }
    }

    if useStream {
        // streaming path
        switch cfg.Input.Type {
        case "", "csv":
            // expand globs
            paths := []string{cfg.Input.Path}
            if hasWildcards(cfg.Input.Path) {
                matches, _ := filepath.Glob(cfg.Input.Path)
                if len(matches) == 0 { fmt.Fprintln(os.Stderr, "no files matched input path pattern"); os.Exit(2) }
                paths = matches
            }
            if len(paths) > 1 && !strings.Contains(cfg.Output.Path, "{basename}") {
                fmt.Fprintln(os.Stderr, "multiple input files require output.path to include {basename} placeholder")
                os.Exit(2)
            }
            for _, in := range paths {
                sr, f, err := csvio.NewStreamReader(in, csvio.ReaderOptions{HasHeader: cfg.Input.HasHeader, Delimiter: inputDelimiter, SampleRows: 100, Strict: cfg.Input.CSVStrict}, *chunkSize)
                if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                if f != nil { defer func() { _ = f.Close() }() }
                switch cfg.Output.Type {
                case "", "csv":
                    outPath := cfg.Output.Path
                    if strings.Contains(outPath, "{basename}") {
                        base := filepath.Base(in)
                        outPath = strings.ReplaceAll(outPath, "{basename}", strings.TrimSuffix(base, filepath.Ext(base)))
                    }
                    if len(cfg.Output.PartitionBy) > 0 {
                        makeSink := func(path string, schema j.Schema) (j.ChunkSink, error) {
                            return csvio.NewStreamWriter(path, schema, csvio.WriterOptions{Delimiter: outputDelimiter})
                        }
                        if err := runStreamPartitioned(context.Background(), p, sr, outPath, makeSink, sr.Schema(), cfg.Output.PartitionBy, *verbose, *expectedRows, *logJSON); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    } else {
                        sw, err := csvio.NewStreamWriter(outPath, sr.Schema(), csvio.WriterOptions{Delimiter: outputDelimiter})
                        if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows, *logJSON); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    }
                case "jsonl":
                    outPath := cfg.Output.Path
                    if strings.Contains(outPath, "{basename}") {
                        base := filepath.Base(in)
                        outPath = strings.ReplaceAll(outPath, "{basename}", strings.TrimSuffix(base, filepath.Ext(base)))
                    }
                    if len(cfg.Output.PartitionBy) > 0 {
                        makeSink := func(path string, schema j.Schema) (j.ChunkSink, error) { return jsonlio.NewStreamWriter(path) }
                        if err := runStreamPartitioned(context.Background(), p, sr, outPath, makeSink, sr.Schema(), cfg.Output.PartitionBy, *verbose, *expectedRows, *logJSON); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    } else {
                        sw, err := jsonlio.NewStreamWriter(outPath)
                        if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                        if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows, *logJSON); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                    }
                default:
                    fmt.Fprintf(os.Stderr, "unsupported output type %q for streaming\n", cfg.Output.Type)
                    os.Exit(2)
                }
            }
        case "jsonl":
			sr, f, err := jsonlio.NewStreamReader(cfg.Input.Path, *chunkSize)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
            defer func() { _ = f.Close() }()
                switch cfg.Output.Type {
                case "jsonl":
				sw, err := jsonlio.NewStreamWriter(cfg.Output.Path)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
                    if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows, *logJSON); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
                case "", "csv":
                sw, err := csvio.NewStreamWriter(cfg.Output.Path, sr.Schema(), csvio.WriterOptions{Delimiter: outputDelimiter})
                if err != nil {
                    fmt.Fprintln(os.Stderr, err)
                    os.Exit(1)
                }
                    if err := runStreamWithProgress(context.Background(), p, sr, sw, *verbose, *expectedRows, *logJSON); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
            default:
                fmt.Fprintf(os.Stderr, "unsupported output type %q for streaming\n", cfg.Output.Type)
                os.Exit(2)
            }
        default:
            fmt.Fprintf(os.Stderr, "unsupported input type %q\n", cfg.Input.Type)
            os.Exit(2)
        }
        return
    }

	// batch path
	outFrame, err := p.Run(context.Background(), frame)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	switch cfg.Output.Type {
	case "", "csv":
        if err := csvio.WriteAll(cfg.Output.Path, outFrame, csvio.WriterOptions{Delimiter: outputDelimiter}); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    case "jsonl":
        if err := jsonlio.WriteAll(cfg.Output.Path, outFrame); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    case "parquet":
        if err := parquetio.WriteAll(cfg.Output.Path, outFrame); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    default:
        fmt.Fprintf(os.Stderr, "unsupported output type %q\n", cfg.Output.Type)
        os.Exit(2)
    }
    if *verbose {
        fmt.Fprintf(os.Stderr, "batch complete: rows=%d cols=%d steps=%v -> %s\n", outFrame.Rows(), len(outFrame.Schema().Columns), stepNames, cfg.Output.Path)
    }
}

func parseDelimiter(value string, defaultDelimiter rune) (rune, error) {
    if value == "" {
        return defaultDelimiter, nil
    }
    if !utf8.ValidString(value) {
        return 0, fmt.Errorf("delimiter must be valid UTF-8")
    }
    delimiter, size := utf8.DecodeRuneInString(value)
    if size != len(value) {
        return 0, fmt.Errorf("delimiter must contain exactly one Unicode rune, got %q", value)
    }
    return delimiter, nil
}

// parseConfig detects format from extension (.json, .yaml/.yml, .toml) and unmarshals into cfg.
func parseConfig(path string, b []byte, cfg *Config) error {
    ext := strings.ToLower(filepath.Ext(path))
    var err error
    switch ext {
    case ".json", "":
        err = json.Unmarshal(b, cfg)
    case ".yaml", ".yml":
        err = normalizeConfig(b, yaml.Unmarshal, cfg)
    case ".toml":
        err = normalizeConfig(b, toml.Unmarshal, cfg)
    default:
        err = json.Unmarshal(b, cfg)
    }
    if err != nil {
        return fmt.Errorf("parse config: %w", err)
    }
    for i, raw := range cfg.Steps {
        var step map[string]json.RawMessage
        if err := json.Unmarshal(raw, &step); err != nil {
            return fmt.Errorf("parse config: step %d must be an object with exactly one key: %w", i+1, err)
        }
        if len(step) != 1 {
            return fmt.Errorf("parse config: step %d must contain exactly one key, got %d", i+1, len(step))
        }
        for _, params := range step {
            var object map[string]json.RawMessage
            if err := json.Unmarshal(params, &object); err != nil {
                return fmt.Errorf("parse config: step %d parameters must be an object: %w", i+1, err)
            }
            if object == nil {
                return fmt.Errorf("parse config: step %d parameters must be an object", i+1)
            }
        }
    }
    return nil
}

func normalizeConfig(b []byte, unmarshal func([]byte, any) error, cfg *Config) error {
    var value any
    if err := unmarshal(b, &value); err != nil {
        return err
    }
    normalized, err := json.Marshal(value)
    if err != nil {
        return err
    }
    return json.Unmarshal(normalized, cfg)
}

// runStreamWithProgress processes chunks and prints periodic progress when verbose.
// progressReporter formats one progress line per tick from a single row-count snapshot.
type progressReporter struct {
    w        io.Writer
    start    time.Time
    expected int
    logJSON  bool
    rates    []float64
}

func (pr *progressReporter) report(rows int) {
    elapsed := time.Since(pr.start).Seconds()
    instRate := float64(rows) / (elapsed + 1e-9)
    pr.rates = append(pr.rates, instRate)
    if len(pr.rates) > 5 { pr.rates = pr.rates[len(pr.rates)-5:] }
    var sum float64
    for _, r := range pr.rates { sum += r }
    rate := sum / float64(len(pr.rates))
    expected := pr.expected
    if expected > 0 {
        remaining := expected - rows
        if remaining < 0 { remaining = 0 }
        eta := time.Duration(float64(remaining)/(rate+1e-9)) * time.Second
        pct := float64(rows) / float64(expected)
        if pct > 1 { pct = 1 }
        width := 30
        filled := int(pct * float64(width))
        if pr.logJSON {
            evt := map[string]any{"type": "progress", "rows": rows, "expected": expected, "rate_rps": rate, "eta": int(eta.Truncate(time.Second).Seconds())}
            b, _ := json.Marshal(evt)
            fmt.Fprintln(pr.w, string(b))
        } else {
            bar := strings.Repeat("=", filled) + strings.Repeat(" ", width-filled)
            fmt.Fprintf(pr.w, "[%s] %5.1f%% rows=%d/%d (%.1f r/s) ETA=%s\n", bar, pct*100, rows, expected, rate, eta.Truncate(time.Second))
        }
    } else {
        if pr.logJSON {
            evt := map[string]any{"type": "progress", "rows": rows, "rate_rps": rate}
            b, _ := json.Marshal(evt)
            fmt.Fprintln(pr.w, string(b))
        } else {
            fmt.Fprintf(pr.w, "processed rows=%d (%.1f rows/s) ...\n", rows, rate)
        }
    }
}

// runStreamWithProgress processes chunks and prints periodic progress when verbose.
func runStreamWithProgress(ctx context.Context, p *j.Pipeline, src j.ChunkSource, sink j.ChunkSink, verbose bool, expected int, logJSON bool) error {
    if !verbose { return j.RunStream(ctx, p, src, sink) }
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    return streamWithProgress(ctx, p, src, sink, expected, logJSON, ticker.C, os.Stderr)
}

func streamWithProgress(ctx context.Context, p *j.Pipeline, src j.ChunkSource, sink j.ChunkSink, expected int, logJSON bool, tick <-chan time.Time, w io.Writer) error {
    done := make(chan error, 1)
    var rows atomic.Int64
    pr := &progressReporter{w: w, start: time.Now(), expected: expected, logJSON: logJSON}
    go func() {
        for {
            f, err := src.Next()
            if err == io.EOF { done <- nil; return }
            if err != nil { done <- err; return }
            rows.Add(int64(f.Rows()))
            out, err := p.Run(ctx, f)
            if err != nil { done <- err; return }
            if err := sink.Write(out); err != nil { done <- err; return }
        }
    }()
    for {
        select {
        case err := <-done:
            return err
        case <-tick:
            pr.report(int(rows.Load()))
        }
    }
}

// runStreamPartitioned applies p to each chunk from src, splits rows by partition columns,
// and writes each partition to a sink keyed by the expanded outPath template. outPath must
// include placeholders like {col:Name} which will be replaced with the row's column value.
func runStreamPartitioned(ctx context.Context, p *j.Pipeline, src j.ChunkSource, outPath string, makeSink func(path string, schema j.Schema) (j.ChunkSink, error), schema j.Schema, partCols []string, verbose bool, expected int, logJSON bool) error {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    return streamPartitioned(ctx, p, src, outPath, makeSink, schema, partCols, verbose, expected, logJSON, ticker.C, os.Stderr)
}

func streamPartitioned(ctx context.Context, p *j.Pipeline, src j.ChunkSource, outPath string, makeSink func(path string, schema j.Schema) (j.ChunkSink, error), schema j.Schema, partCols []string, verbose bool, expected int, logJSON bool, tick <-chan time.Time, w io.Writer) error {
    sinks := map[string]j.ChunkSink{}
    closeAll := func() {
        for _, s := range sinks { _ = s.Close() }
    }
    defer closeAll()
    done := make(chan error, 1)
    var rows atomic.Int64
    pr := &progressReporter{w: w, start: time.Now(), expected: expected, logJSON: logJSON}
    go func() {
        for {
            f, err := src.Next()
            if err == io.EOF { done <- nil; return }
            if err != nil { done <- err; return }
            out, err := p.Run(ctx, f)
            if err != nil { done <- err; return }
            // split by partition keys
            parts := splitFrameByPartitions(out, partCols)
            for key, pf := range parts {
                path := expandOutPath(outPath, schema, partCols, pf, key)
                s, ok := sinks[path]
                if !ok {
                    ss, err := makeSink(path, schema)
                    if err != nil { done <- err; return }
                    sinks[path] = ss
                    s = ss
                }
                if err := s.Write(pf); err != nil { done <- err; return }
                rows.Add(int64(pf.Rows()))
            }
        }
    }()
    if !verbose {
        return <-done
    }
    for {
        select {
        case err := <-done:
            return err
        case <-tick:
            pr.report(int(rows.Load()))
        }
    }
}

func splitFrameByPartitions(f *j.Frame, cols []string) map[string]*j.Frame {
    res := map[string]*j.Frame{}
    for r := 0; r < f.Rows(); r++ {
        key := partitionKeyForRow(f, r, cols)
        pf, ok := res[key]
        if !ok {
            pf = j.NewFrame(f.Schema())
            res[key] = pf
        }
        pf.AppendNullRow()
        row := pf.Rows() - 1
        // copy all columns
        for _, cs := range f.Schema().Columns {
            col, _ := f.ColumnByName(cs.Name)
            switch cs.Type {
            case j.KindFloat:
                if v, ok := col.(*j.FloatColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindInt:
                if v, ok := col.(*j.IntColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindBool:
                if v, ok := col.(*j.BoolColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindString:
                if v, ok := col.(*j.StringColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            case j.KindTime:
                if v, ok := col.(*j.TimeColumn).Get(r); ok { _ = pf.SetCell(row, cs.Name, v) }
            }
        }
    }
    return res
}

func partitionKeyForRow(f *j.Frame, r int, cols []string) string {
    parts := make([]string, len(cols))
    for i, name := range cols {
        col, _ := f.ColumnByName(name)
        switch col := col.(type) {
        case *j.StringColumn:
            if v, ok := col.Get(r); ok { parts[i] = sanitizePartition(v) } else { parts[i] = "_null" }
        case *j.FloatColumn:
            if v, ok := col.Get(r); ok { parts[i] = fmt.Sprintf("%.6g", v) } else { parts[i] = "_null" }
        case *j.IntColumn:
            if v, ok := col.Get(r); ok { parts[i] = fmt.Sprintf("%d", v) } else { parts[i] = "_null" }
        case *j.BoolColumn:
            if v, ok := col.Get(r); ok { if v { parts[i] = "true" } else { parts[i] = "false" } } else { parts[i] = "_null" }
        case *j.TimeColumn:
            if v, ok := col.Get(r); ok { parts[i] = sanitizePartition(v.Format("2006-01-02")) } else { parts[i] = "_null" }
        default:
            parts[i] = "_"
        }
    }
    return strings.Join(parts, "/")
}

func sanitizePartition(s string) string {
    // replace path separators and spaces
    s = strings.ReplaceAll(s, "/", "-")
    s = strings.ReplaceAll(s, "\\", "-")
    s = strings.ReplaceAll(s, " ", "_")
    return s
}

func expandOutPath(tmpl string, schema j.Schema, cols []string, f *j.Frame, key string) string {
    out := tmpl
    // Support {basename} (handled earlier for CSV/JSONL multi-file) and {col:Name}
    for _, name := range cols {
        placeholder := "{col:" + name + "}"
        if strings.Contains(out, placeholder) {
            // compute value from the first row (all rows in partition share it)
            val := ""
            if f.Rows() > 0 {
                col, _ := f.ColumnByName(name)
                switch col := col.(type) {
                case *j.StringColumn:
                    if v, ok := col.Get(0); ok { val = sanitizePartition(v) }
                case *j.FloatColumn:
                    if v, ok := col.Get(0); ok { val = fmt.Sprintf("%.6g", v) }
                case *j.IntColumn:
                    if v, ok := col.Get(0); ok { val = fmt.Sprintf("%d", v) }
                case *j.BoolColumn:
                    if v, ok := col.Get(0); ok { if v { val = "true" } else { val = "false" } }
                case *j.TimeColumn:
                    if v, ok := col.Get(0); ok { val = sanitizePartition(v.Format("2006-01-02")) }
                }
            }
            out = strings.ReplaceAll(out, placeholder, val)
        }
    }
    // if no {col:} placeholders, fall back to the joined key
    if out == tmpl {
        out = strings.ReplaceAll(out, "{basename}", key)
    }
    return out
}

func hasWildcards(path string) bool {
    return strings.ContainsAny(path, "*?[")
}

func buildPipeline(steps []json.RawMessage) (*j.Pipeline, []string, error) {
    p := j.NewPipeline()
    var names []string
    for i, raw := range steps {
        var probe map[string]json.RawMessage
        if err := json.Unmarshal(raw, &probe); err != nil {
            return nil, nil, fmt.Errorf("step %d: %w", i+1, err)
        }
        for k, v := range probe {
            t, col, ok := newTransform(k, v)
            if !ok {
                return nil, nil, fmt.Errorf("unknown step %q at step %d", k, i+1)
            }
            p.Add(t)
            names = append(names, k+":"+col)
        }
    }
    return p, names, nil
}

func newTransform(key string, v json.RawMessage) (j.Transform, string, bool) {
    var c struct{ Column string `json:"column"` }
    _ = json.Unmarshal(v, &c)
    switch key {
    case "impute_constant":
        var s struct{ Value any `json:"value"` }
        _ = json.Unmarshal(v, &s)
        return &imp.Constant{Column: c.Column, Value: s.Value}, c.Column, true
    case "impute_mean":
        return &imp.Mean{Column: c.Column}, c.Column, true
    case "impute_median":
        return &imp.Median{Column: c.Column}, c.Column, true
    case "impute_mode":
        return &imp.Mode{Column: c.Column}, c.Column, true
    case "trim":
        return &std.Trim{Column: c.Column}, c.Column, true
    case "lower":
        return &std.Lower{Column: c.Column}, c.Column, true
    case "regex_replace":
        var s struct{ Pattern string `json:"pattern"`; Replace string `json:"replace"` }
        _ = json.Unmarshal(v, &s)
        return &std.RegexReplace{Column: c.Column, Pattern: s.Pattern, Replace: s.Replace}, c.Column, true
    case "map_values":
        var s struct{ Map map[string]string `json:"map"` }
        _ = json.Unmarshal(v, &s)
        return &std.MapValues{Column: c.Column, Map: s.Map}, c.Column, true
    case "validate_in":
        var s struct{ Values []string `json:"values"` }
        _ = json.Unmarshal(v, &s)
        return val.NewInSet(c.Column, s.Values), c.Column, true
    case "validate_range":
        var s struct{ Min *float64 `json:"min"`; Max *float64 `json:"max"` }
        _ = json.Unmarshal(v, &s)
        return &val.Range{Column: c.Column, Min: s.Min, Max: s.Max}, c.Column, true
    case "cap_range":
        var s struct{ Min *float64 `json:"min"`; Max *float64 `json:"max"` }
        _ = json.Unmarshal(v, &s)
        return &outl.Cap{Column: c.Column, Min: s.Min, Max: s.Max}, c.Column, true
    }
    return nil, "", false
}
